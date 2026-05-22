//! Consensus reporters for Kora nodes.
#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/refcell/kora/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::{fmt, marker::PhantomData, sync::Arc};

use alloy_consensus::{
    Transaction as _, TxEnvelope,
    transaction::{SignerRecoverable as _, to_eip155_value},
};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{B256, Bytes, U256, keccak256, logs_bloom};
use commonware_consensus::{
    Block as _, Reporter,
    marshal::Update,
    simplex::{
        scheme::bls12381_threshold::vrf::{Scheme, Seedable as _},
        types::Activity,
    },
};
use commonware_cryptography::{Committable as _, bls12381::primitives::variant::Variant};
use commonware_runtime::{Spawner as _, tokio};
use commonware_utils::acknowledgement::Acknowledgement as _;
use kora_consensus::BlockExecution;
use kora_domain::{Block, ConsensusDigest, MempoolEvent, PublicKey};
use kora_executor::{BlockContext, BlockExecutor, ExecutionOutcome};
use kora_indexer::{BlockIndex, IndexedBlock, IndexedLog, IndexedReceipt, IndexedTransaction};
use kora_ledger::LedgerService;
use kora_overlay::OverlayState;
use kora_qmdb_ledger::QmdbState;
use kora_rpc::{MempoolEventSender, NodeState};
use tracing::{error, trace, warn};

/// Provides block execution context for finalized block verification.
pub trait BlockContextProvider: Clone + Send + Sync + 'static {
    /// Build a block execution context for the provided block.
    fn context(&self, block: &Block) -> BlockContext;
}

/// Helper function for SeedReporter::report that owns all its inputs.
async fn seed_report_inner<V: Variant>(
    state: LedgerService,
    activity: Activity<Scheme<PublicKey, V>, ConsensusDigest>,
) {
    match activity {
        Activity::Notarization(notarization) => {
            state
                .set_seed(
                    notarization.proposal.payload,
                    SeedReporter::<V>::hash_seed(notarization.seed()),
                )
                .await;
        }
        Activity::Finalization(finalization) => {
            state
                .set_seed(
                    finalization.proposal.payload,
                    SeedReporter::<V>::hash_seed(finalization.seed()),
                )
                .await;
        }
        _ => {}
    }
}

#[derive(Clone)]
/// Tracks simplex activity to store seed hashes for future proposals.
pub struct SeedReporter<V> {
    /// Ledger service that keeps per-digest seeds and snapshots.
    state: LedgerService,
    /// Marker indicating the variant for the threshold scheme in use.
    _variant: PhantomData<V>,
}

impl<V> fmt::Debug for SeedReporter<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeedReporter").finish_non_exhaustive()
    }
}

impl<V> SeedReporter<V> {
    /// Create a new seed reporter for the provided ledger service.
    pub const fn new(state: LedgerService) -> Self {
        Self { state, _variant: PhantomData }
    }

    fn hash_seed(seed: impl commonware_codec::Encode) -> B256 {
        keccak256(seed.encode())
    }
}

impl<V> Reporter for SeedReporter<V>
where
    V: Variant,
{
    type Activity = Activity<Scheme<PublicKey, V>, ConsensusDigest>;

    fn report(&mut self, activity: Self::Activity) -> impl std::future::Future<Output = ()> + Send {
        let state = self.state.clone();
        async move {
            seed_report_inner(state, activity).await;
        }
    }
}

async fn handle_finalized_update<E, P>(
    state: LedgerService,
    context: tokio::Context,
    executor: E,
    provider: P,
    block_index: Option<Arc<BlockIndex>>,
    mempool_broadcast: Option<MempoolEventSender>,
    update: Update<Block>,
) where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes>,
    P: BlockContextProvider,
{
    match update {
        Update::Tip(..) => {}
        Update::Block(block, ack) => {
            let result = finalize_block(
                &state,
                &context,
                &executor,
                &provider,
                block_index.as_ref(),
                &block,
            )
            .await;

            if let Ok((Some(outcome), Some(block_context))) = result.as_ref()
                && let Some(index) = block_index.as_ref()
            {
                index_finalized_block(index, &block, block_context, outcome);
            }

            // Always prune the mempool regardless of whether finalization succeeded.
            // The block is consensus-finalized, so its transactions must never be
            // re-proposed even if local execution or persistence failed.
            state.prune_mempool(&block.txs).await;
            publish_mempool_inclusions(mempool_broadcast.as_ref(), &block);
            // Marshal waits for the application to acknowledge processing before advancing the
            // delivery floor. Without this, the node can stall on finalized block delivery.
            ack.acknowledge();
        }
    }
}

/// Inner helper that performs the fallible finalization work for a single block.
///
/// Returns `Ok((execution_outcome, execution_context))` on success, where the
/// inner `Option`s may be `None` when a cached snapshot was reused without
/// re-execution.  Returns `Err(())` when a fatal error is encountered (already
/// logged inside this function).
async fn finalize_block<E, P>(
    state: &LedgerService,
    context: &tokio::Context,
    executor: &E,
    provider: &P,
    block_index: Option<&Arc<BlockIndex>>,
    block: &Block,
) -> Result<(Option<ExecutionOutcome>, Option<BlockContext>), ()>
where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes>,
    P: BlockContextProvider,
{
    let digest = block.commitment();
    let snapshot_exists = state.query_state_root(digest).await.is_some();
    let mut execution_outcome = None;
    let mut execution_context = None;

    if !snapshot_exists || block_index.is_some() {
        if snapshot_exists {
            trace!(?digest, "re-executing finalized block for RPC indexing");
        } else {
            trace!(?digest, "missing snapshot for finalized block; re-executing");
        }
        let parent_digest = block.parent();
        if let Some(parent_snapshot) = state.parent_snapshot(parent_digest).await {
            let block_context = provider.context(block);
            let execution = match BlockExecution::execute(
                &parent_snapshot,
                executor,
                &block_context,
                &block.txs,
            )
            .await
            {
                Ok(result) => result,
                Err(err) => {
                    error!(?digest, error = ?err, "failed to execute finalized block");
                    return Err(());
                }
            };

            let state_root = match state
                .compute_root_from_store(parent_digest, execution.outcome.changes.clone())
                .await
            {
                Ok(root) => root,
                Err(err) => {
                    error!(?digest, error = ?err, "failed to compute qmdb root");
                    return Err(());
                }
            };
            if state_root != block.state_root {
                warn!(
                    ?digest,
                    expected = ?block.state_root,
                    computed = ?state_root,
                    "state root mismatch for finalized block"
                );
                return Err(());
            }

            if !snapshot_exists {
                let merged_changes =
                    parent_snapshot.state.merge_changes(execution.outcome.changes.clone());
                let next_state = OverlayState::new(parent_snapshot.state.base(), merged_changes);
                state
                    .insert_snapshot(
                        digest,
                        parent_digest,
                        next_state,
                        state_root,
                        execution.outcome.changes.clone(),
                        &block.txs,
                    )
                    .await;
            }

            execution_outcome = Some(execution.outcome);
            execution_context = Some(block_context);
        } else if snapshot_exists {
            warn!(
                ?digest,
                ?parent_digest,
                "missing parent snapshot for cached finalized block; skipping RPC indexing replay"
            );
        } else {
            error!(?digest, ?parent_digest, "missing parent snapshot for finalized block");
            return Err(());
        }
    } else {
        trace!(?digest, "using cached snapshot for finalized block");
    }
    let persist_state = state.clone();
    let persist_handle = context
        .clone()
        .shared(true)
        .spawn(move |_| async move { persist_state.persist_snapshot(digest).await });
    let persist_result = match persist_handle.await {
        Ok(result) => result,
        Err(err) => {
            error!(?digest, error = ?err, "persist task failed");
            return Err(());
        }
    };
    if let Err(err) = persist_result {
        error!(?digest, error = ?err, "failed to persist finalized block");
        return Err(());
    }

    Ok((execution_outcome, execution_context))
}

fn publish_mempool_inclusions(mempool_broadcast: Option<&MempoolEventSender>, block: &Block) {
    let Some(sender) = mempool_broadcast else {
        return;
    };

    let block_hash = block.id().0;
    for tx in &block.txs {
        let _ = sender.send(MempoolEvent::TxIncluded {
            hash: keccak256(&tx.bytes),
            block_number: block.height,
            block_hash,
        });
    }
}

#[cfg(test)]
mod mempool_tests {
    use alloy_primitives::{B256, Bytes, keccak256};
    use kora_domain::{BlockId, StateRoot, Tx};

    use super::*;

    #[test]
    fn publish_mempool_inclusions_broadcasts_tx_included() {
        let (sender, mut receiver) = kora_rpc::mempool_event_channel();
        let tx = Tx::new(Bytes::from_static(&[0x01, 0x02, 0x03]));
        let block = Block {
            parent: BlockId(B256::ZERO),
            height: 7,
            timestamp: 0,
            prevrandao: B256::ZERO,
            state_root: StateRoot(B256::ZERO),
            txs: vec![tx.clone()],
        };
        let block_hash = block.id().0;

        publish_mempool_inclusions(Some(&sender), &block);

        assert_eq!(
            receiver.try_recv().unwrap(),
            MempoolEvent::TxIncluded {
                hash: keccak256(&tx.bytes),
                block_number: block.height,
                block_hash,
            }
        );
    }
}

#[cfg(test)]
mod finalize_error_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_consensus::Header;
    use alloy_primitives::{B256, Bytes};
    use commonware_runtime::Runner as _;
    use commonware_utils::acknowledgement::{Acknowledgement as _, Exact};
    use kora_domain::{StateRoot, Tx};
    use kora_executor::ExecutionError;
    use kora_ledger::LedgerView;

    use super::*;

    static PARTITION_COUNTER: AtomicUsize = AtomicUsize::new(10_000);

    fn next_partition(prefix: &str) -> String {
        let id = PARTITION_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }

    /// A block executor that always returns an error.
    ///
    /// Used to force `finalize_block` into an error path so the caller can
    /// verify that pruning and acknowledgement still happen unconditionally.
    #[derive(Clone)]
    struct FailingExecutor;

    impl BlockExecutor<OverlayState<QmdbState>> for FailingExecutor {
        type Tx = Bytes;

        fn execute(
            &self,
            _state: &OverlayState<QmdbState>,
            _context: &BlockContext,
            _txs: &[Bytes],
        ) -> Result<ExecutionOutcome, ExecutionError> {
            Err(ExecutionError::TxExecution("injected test failure".into()))
        }

        fn validate_header(&self, _header: &Header) -> Result<(), ExecutionError> {
            Ok(())
        }
    }

    /// A trivial block-context provider for tests.
    #[derive(Clone)]
    struct StubProvider;

    impl BlockContextProvider for StubProvider {
        fn context(&self, block: &Block) -> BlockContext {
            BlockContext::new(Header::default(), block.parent.0, block.prevrandao)
        }
    }

    /// Regression test: when `finalize_block` returns `Err(())` (e.g. executor
    /// failure), `handle_finalized_update` must still prune the mempool and
    /// acknowledge the update so the node does not stall.
    ///
    /// This covers the bug where early-returns on error paths skipped pruning
    /// and acknowledgement, leading to stale tx re-proposals and marshal
    /// delivery stalls.
    #[test]
    fn prune_and_ack_still_run_when_finalization_fails() {
        let runner = tokio::Runner::default();
        runner.start(|context| async move {
            // -- set up ledger with an empty genesis --
            let ledger = LedgerView::init(
                context.clone(),
                next_partition("reporters-finalize-err"),
                Vec::new(),
            )
            .await
            .expect("init ledger");
            let service = LedgerService::new(ledger);
            let genesis = service.genesis_block();

            // -- insert a transaction into the mempool --
            let tx = Tx::new(Bytes::from_static(&[0xab, 0xcd]));
            assert!(service.submit_tx(tx.clone()).await, "tx should be accepted into mempool");
            let pool = service.txpool().await;
            assert_eq!(pool.len(), 1, "mempool should contain the submitted tx");

            // -- build a block that references genesis as parent --
            // The block's own snapshot does NOT exist in the store, so
            // `finalize_block` will attempt execution (and our FailingExecutor
            // will cause it to return Err(())).
            let block = Block {
                parent: genesis.id(),
                height: 1,
                timestamp: 1,
                prevrandao: B256::ZERO,
                state_root: StateRoot(B256::ZERO),
                txs: vec![tx],
            };

            // -- create an acknowledgement we can observe --
            let (ack, waiter) = Exact::handle();

            // -- invoke the handler --
            handle_finalized_update(
                service.clone(),
                context,
                FailingExecutor,
                StubProvider,
                None,
                None,
                Update::Block(block, ack),
            )
            .await;

            // -- assert: mempool was pruned --
            assert_eq!(pool.len(), 0, "mempool must be pruned even when finalization fails");

            // -- assert: acknowledgement was delivered --
            waiter.await.expect("ack must be called even when finalization fails");
        });
    }
}

#[derive(Clone, Debug)]
struct TxMetadata {
    from: alloy_primitives::Address,
    to: Option<alloy_primitives::Address>,
    value: alloy_primitives::U256,
    gas_limit: u64,
    gas_price: u128,
    tx_type: u8,
    chain_id: Option<u64>,
    max_fee_per_gas: Option<u128>,
    max_priority_fee_per_gas: Option<u128>,
    v: u128,
    r: U256,
    s: U256,
    input: Bytes,
    nonce: u64,
}

fn index_finalized_block(
    index: &BlockIndex,
    block: &Block,
    block_context: &BlockContext,
    outcome: &ExecutionOutcome,
) {
    let block_hash = block.id().0;
    let transaction_hashes = block.txs.iter().map(|tx| keccak256(&tx.bytes)).collect::<Vec<_>>();
    let tx_metadata = block.txs.iter().map(|tx| decode_tx_metadata(&tx.bytes)).collect::<Vec<_>>();

    let indexed_block = IndexedBlock {
        hash: block_hash,
        number: block.height,
        parent_hash: block.parent.0,
        state_root: block.state_root.0,
        timestamp: block.timestamp,
        gas_limit: block_context.header.gas_limit,
        gas_used: outcome.gas_used,
        base_fee_per_gas: block_context.header.base_fee_per_gas,
        transaction_hashes,
    };

    let indexed_txs = tx_metadata
        .iter()
        .enumerate()
        .filter_map(|(idx, metadata)| {
            let metadata = metadata.as_ref()?;
            let hash = keccak256(&block.txs[idx].bytes);
            Some(IndexedTransaction {
                hash,
                block_hash,
                block_number: block.height,
                index: idx as u64,
                from: metadata.from,
                to: metadata.to,
                value: metadata.value,
                gas_limit: metadata.gas_limit,
                gas_price: metadata.gas_price,
                tx_type: metadata.tx_type,
                chain_id: metadata.chain_id,
                max_fee_per_gas: metadata.max_fee_per_gas,
                max_priority_fee_per_gas: metadata.max_priority_fee_per_gas,
                v: metadata.v,
                r: metadata.r,
                s: metadata.s,
                input: metadata.input.clone(),
                nonce: metadata.nonce,
            })
        })
        .collect();

    let mut next_log_index = 0u64;
    let indexed_receipts = outcome
        .receipts
        .iter()
        .enumerate()
        .filter_map(|(idx, receipt)| {
            let metadata = tx_metadata.get(idx)?.as_ref()?;
            let transaction_hash = receipt.tx_hash;
            let transaction_index = idx as u64;
            let receipt_logs_bloom = logs_bloom(receipt.logs());
            let logs = receipt
                .logs()
                .iter()
                .map(|log| {
                    let (topics, data) = log.data.clone().split();
                    let log_index = next_log_index;
                    next_log_index += 1;
                    IndexedLog {
                        address: log.address,
                        topics,
                        data,
                        log_index,
                        block_number: block.height,
                        block_hash,
                        transaction_hash,
                        transaction_index,
                    }
                })
                .collect();

            Some(IndexedReceipt {
                transaction_hash,
                block_hash,
                block_number: block.height,
                transaction_index,
                from: metadata.from,
                to: metadata.to,
                cumulative_gas_used: receipt.cumulative_gas_used(),
                gas_used: receipt.gas_used,
                contract_address: receipt.contract_address,
                logs,
                logs_bloom: receipt_logs_bloom,
                tx_type: metadata.tx_type,
                effective_gas_price: receipt_effective_gas_price(
                    metadata,
                    block_context.header.base_fee_per_gas,
                ),
                status: receipt.success(),
            })
        })
        .collect();

    index.insert_block(indexed_block, indexed_txs, indexed_receipts);
}

fn decode_tx_metadata(tx_bytes: &Bytes) -> Option<TxMetadata> {
    let envelope = match TxEnvelope::decode_2718(&mut tx_bytes.as_ref()) {
        Ok(envelope) => envelope,
        Err(err) => {
            warn!(error = %err, "failed to decode finalized transaction for indexing");
            return None;
        }
    };
    let from = match envelope.recover_signer() {
        Ok(from) => from,
        Err(err) => {
            warn!(error = %err, "failed to recover finalized transaction sender for indexing");
            return None;
        }
    };
    let signature = envelope.signature();

    Some(TxMetadata {
        from,
        to: envelope.to(),
        value: envelope.value(),
        gas_limit: envelope.gas_limit(),
        gas_price: transaction_gas_price(&envelope),
        tx_type: transaction_type(&envelope),
        chain_id: envelope.chain_id(),
        max_fee_per_gas: max_fee_per_gas(&envelope),
        max_priority_fee_per_gas: max_priority_fee_per_gas(&envelope),
        v: signature_v(&envelope),
        r: signature.r(),
        s: signature.s(),
        input: envelope.input().clone(),
        nonce: envelope.nonce(),
    })
}

fn signature_v(envelope: &TxEnvelope) -> u128 {
    let y_parity = envelope.signature().v();
    match envelope {
        TxEnvelope::Legacy(tx) => to_eip155_value(y_parity, tx.tx().chain_id),
        TxEnvelope::Eip2930(_)
        | TxEnvelope::Eip1559(_)
        | TxEnvelope::Eip4844(_)
        | TxEnvelope::Eip7702(_) => u128::from(y_parity),
    }
}

const fn transaction_type(envelope: &TxEnvelope) -> u8 {
    match envelope {
        TxEnvelope::Legacy(_) => 0,
        TxEnvelope::Eip2930(_) => 1,
        TxEnvelope::Eip1559(_) => 2,
        TxEnvelope::Eip4844(_) => 3,
        TxEnvelope::Eip7702(_) => 4,
    }
}

const fn transaction_gas_price(envelope: &TxEnvelope) -> u128 {
    match envelope {
        TxEnvelope::Legacy(tx) => tx.tx().gas_price,
        TxEnvelope::Eip2930(tx) => tx.tx().gas_price,
        TxEnvelope::Eip1559(tx) => tx.tx().max_fee_per_gas,
        TxEnvelope::Eip4844(tx) => tx.tx().tx().max_fee_per_gas,
        TxEnvelope::Eip7702(tx) => tx.tx().max_fee_per_gas,
    }
}

const fn max_fee_per_gas(envelope: &TxEnvelope) -> Option<u128> {
    match envelope {
        TxEnvelope::Legacy(_) | TxEnvelope::Eip2930(_) => None,
        TxEnvelope::Eip1559(tx) => Some(tx.tx().max_fee_per_gas),
        TxEnvelope::Eip4844(tx) => Some(tx.tx().tx().max_fee_per_gas),
        TxEnvelope::Eip7702(tx) => Some(tx.tx().max_fee_per_gas),
    }
}

const fn max_priority_fee_per_gas(envelope: &TxEnvelope) -> Option<u128> {
    match envelope {
        TxEnvelope::Legacy(_) | TxEnvelope::Eip2930(_) => None,
        TxEnvelope::Eip1559(tx) => Some(tx.tx().max_priority_fee_per_gas),
        TxEnvelope::Eip4844(tx) => Some(tx.tx().tx().max_priority_fee_per_gas),
        TxEnvelope::Eip7702(tx) => Some(tx.tx().max_priority_fee_per_gas),
    }
}

fn receipt_effective_gas_price(metadata: &TxMetadata, base_fee_per_gas: Option<u64>) -> u128 {
    let Some(max_fee_per_gas) = metadata.max_fee_per_gas else {
        return metadata.gas_price;
    };
    let Some(base_fee_per_gas) = base_fee_per_gas else {
        return max_fee_per_gas;
    };

    let priority_fee = metadata.max_priority_fee_per_gas.unwrap_or_default();
    max_fee_per_gas.min(u128::from(base_fee_per_gas).saturating_add(priority_fee))
}

#[derive(Clone)]
/// Persists finalized blocks.
pub struct FinalizedReporter<E, P> {
    /// Ledger service used to verify blocks and persist snapshots.
    state: LedgerService,
    /// Tokio context used to schedule blocking work.
    context: tokio::Context,
    /// Block executor used to replay finalized blocks.
    executor: E,
    /// Provider that builds block execution context.
    provider: P,
    /// Optional RPC block index updated after finalized blocks are persisted.
    block_index: Option<Arc<BlockIndex>>,
    /// Optional mempool event channel for RPC subscriptions.
    mempool_broadcast: Option<MempoolEventSender>,
}

impl<E, P> fmt::Debug for FinalizedReporter<E, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinalizedReporter").finish_non_exhaustive()
    }
}

impl<E, P> FinalizedReporter<E, P>
where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes>,
    P: BlockContextProvider,
{
    /// Create a new finalized reporter.
    pub const fn new(
        state: LedgerService,
        context: tokio::Context,
        executor: E,
        provider: P,
    ) -> Self {
        Self { state, context, executor, provider, block_index: None, mempool_broadcast: None }
    }

    /// Attach the RPC-visible block index to update when blocks finalize.
    #[must_use]
    pub fn with_block_index(mut self, block_index: Arc<BlockIndex>) -> Self {
        self.block_index = Some(block_index);
        self
    }

    /// Attach the mempool event channel used by RPC subscriptions.
    #[must_use]
    pub fn with_mempool_broadcast(mut self, mempool_broadcast: MempoolEventSender) -> Self {
        self.mempool_broadcast = Some(mempool_broadcast);
        self
    }
}

impl<E, P> Reporter for FinalizedReporter<E, P>
where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes>,
    P: BlockContextProvider,
{
    type Activity = Update<Block>;

    fn report(&mut self, update: Self::Activity) -> impl std::future::Future<Output = ()> + Send {
        let state = self.state.clone();
        let context = self.context.clone();
        let executor = self.executor.clone();
        let provider = self.provider.clone();
        let block_index = self.block_index.clone();
        let mempool_broadcast = self.mempool_broadcast.clone();
        async move {
            handle_finalized_update(
                state,
                context,
                executor,
                provider,
                block_index,
                mempool_broadcast,
                update,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Header, SignableTransaction as _, TxEip1559};
    use alloy_eips::eip2718::Encodable2718 as _;
    use alloy_primitives::{
        Address, B256, Bloom, Log, LogData, Signature, TxKind, U256, keccak256,
    };
    use k256::ecdsa::SigningKey;
    use kora_domain::{BlockId, StateRoot, Tx};
    use kora_executor::ExecutionReceipt;
    use sha3::{Digest as _, Keccak256};

    use super::*;

    fn signed_eip1559_tx(
        chain_id: u64,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    ) -> Bytes {
        let mut secret = [0u8; 32];
        secret[31] = 1;
        let key = SigningKey::from_bytes((&secret).into()).expect("valid key");
        let tx = TxEip1559 {
            chain_id,
            nonce: 7,
            gas_limit: 50_000,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(Address::repeat_byte(0xbb)),
            value: U256::from(42),
            access_list: Default::default(),
            input: Bytes::from_static(&[0xde, 0xad]),
        };
        let digest = Keccak256::new_with_prefix(tx.encoded_for_signing());
        let (sig, recid) = key.sign_digest_recoverable(digest).expect("sign tx");
        let signature = Signature::from((sig, recid));
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        let mut raw = Vec::new();
        envelope.encode_2718(&mut raw);
        Bytes::from(raw)
    }

    #[test]
    fn finalized_index_preserves_transaction_receipt_and_log_metadata() {
        let tx_bytes = signed_eip1559_tx(1337, 20, 3);
        let tx_hash = keccak256(&tx_bytes);
        let block = Block {
            parent: BlockId(B256::repeat_byte(0x10)),
            height: 5,
            timestamp: 1234,
            prevrandao: B256::repeat_byte(0x20),
            state_root: StateRoot(B256::repeat_byte(0x30)),
            txs: vec![Tx::new(tx_bytes)],
        };
        let block_hash = block.id().0;
        let block_context = BlockContext::new(
            Header {
                timestamp: 1234,
                gas_limit: 30_000_000,
                base_fee_per_gas: Some(10),
                ..Header::default()
            },
            block.parent.0,
            block.prevrandao,
        );
        let log = Log {
            address: Address::repeat_byte(0xcc),
            data: LogData::new_unchecked(
                vec![B256::repeat_byte(0xdd)],
                Bytes::from_static(&[0x01, 0x02]),
            ),
        };
        let mut outcome = ExecutionOutcome::new();
        outcome.gas_used = 21_000;
        outcome.receipts =
            vec![ExecutionReceipt::new(tx_hash, true, 21_000, 21_000, vec![log], None)];

        let index = BlockIndex::new();
        index_finalized_block(&index, &block, &block_context, &outcome);

        let indexed_tx = index.get_transaction(&tx_hash).expect("indexed transaction");
        assert_eq!(indexed_tx.hash, tx_hash);
        assert_eq!(indexed_tx.block_hash, block_hash);
        assert_eq!(indexed_tx.tx_type, 2);
        assert_eq!(indexed_tx.chain_id, Some(1337));
        assert_eq!(indexed_tx.gas_price, 20);
        assert_eq!(indexed_tx.max_fee_per_gas, Some(20));
        assert_eq!(indexed_tx.max_priority_fee_per_gas, Some(3));
        assert_ne!(indexed_tx.r, U256::ZERO);
        assert_ne!(indexed_tx.s, U256::ZERO);

        let receipt = index.get_receipt(&tx_hash).expect("indexed receipt");
        assert_eq!(receipt.tx_type, 2);
        assert_eq!(receipt.effective_gas_price, 13);
        assert_ne!(receipt.logs_bloom, Bloom::ZERO);
        assert_eq!(receipt.logs.len(), 1);
        assert_eq!(receipt.logs[0].block_number, 5);
        assert_eq!(receipt.logs[0].block_hash, block_hash);
        assert_eq!(receipt.logs[0].transaction_hash, tx_hash);
        assert_eq!(receipt.logs[0].transaction_index, 0);
    }
}

/// Reporter that updates RPC-visible node state from consensus activity.
///
/// This reporter tracks:
/// - Current view number (from notarizations)
/// - Finalized block count
/// - Nullified round count
#[derive(Clone)]
pub struct NodeStateReporter<S> {
    /// RPC node state to update.
    state: NodeState,
    /// Marker for the signing scheme.
    _scheme: PhantomData<S>,
}

impl<S> fmt::Debug for NodeStateReporter<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeStateReporter").finish_non_exhaustive()
    }
}

impl<S> NodeStateReporter<S> {
    /// Create a new node state reporter.
    pub const fn new(state: NodeState) -> Self {
        Self { state, _scheme: PhantomData }
    }
}

impl<S> Reporter for NodeStateReporter<S>
where
    S: commonware_cryptography::certificate::Scheme + Clone + Send + 'static,
{
    type Activity = Activity<S, ConsensusDigest>;

    fn report(&mut self, activity: Self::Activity) -> impl std::future::Future<Output = ()> + Send {
        match &activity {
            Activity::Notarization(n) => {
                self.state.set_view(n.proposal.round.view().get());
            }
            Activity::Finalization(f) => {
                self.state.set_view(f.proposal.round.view().get());
                self.state.inc_finalized();
            }
            Activity::Nullification(_) => {
                self.state.inc_nullified();
            }
            _ => {}
        }
        async {}
    }
}
