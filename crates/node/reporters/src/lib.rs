//! Consensus reporters for Kora nodes.
#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/refcell/kora/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod gc_log;

use std::{
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_consensus::{
    ReceiptEnvelope, ReceiptWithBloom, Transaction as _, TxEnvelope,
    proofs::{calculate_receipt_root, calculate_transaction_root},
    transaction::{SignerRecoverable as _, to_eip155_value},
};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{B256, Bloom, Bytes, U256, keccak256, logs_bloom};
use commonware_consensus::{
    Block as _, Reporter, Viewable as _,
    marshal::Update,
    simplex::{
        scheme::bls12381_threshold::vrf::{Scheme, Seedable as _},
        types::{Activity, Attributable as _},
    },
};
use commonware_cryptography::{Committable as _, bls12381::primitives::variant::Variant};
use commonware_runtime::{Spawner as _, tokio};
use commonware_utils::acknowledgement::{Acknowledgement as _, Exact};
pub use gc_log::SelfdestructGcLog;
use kora_consensus::BlockExecution;
use kora_domain::{Block, ConsensusDigest, MempoolEvent, PublicKey, StateRoot};
use kora_executor::{BlockContext, BlockExecutor, ExecutionOutcome};
use kora_indexer::{BlockIndex, IndexedBlock, IndexedLog, IndexedReceipt, IndexedTransaction};
use kora_ledger::{LedgerError, LedgerService};
use kora_metrics::{AppMetrics, EquivocationTypeLabel};
use kora_overlay::OverlayState;
use kora_qmdb_ledger::QmdbState;
use kora_rpc::{MempoolEventSender, NodeState};
use thiserror::Error;
use tracing::{error, info, trace, warn};

/// Provides block execution context for finalized block verification.
pub trait BlockContextProvider: Clone + Send + Sync + 'static {
    /// Build a block execution context for the provided block.
    fn context(&self, block: &Block) -> BlockContext;
}

/// Maximum number of attempts for transient finalization failures.
const MAX_FINALIZATION_ATTEMPTS: u32 = 3;

/// Base delay between retry attempts (doubles each attempt: 100ms, 200ms, 400ms).
const FINALIZATION_RETRY_BASE: Duration = Duration::from_millis(100);

/// Default QMDB checkpoint cadence. A value of 1 preserves per-block persistence.
const DEFAULT_CHECKPOINT_INTERVAL: u64 = 1;

/// Errors that can occur during block finalization.
///
/// Each variant corresponds to a specific failure mode so callers can
/// distinguish transient errors (worth retrying) from permanent ones
/// (indicating state divergence or eviction).
#[derive(Debug, Error)]
enum FinalizationError {
    /// Block execution failed during finalization replay.
    #[error("execution failed: {0}")]
    ExecutionFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// QMDB root computation failed.
    #[error("root computation failed: {0}")]
    RootComputationFailed(#[source] LedgerError),

    /// Computed state root does not match the block's declared root.
    /// This is a deterministic mismatch and is NOT retryable.
    #[error("state root mismatch: expected {expected:?}, computed {computed:?}")]
    StateRootMismatch { expected: StateRoot, computed: StateRoot },

    /// The spawned persistence task panicked or was cancelled.
    #[error("persist task failed: {0}")]
    PersistTaskFailed(String),

    /// QMDB persistence returned an error.
    #[error("persist failed: {0}")]
    PersistFailed(#[source] LedgerError),
}

impl FinalizationError {
    /// Returns `true` if this error is potentially transient and the operation
    /// should be retried.
    const fn is_retryable(&self) -> bool {
        match self {
            // Deterministic: local state has diverged, retry produces the same mismatch.
            Self::StateRootMismatch { .. } => false,
            // All other failures may be transient (I/O, OOM, race condition).
            Self::ExecutionFailed(_)
            | Self::RootComputationFailed(_)
            | Self::PersistTaskFailed(_)
            | Self::PersistFailed(_) => true,
        }
    }

    /// Returns a static label suitable for Prometheus metric labels.
    const fn metric_label(&self) -> &'static str {
        match self {
            Self::ExecutionFailed(_) => "execution_failed",
            Self::RootComputationFailed(_) => "root_computation_failed",
            Self::StateRootMismatch { .. } => "state_root_mismatch",
            Self::PersistTaskFailed(_) => "persist_task_failed",
            Self::PersistFailed(_) => "persist_failed",
        }
    }
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
        Activity::ConflictingNotarize(ref proof) => {
            warn!(
                signer = ?proof.signer(),
                view = ?proof.view(),
                "EQUIVOCATION: conflicting notarize detected"
            );
        }
        Activity::ConflictingFinalize(ref proof) => {
            warn!(
                signer = ?proof.signer(),
                view = ?proof.view(),
                "EQUIVOCATION: conflicting finalize detected"
            );
        }
        Activity::NullifyFinalize(ref proof) => {
            warn!(
                signer = ?proof.signer(),
                view = ?proof.view(),
                "EQUIVOCATION: nullify-finalize conflict detected"
            );
        }
        // Normal per-vote and aggregate events that don't affect seed state.
        Activity::Notarize(_)
        | Activity::Certification(_)
        | Activity::Nullify(_)
        | Activity::Nullification(_)
        | Activity::Finalize(_) => {}
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

#[allow(clippy::too_many_arguments)]
async fn handle_finalized_update<E, P>(
    state: LedgerService,
    context: tokio::Context,
    executor: E,
    provider: P,
    block_index: Option<Arc<BlockIndex>>,
    mempool_broadcast: Option<MempoolEventSender>,
    gc_log: Option<Arc<SelfdestructGcLog>>,
    metrics: Option<AppMetrics>,
    checkpoint_interval: u64,
    pending_acks: Arc<Mutex<Vec<Exact>>>,
    node_state: Option<NodeState>,
    update: Update<Block>,
) where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes>,
    P: BlockContextProvider,
{
    match update {
        Update::Tip(..) => {}
        Update::Block(block, ack) => {
            if let Some(ref ns) = node_state {
                ns.set_finalized_height(block.height);
            }
            let persist_checkpoint =
                checkpoint_interval <= 1 || block.height.is_multiple_of(checkpoint_interval);
            let result = finalize_with_retry(
                &state,
                &context,
                &executor,
                &provider,
                block_index.as_ref(),
                &block,
                persist_checkpoint,
            )
            .await;

            // Record finalization result in metrics.
            if let Some(ref m) = metrics {
                if result.is_ok() {
                    m.blocks_finalized.inc();
                } else {
                    m.finalization_failures.inc();
                }

                // Update snapshot store depth gauges so operators can detect
                // when the persistence pipeline falls behind block production.
                let (total, unpersisted) = state.snapshot_store_stats().await;
                m.snapshot_store_total.set(total as i64);
                m.unpersisted_snapshot_depth.set(unpersisted as i64);
            }

            // If finalization permanently failed, the node's QMDB state has
            // diverged from the consensus chain.  Continuing would produce
            // incorrect state roots for all subsequent blocks, cause failed
            // proposals when this node is leader, and vote against valid blocks
            // from other validators.
            //
            // We deliberately do NOT acknowledge the checkpoint to the marshal
            // so it does not garbage-collect data that was never persisted.
            // Then we abort the process to prevent silent state divergence.
            //
            // See: https://github.com/Nunchi-trade/daeji/issues/269
            if let Err(ref e) = result {
                error!(
                    block_height = block.height,
                    error = %e,
                    error_kind = e.metric_label(),
                    "FATAL: finalization permanently failed -- \
                     aborting to prevent state divergence. \
                     The node must be restarted after investigating the root cause."
                );
                // Prune mempool before halting so a restart does not re-propose
                // transactions from the finalized block.
                state.prune_mempool(&block.txs).await;
                // Allow a brief window for log buffers to flush.
                ::tokio::time::sleep(Duration::from_millis(200)).await;
                std::process::abort();
            }

            if let Ok((Some(outcome), Some(block_context))) = result.as_ref() {
                if let Some(index) = block_index.as_ref() {
                    index_finalized_block(index, &block, block_context, outcome);
                    // Prune old blocks to bound memory usage (see issue #262).
                    let min_height = block.height.saturating_sub(BlockIndex::MAX_RETAINED_BLOCKS);
                    if min_height > 0 {
                        index.prune_before(min_height);
                    }
                }

                // Record selfdestructed addresses for future GC.
                if !outcome.selfdestructed_addresses.is_empty()
                    && let Some(ref log) = gc_log
                {
                    log.record(block.height, &outcome.selfdestructed_addresses);
                }
            }

            acknowledge_checkpoint(pending_acks, block.height, checkpoint_interval, ack).await;

            // Prune the mempool -- the block is consensus-finalized, so its
            // transactions must never be re-proposed.
            state.prune_mempool(&block.txs).await;

            // Evict any remaining transactions whose nonces are now stale
            // relative to finalized state.
            state.prune_stale_nonces().await;

            publish_mempool_inclusions(mempool_broadcast.as_ref(), &block);
        }
    }
}

async fn acknowledge_checkpoint(
    pending_acks: Arc<Mutex<Vec<Exact>>>,
    height: u64,
    checkpoint_interval: u64,
    ack: Exact,
) {
    let is_checkpoint = checkpoint_interval <= 1 || height.is_multiple_of(checkpoint_interval);
    if is_checkpoint {
        // Checkpoint boundary reached: acknowledge this block and all pending
        // blocks from previous non-checkpoint heights.  This tells the marshal
        // that all blocks up through this checkpoint are durably persisted
        // (QMDB has been fsynced and the archive has been fsynced).
        let pending = {
            let mut guard = pending_acks.lock().expect("pending_acks mutex poisoned");
            std::mem::take(&mut *guard)
        };
        for pending_ack in pending {
            pending_ack.acknowledge();
        }
        ack.acknowledge();
    } else {
        // Between checkpoints: defer acknowledgment until the next boundary.
        let mut guard = pending_acks.lock().expect("pending_acks mutex poisoned");
        guard.push(ack);
    }
}

/// Retry wrapper around [`finalize_block`] that retries transient failures
/// with exponential backoff.
///
/// Non-retryable errors (state root mismatch, evicted parent snapshot) are
/// returned immediately. Transient errors are retried up to
/// [`MAX_FINALIZATION_ATTEMPTS`] times with delays of 100ms, 200ms, 400ms, etc.
async fn finalize_with_retry<E, P>(
    state: &LedgerService,
    context: &tokio::Context,
    executor: &E,
    provider: &P,
    block_index: Option<&Arc<BlockIndex>>,
    block: &Block,
    persist_checkpoint: bool,
) -> Result<(Option<ExecutionOutcome>, Option<BlockContext>), FinalizationError>
where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes>,
    P: BlockContextProvider,
{
    let digest = block.commitment();
    let mut last_err = None;

    for attempt in 0..MAX_FINALIZATION_ATTEMPTS {
        match finalize_block(
            state,
            context,
            executor,
            provider,
            block_index,
            block,
            persist_checkpoint,
        )
        .await
        {
            Ok(result) => {
                if attempt > 0 {
                    info!(?digest, attempt, "finalization succeeded after retry");
                }
                return Ok(result);
            }
            Err(e) if e.is_retryable() && attempt < MAX_FINALIZATION_ATTEMPTS - 1 => {
                let delay = FINALIZATION_RETRY_BASE * 2u32.pow(attempt);
                warn!(
                    ?digest,
                    attempt = attempt + 1,
                    max_attempts = MAX_FINALIZATION_ATTEMPTS,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    error_kind = e.metric_label(),
                    "finalization failed with transient error, retrying"
                );
                ::tokio::time::sleep(delay).await;
                last_err = Some(e);
            }
            Err(e) => {
                // Either non-retryable or final attempt exhausted.
                error!(
                    ?digest,
                    attempt = attempt + 1,
                    max_attempts = MAX_FINALIZATION_ATTEMPTS,
                    error = %e,
                    error_kind = e.metric_label(),
                    retryable = e.is_retryable(),
                    block_height = block.height,
                    parent = ?block.parent(),
                    state_root = ?block.state_root,
                    tx_count = block.txs.len(),
                    "CRITICAL: finalization failed permanently -- \
                     consensus-agreed block will NOT be persisted to QMDB, \
                     node state may diverge from the network"
                );
                return Err(e);
            }
        }
    }

    // All retryable attempts exhausted (should only reach here if
    // MAX_FINALIZATION_ATTEMPTS > 0 and the last attempt was retryable).
    let e = last_err.expect("at least one attempt was made");
    error!(
        ?digest,
        attempts = MAX_FINALIZATION_ATTEMPTS,
        error = %e,
        error_kind = e.metric_label(),
        block_height = block.height,
        parent = ?block.parent(),
        state_root = ?block.state_root,
        tx_count = block.txs.len(),
        "CRITICAL: finalization retries exhausted -- \
         consensus-agreed block will NOT be persisted to QMDB, \
         node state may diverge from the network"
    );
    Err(e)
}

/// Inner helper that performs the fallible finalization work for a single block.
///
/// Returns `Ok((execution_outcome, execution_context))` on success, where the
/// inner `Option`s may be `None` when a cached snapshot was reused without
/// re-execution. Returns a typed [`FinalizationError`] on failure so the
/// caller can decide whether to retry.
async fn finalize_block<E, P>(
    state: &LedgerService,
    context: &tokio::Context,
    executor: &E,
    provider: &P,
    block_index: Option<&Arc<BlockIndex>>,
    block: &Block,
    persist_checkpoint: bool,
) -> Result<(Option<ExecutionOutcome>, Option<BlockContext>), FinalizationError>
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

        // Retry parent snapshot lookup with exponential backoff. A concurrent
        // persist_snapshot() call may be evicting or replacing snapshots; a
        // brief retry window avoids spurious "missing parent" failures that
        // would otherwise nullify the view.
        const MAX_PARENT_RETRIES: u32 = 3;
        const PARENT_RETRY_BASE_MS: u64 = 10;

        let mut parent_snapshot = state.parent_snapshot(parent_digest).await;
        if parent_snapshot.is_none() && !snapshot_exists {
            for attempt in 1..=MAX_PARENT_RETRIES {
                let delay = Duration::from_millis(PARENT_RETRY_BASE_MS << (attempt - 1));
                warn!(
                    ?digest,
                    ?parent_digest,
                    attempt,
                    ?delay,
                    "parent snapshot not found, retrying"
                );
                ::tokio::time::sleep(delay).await;
                parent_snapshot = state.parent_snapshot(parent_digest).await;
                if parent_snapshot.is_some() {
                    break;
                }
            }
        }

        if let Some(parent_snapshot) = parent_snapshot {
            let block_context = provider.context(block);
            let execution =
                BlockExecution::execute(&parent_snapshot, executor, &block_context, &block.txs)
                    .await
                    .map_err(|err| FinalizationError::ExecutionFailed(Box::new(err)))?;

            let state_root = state
                .compute_root_from_store(parent_digest, &execution.outcome.changes)
                .await
                .map_err(FinalizationError::RootComputationFailed)?;

            if state_root != block.state_root {
                return Err(FinalizationError::StateRootMismatch {
                    expected: block.state_root,
                    computed: state_root,
                });
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
            // Parent snapshot is missing and the block's own snapshot is also
            // missing.  This can happen during catch-up when blocks arrive
            // faster than they can be verified, or after a restart when
            // eviction races with finalization.
            //
            // Rather than permanently failing (which stalls the finalization
            // pipeline), restore the block as a persisted snapshot over the
            // current QMDB state.  The snapshot won't have correct overlay
            // changes, but the block is consensus-finalized so the state
            // root is authoritative.  The QMDB commit path uses the
            // declared state root, not the overlay, so persistence is safe.
            let is_evicted = state.is_snapshot_persisted(&parent_digest).await;
            warn!(
                ?digest,
                ?parent_digest,
                parent_evicted = is_evicted,
                height = block.height,
                "finalize_block: parent snapshot unavailable; restoring block as \
                 trusted persisted snapshot to unblock finalization pipeline"
            );
            state.restore_persisted_snapshot(block).await;
            // After restoring, the snapshot exists so persistence can
            // proceed.  We do not have execution results for RPC indexing,
            // but that is acceptable: the alternative was permanent failure.
        }
    } else {
        trace!(?digest, "using cached snapshot for finalized block");
    }
    if persist_checkpoint {
        let persist_state = state.clone();
        let persist_handle = context
            .clone()
            .shared(true)
            .spawn(move |_| async move { persist_state.persist_snapshot(digest).await });
        let persist_result = persist_handle
            .await
            .map_err(|err| FinalizationError::PersistTaskFailed(format!("{err}")))?;
        if let Err(err) = persist_result {
            return Err(FinalizationError::PersistFailed(err));
        }
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
        let block = Block::new(
            BlockId(B256::ZERO),
            7,
            0,
            B256::ZERO,
            StateRoot(B256::ZERO),
            vec![tx.clone()],
        );
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
    use kora_domain::StateRoot;
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
    /// Used to force `finalize_with_retry` into an error path so the caller
    /// can verify that permanent failures are surfaced correctly.
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

    /// Verify that `finalize_with_retry` returns an error when the executor
    /// permanently fails, which causes `handle_finalized_update` to abort the
    /// process (preventing silent state divergence).
    ///
    /// We cannot test `handle_finalized_update` end-to-end with a failing
    /// executor because it calls `std::process::abort()` on permanent
    /// finalization failure (see #269). Instead, we test the inner retry
    /// logic directly and verify it surfaces the expected error.
    ///
    /// Note: with retry logic, execution failures are retried up to 3 times
    /// before the error is considered permanent.
    #[test]
    fn finalize_with_retry_returns_error_on_permanent_failure() {
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

            // -- build a block that references genesis as parent --
            // The block's own snapshot does NOT exist in the store, so
            // `finalize_block` will attempt execution (and our FailingExecutor
            // will cause it to return Err(FinalizationError::ExecutionFailed)).
            let block = Block::new(genesis.id(), 1, 1, B256::ZERO, StateRoot(B256::ZERO), vec![]);

            // -- invoke finalize_with_retry directly --
            let result = finalize_with_retry(
                &service,
                &context,
                &FailingExecutor,
                &StubProvider,
                None,
                &block,
                true,
            )
            .await;

            // -- assert: finalization failed with execution error --
            assert!(result.is_err(), "finalize_with_retry must return Err on permanent failure");
            let err = result.unwrap_err();
            assert!(
                matches!(err, FinalizationError::ExecutionFailed(_)),
                "expected ExecutionFailed, got: {err:?}"
            );
        });
    }
}

#[cfg(test)]
mod finalize_success_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_consensus::Header;
    use alloy_primitives::{Address, B256, U256};
    use commonware_runtime::Runner as _;
    use commonware_utils::acknowledgement::{Acknowledgement as _, Exact};
    use k256::ecdsa::SigningKey;
    use kora_domain::evm::Evm;
    use kora_executor::ExecutionError;
    use kora_ledger::LedgerView;

    use super::*;

    static PARTITION_COUNTER: AtomicUsize = AtomicUsize::new(20_000);

    fn next_partition(prefix: &str) -> String {
        let id = PARTITION_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }

    /// A block executor that always returns an empty successful outcome.
    ///
    /// Produces no state changes, so the state root stays the same as the
    /// parent. This allows `finalize_block` to succeed with a matching root.
    #[derive(Clone)]
    struct EmptySuccessExecutor;

    impl BlockExecutor<OverlayState<QmdbState>> for EmptySuccessExecutor {
        type Tx = Bytes;

        fn execute(
            &self,
            _state: &OverlayState<QmdbState>,
            _context: &BlockContext,
            _txs: &[Bytes],
        ) -> Result<ExecutionOutcome, ExecutionError> {
            Ok(ExecutionOutcome::new())
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

    /// When finalization succeeds (executor returns Ok, state root matches),
    /// the handler must persist the snapshot, prune the mempool, and
    /// acknowledge the update.
    #[test]
    fn successful_finalization_persists_and_acknowledges() {
        let runner = tokio::Runner::default();
        runner.start(|context| async move {
            // -- set up ledger with an empty genesis --
            let ledger = LedgerView::init(
                context.clone(),
                next_partition("reporters-finalize-ok"),
                Vec::new(),
            )
            .await
            .expect("init ledger");
            let service = LedgerService::new(ledger);
            let genesis = service.genesis_block();
            let genesis_digest = genesis.commitment();

            // Fetch the genesis state root so we can build a matching block.
            let genesis_root =
                service.query_state_root(genesis_digest).await.expect("genesis state root");

            // -- insert a dummy tx into the mempool so we can verify pruning --
            let sender_key = SigningKey::from_bytes(&[2u8; 32].into()).expect("valid key");
            let to = Address::repeat_byte(0xcd);
            let tx = Evm::sign_eip1559_transfer(&sender_key, 1, to, U256::ZERO, 0, 21_000, 0, 0);
            assert!(service.submit_tx(tx.clone()).await, "tx should be accepted");
            let pool = service.txpool().await;
            assert_eq!(pool.len(), 1);

            // -- build a block with no real txs but containing the dummy tx --
            // EmptySuccessExecutor ignores transactions and produces an empty
            // changeset, so the state root stays at genesis_root.
            let block = Block::new(genesis.id(), 1, 1, B256::ZERO, genesis_root, vec![tx]);

            let (ack, waiter) = Exact::handle();

            handle_finalized_update(
                service.clone(),
                context,
                EmptySuccessExecutor,
                StubProvider,
                None,
                None,
                None,
                None,
                1,
                Arc::new(Mutex::new(Vec::new())),
                None,
                Update::Block(block.clone(), ack),
            )
            .await;

            // -- assert: mempool was pruned --
            assert_eq!(pool.len(), 0, "mempool must be pruned after successful finalization");

            // -- assert: acknowledgement was delivered --
            waiter.await.expect("ack must be called after successful finalization");

            // -- assert: snapshot was persisted (state root is queryable) --
            let block_digest = block.commitment();
            let stored_root = service.query_state_root(block_digest).await;
            assert!(stored_root.is_some(), "snapshot must exist after successful finalization");
            assert_eq!(
                stored_root.unwrap(),
                genesis_root,
                "persisted root must match the block state root"
            );
        });
    }

    /// When a `BlockIndex` is provided, successful finalization must populate
    /// the index with the finalized block metadata.
    #[test]
    fn finalization_updates_block_index() {
        let runner = tokio::Runner::default();
        runner.start(|context| async move {
            let ledger = LedgerView::init(
                context.clone(),
                next_partition("reporters-finalize-index"),
                Vec::new(),
            )
            .await
            .expect("init ledger");
            let service = LedgerService::new(ledger);
            let genesis = service.genesis_block();
            let genesis_digest = genesis.commitment();
            let genesis_root =
                service.query_state_root(genesis_digest).await.expect("genesis state root");

            // Build an empty block whose state root matches genesis (no changes).
            let block = Block::new(genesis.id(), 1, 1, B256::ZERO, genesis_root, Vec::new());
            let block_hash = block.id().0;

            let index = Arc::new(BlockIndex::new());
            let (ack, waiter) = Exact::handle();

            handle_finalized_update(
                service.clone(),
                context,
                EmptySuccessExecutor,
                StubProvider,
                Some(index.clone()),
                None,
                None,
                None,
                1,
                Arc::new(Mutex::new(Vec::new())),
                None,
                Update::Block(block, ack),
            )
            .await;

            waiter.await.expect("ack must be called");

            // -- assert: the block was indexed --
            let indexed = index.get_block_by_hash(&block_hash);
            assert!(indexed.is_some(), "block must be indexed after finalization");
            let indexed_block = indexed.unwrap();
            assert_eq!(indexed_block.number, 1);
            assert_eq!(indexed_block.hash, block_hash);
        });
    }

    #[test]
    fn checkpoint_interval_persists_chain_only_on_boundary() {
        let runner = tokio::Runner::default();
        runner.start(|context| async move {
            let ledger = LedgerView::init(
                context.clone(),
                next_partition("reporters-finalize-checkpoint"),
                Vec::new(),
            )
            .await
            .expect("init ledger");
            let service = LedgerService::new(ledger);
            let genesis = service.genesis_block();
            let genesis_digest = genesis.commitment();
            let genesis_root =
                service.query_state_root(genesis_digest).await.expect("genesis state root");

            let block1 = Block::new(genesis.id(), 1, 1, B256::ZERO, genesis_root, Vec::new());
            let block1_digest = block1.commitment();
            let block1_id = block1.id();
            let (ack1, waiter1) = Exact::handle();
            let pending_acks = Arc::new(Mutex::new(Vec::new()));

            handle_finalized_update(
                service.clone(),
                context.clone(),
                EmptySuccessExecutor,
                StubProvider,
                None,
                None,
                None,
                None,
                2,
                pending_acks.clone(),
                None,
                Update::Block(block1, ack1),
            )
            .await;

            assert_eq!(service.query_state_root(block1_digest).await, Some(genesis_root));
            assert!(
                !service.is_snapshot_persisted(&block1_digest).await,
                "height 1 should remain an in-memory snapshot before the checkpoint boundary"
            );

            let block2 = Block::new(block1_id, 2, 2, B256::ZERO, genesis_root, Vec::new());
            let block2_digest = block2.commitment();
            let (ack2, waiter2) = Exact::handle();

            handle_finalized_update(
                service.clone(),
                context,
                EmptySuccessExecutor,
                StubProvider,
                None,
                None,
                None,
                None,
                2,
                pending_acks,
                None,
                Update::Block(block2, ack2),
            )
            .await;
            waiter1.await.expect("first ack must be called at checkpoint");
            waiter2.await.expect("ack must be called");

            assert!(
                service.is_snapshot_persisted(&block1_digest).await,
                "checkpoint should persist unpersisted ancestors"
            );
            assert!(
                service.is_snapshot_persisted(&block2_digest).await,
                "checkpoint boundary should persist the boundary block"
            );
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

    // Approximate block size: fixed header overhead + sum of raw transaction sizes.
    // An Ethereum block header is ~508 bytes RLP-encoded; we use 508 as the
    // constant and add the raw EIP-2718 envelope bytes for each transaction.
    let tx_bytes_total: u64 = block.txs.iter().map(|tx| tx.bytes.len() as u64).sum();
    let block_size = 508 + tx_bytes_total;

    // Compute the transactions trie root from the raw EIP-2718 encoded transactions.
    let tx_envelopes: Vec<TxEnvelope> = block
        .txs
        .iter()
        .filter_map(|tx| TxEnvelope::decode_2718(&mut tx.bytes.as_ref()).ok())
        .collect();
    let transactions_root = calculate_transaction_root(&tx_envelopes);

    // Compute the receipts trie root from the execution receipts.
    let receipt_envelopes: Vec<ReceiptEnvelope> = outcome
        .receipts
        .iter()
        .zip(tx_metadata.iter())
        .filter_map(|(receipt, metadata)| {
            let metadata = metadata.as_ref()?;
            let bloom = logs_bloom(receipt.logs());
            let rwb = ReceiptWithBloom::new(receipt.receipt.clone(), bloom);
            Some(match metadata.tx_type {
                0 => ReceiptEnvelope::Legacy(rwb),
                1 => ReceiptEnvelope::Eip2930(rwb),
                2 => ReceiptEnvelope::Eip1559(rwb),
                3 => ReceiptEnvelope::Eip4844(rwb),
                4 => ReceiptEnvelope::Eip7702(rwb),
                _ => ReceiptEnvelope::Legacy(rwb),
            })
        })
        .collect();
    let receipts_root = calculate_receipt_root(&receipt_envelopes);

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
    let indexed_receipts: Vec<IndexedReceipt> = outcome
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

    // Compute block-level Bloom as the bitwise OR of all receipt Blooms.
    let mut block_logs_bloom = Bloom::ZERO;
    for receipt in &indexed_receipts {
        block_logs_bloom |= receipt.logs_bloom;
    }

    let indexed_block = IndexedBlock {
        hash: block_hash,
        number: block.height,
        parent_hash: block.parent.0,
        state_root: block.state_root.0,
        transactions_root,
        receipts_root,
        timestamp: block.timestamp,
        gas_limit: block_context.header.gas_limit,
        gas_used: outcome.gas_used,
        base_fee_per_gas: block_context.header.base_fee_per_gas,
        mix_hash: block.prevrandao,
        logs_bloom: block_logs_bloom,
        size: block_size,
        transaction_hashes,
    };

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
    /// Optional GC log for tracking selfdestructed addresses.
    gc_log: Option<Arc<SelfdestructGcLog>>,
    /// Optional application-level metrics.
    metrics: Option<AppMetrics>,
    /// Persist QMDB every N finalized blocks.
    checkpoint_interval: u64,
    /// Marshal acknowledgements held until the next checkpoint boundary.
    pending_acks: Arc<Mutex<Vec<Exact>>>,
    /// Optional node state for tracking the latest finalized height.
    node_state: Option<NodeState>,
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
    pub fn new(state: LedgerService, context: tokio::Context, executor: E, provider: P) -> Self {
        Self {
            state,
            context,
            executor,
            provider,
            block_index: None,
            mempool_broadcast: None,
            gc_log: None,
            metrics: None,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            pending_acks: Arc::new(Mutex::new(Vec::new())),
            node_state: None,
        }
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

    /// Attach a GC log for tracking selfdestructed contract addresses.
    ///
    /// When a finalized block contains selfdestructed contracts, their
    /// addresses are appended to this log for future garbage collection of
    /// orphaned QMDB storage entries.
    #[must_use]
    pub fn with_gc_log(mut self, gc_log: Arc<SelfdestructGcLog>) -> Self {
        self.gc_log = Some(gc_log);
        self
    }

    /// Attach application-level metrics for tracking finalization outcomes.
    #[must_use]
    pub fn with_metrics(mut self, metrics: AppMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Persist QMDB every `interval` finalized blocks.
    #[must_use]
    pub const fn with_checkpoint_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval = if interval == 0 { 1 } else { interval };
        self
    }

    /// Attach the RPC node state so the reporter can update finalized height.
    #[must_use]
    pub fn with_node_state(mut self, node_state: NodeState) -> Self {
        self.node_state = Some(node_state);
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
        let gc_log = self.gc_log.clone();
        let metrics = self.metrics.clone();
        let checkpoint_interval = self.checkpoint_interval;
        let pending_acks = self.pending_acks.clone();
        let node_state = self.node_state.clone();
        async move {
            handle_finalized_update(
                state,
                context,
                executor,
                provider,
                block_index,
                mempool_broadcast,
                gc_log,
                metrics,
                checkpoint_interval,
                pending_acks,
                node_state,
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
        let block = Block::new(
            BlockId(B256::repeat_byte(0x10)),
            5,
            1234,
            B256::repeat_byte(0x20),
            StateRoot(B256::repeat_byte(0x30)),
            vec![Tx::new(tx_bytes)],
        );
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
/// - Equivocation events (Byzantine behavior)
#[derive(Clone)]
pub struct NodeStateReporter<S> {
    /// RPC node state to update.
    state: NodeState,
    /// Optional application-level metrics for Prometheus counters.
    metrics: Option<AppMetrics>,
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
        Self { state, metrics: None, _scheme: PhantomData }
    }

    /// Attach application-level metrics for tracking equivocation events.
    #[must_use]
    pub fn with_metrics(mut self, metrics: AppMetrics) -> Self {
        self.metrics = Some(metrics);
        self
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
            Activity::ConflictingNotarize(proof) => {
                warn!(
                    signer = ?proof.signer(),
                    view = ?proof.view(),
                    "EQUIVOCATION: conflicting notarize detected"
                );
                self.state.inc_equivocations();
                if let Some(ref m) = self.metrics {
                    m.equivocations
                        .get_or_create(&EquivocationTypeLabel {
                            r#type: "conflicting_notarize".into(),
                        })
                        .inc();
                }
            }
            Activity::ConflictingFinalize(proof) => {
                warn!(
                    signer = ?proof.signer(),
                    view = ?proof.view(),
                    "EQUIVOCATION: conflicting finalize detected"
                );
                self.state.inc_equivocations();
                if let Some(ref m) = self.metrics {
                    m.equivocations
                        .get_or_create(&EquivocationTypeLabel {
                            r#type: "conflicting_finalize".into(),
                        })
                        .inc();
                }
            }
            Activity::NullifyFinalize(proof) => {
                warn!(
                    signer = ?proof.signer(),
                    view = ?proof.view(),
                    "EQUIVOCATION: nullify-finalize conflict detected"
                );
                self.state.inc_equivocations();
                if let Some(ref m) = self.metrics {
                    m.equivocations
                        .get_or_create(&EquivocationTypeLabel { r#type: "nullify_finalize".into() })
                        .inc();
                }
            }
            // Normal per-vote and aggregate events that don't affect node state.
            Activity::Notarize(_)
            | Activity::Certification(_)
            | Activity::Nullify(_)
            | Activity::Finalize(_) => {}
        }
        async {}
    }
}
