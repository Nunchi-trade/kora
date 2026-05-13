use std::{sync::Arc, time::Duration};

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, keccak256};
use anyhow::Context as _;
use commonware_consensus::{
    Reporters,
    marshal::{
        core::Mailbox,
        standard::{Inline, Standard},
    },
    simplex::{
        self, elector::Random, scheme::bls12381_threshold::vrf::Seedable as _, types::Finalization,
    },
    types::{Epoch, FixedEpocher, ViewDelta},
};
use commonware_cryptography::{bls12381::primitives::variant::MinSig, ed25519};
use commonware_p2p::{Manager, TrackedPeers};
use commonware_parallel::Sequential;
use commonware_runtime::{Metrics as _, Spawner, buffer::paged::CacheRef, tokio};
use commonware_storage::archive::{Archive, Identifier as ArchiveId};
use commonware_utils::{NZU64, NZUsize, acknowledgement::Exact, ordered::Set};
use futures::StreamExt;
use kora_domain::{Block, BlockCfg, BootstrapConfig, ConsensusDigest, LedgerEvent, Tx, TxCfg};
use kora_executor::{BlockContext, RevmExecutor};
use kora_indexer::{BlockIndex, IndexedBlock};
use kora_ledger::{LedgerService, LedgerView};
use kora_marshal::{ArchiveInitializer, BroadcastInitializer, PeerInitializer};
use kora_reporters::{BlockContextProvider, FinalizedReporter, NodeStateReporter, SeedReporter};
use kora_service::{NodeRunContext, NodeRunner};
use kora_simplex::{DEFAULT_MAILBOX_SIZE as MAILBOX_SIZE, DefaultPool};
use kora_transport::NetworkTransport;
use kora_txpool::{PoolConfig, TransactionValidator};
use tracing::{debug, info, trace, warn};

use crate::{RevmApplication, RunnerError, scheme::ThresholdScheme};

const BLOCK_CODEC_MAX_TXS: usize = 10_000;
// Large enough for a devnet stress batch of 10k signed transfers while still
// preserving the per-transaction 128 KiB admission limit in the tx validator.
const BLOCK_CODEC_MAX_TX_BYTES: usize = 8 * 1024 * 1024;
const EPOCH_LENGTH: u64 = u64::MAX;
const PARTITION_PREFIX: &str = "kora";

type Peer = ed25519::PublicKey;
type CertArchive = Finalization<ThresholdScheme, ConsensusDigest>;
type MarshalMailbox = Mailbox<ThresholdScheme, Standard<Block>>;
type NodeStateRptr = NodeStateReporter<ThresholdScheme>;

fn default_page_cache(context: &tokio::Context) -> CacheRef {
    DefaultPool::init(context)
}

const fn block_codec_cfg() -> BlockCfg {
    BlockCfg { max_txs: BLOCK_CODEC_MAX_TXS, tx: TxCfg { max_tx_bytes: BLOCK_CODEC_MAX_TX_BYTES } }
}

#[derive(Clone)]
struct ConstantSchemeProvider(Arc<ThresholdScheme>);

impl commonware_cryptography::certificate::Provider for ConstantSchemeProvider {
    type Scope = Epoch;
    type Scheme = ThresholdScheme;

    fn scoped(&self, _epoch: Epoch) -> Option<Arc<Self::Scheme>> {
        Some(self.0.clone())
    }

    fn all(&self) -> Option<Arc<Self::Scheme>> {
        Some(self.0.clone())
    }
}

impl From<ThresholdScheme> for ConstantSchemeProvider {
    fn from(scheme: ThresholdScheme) -> Self {
        Self(Arc::new(scheme))
    }
}

#[derive(Clone, Debug)]
struct RevmContextProvider {
    gas_limit: u64,
}

impl BlockContextProvider for RevmContextProvider {
    fn context(&self, block: &Block) -> BlockContext {
        let header = Header {
            number: block.height,
            timestamp: block.height,
            gas_limit: self.gas_limit,
            beneficiary: Address::ZERO,
            base_fee_per_gas: Some(0),
            ..Default::default()
        };
        BlockContext::new(header, B256::ZERO, block.prevrandao)
    }
}

fn spawn_ledger_observers<S: Spawner>(service: LedgerService, spawner: S) {
    let mut receiver = service.subscribe();
    spawner.shared(true).spawn(move |_| async move {
        while let Some(event) = receiver.next().await {
            match event {
                LedgerEvent::TransactionSubmitted(id) => {
                    trace!(tx=?id, "mempool accepted transaction");
                }
                LedgerEvent::SeedUpdated(digest, seed) => {
                    debug!(digest=?digest, seed=?seed, "seed cache refreshed");
                }
                LedgerEvent::SnapshotPersisted(digest) => {
                    trace!(?digest, "snapshot persisted");
                }
            }
        }
    });
}

fn seed_hash(seed: impl commonware_codec::Encode) -> B256 {
    keccak256(seed.encode())
}

fn index_recovered_block(index: &BlockIndex, block: &Block, provider: &RevmContextProvider) {
    let block_context = provider.context(block);
    let transaction_hashes = block.txs.iter().map(|tx| keccak256(&tx.bytes)).collect();
    let indexed_block = IndexedBlock {
        hash: block.id().0,
        number: block.height,
        parent_hash: block.parent.0,
        state_root: block.state_root.0,
        timestamp: block_context.header.timestamp,
        gas_limit: block_context.header.gas_limit,
        gas_used: 0,
        base_fee_per_gas: block_context.header.base_fee_per_gas,
        transaction_hashes,
    };
    index.insert_block(indexed_block, Vec::new(), Vec::new());
}

async fn recover_finalized_state<FB, FC>(
    ledger: &LedgerService,
    block_index: Option<&Arc<BlockIndex>>,
    finalized_blocks: &FB,
    finalizations_by_height: &FC,
    provider: &RevmContextProvider,
) -> anyhow::Result<()>
where
    FB: Archive<Key = ConsensusDigest, Value = Block>,
    FC: Archive<Key = ConsensusDigest, Value = CertArchive>,
{
    let block_ranges: Vec<_> = finalized_blocks.ranges().collect();
    let finalization_ranges: Vec<_> = finalizations_by_height.ranges().collect();

    for (start, end) in finalization_ranges {
        for height in start..=end {
            if let Some(finalization) = finalizations_by_height
                .get(ArchiveId::Index(height))
                .await
                .with_context(|| format!("load finalization at height {height}"))?
            {
                ledger
                    .set_seed(finalization.proposal.payload, seed_hash(finalization.seed()))
                    .await;
            }
        }
    }

    let mut recovered = 0u64;
    let mut head = None;
    for (start, end) in block_ranges {
        for height in start..=end {
            let Some(block) = finalized_blocks
                .get(ArchiveId::Index(height))
                .await
                .with_context(|| format!("load finalized block at height {height}"))?
            else {
                continue;
            };

            if let Some(index) = block_index {
                index_recovered_block(index, &block, provider);
            }
            head = Some(block);
            recovered += 1;
        }
    }

    if let Some(head) = head {
        ledger.restore_persisted_snapshot(&head).await;
        info!(
            height = head.height,
            blocks = recovered,
            "recovered finalized ledger head from archive"
        );
    }

    Ok(())
}

/// Production validator node runner.
#[derive(Clone, Debug)]
pub struct ProductionRunner {
    /// Threshold signing scheme.
    pub scheme: ThresholdScheme,
    /// Chain ID.
    pub chain_id: u64,
    /// Gas limit per block.
    pub gas_limit: u64,
    /// Bootstrap configuration.
    pub bootstrap: BootstrapConfig,
    /// Storage partition prefix.
    pub partition_prefix: String,
    /// Optional RPC configuration (state, bind address).
    pub rpc_config: Option<(kora_rpc::NodeState, std::net::SocketAddr)>,
    /// Secondary peers authorized to follow validator traffic without participating in consensus.
    pub secondary_peers: Vec<Peer>,
}

impl ProductionRunner {
    /// Create a new production runner.
    pub fn new(
        scheme: ThresholdScheme,
        chain_id: u64,
        gas_limit: u64,
        bootstrap: BootstrapConfig,
    ) -> Self {
        Self {
            scheme,
            chain_id,
            gas_limit,
            bootstrap,
            partition_prefix: PARTITION_PREFIX.to_string(),
            rpc_config: None,
            secondary_peers: Vec::new(),
        }
    }

    /// Configure RPC server.
    #[must_use]
    pub fn with_rpc(mut self, state: kora_rpc::NodeState, addr: std::net::SocketAddr) -> Self {
        self.rpc_config = Some((state, addr));
        self
    }

    /// Configure secondary peers that should be tracked by the P2P oracle.
    #[must_use]
    pub fn with_secondary_peers(mut self, peers: Vec<Peer>) -> Self {
        self.secondary_peers = peers;
        self
    }
}

impl ProductionRunner {
    /// Run the validator as a standalone process.
    pub fn run_standalone(self, config: kora_config::NodeConfig) -> Result<(), RunnerError> {
        use commonware_runtime::Runner;
        use kora_transport::NetworkConfigExt;

        let executor = tokio::Runner::new(
            tokio::Config::default().with_storage_directory(config.data_dir.join("runtime")),
        );
        executor.start(|context| async move {
            let validator_key = config
                .validator_key()
                .map_err(|e| anyhow::anyhow!("failed to load validator key: {}", e))?;

            let transport = config
                .network
                .build_local_transport(validator_key, context.clone())
                .map_err(|e| anyhow::anyhow!("failed to build transport: {}", e))?;

            let ctx =
                kora_service::NodeRunContext::new(context, std::sync::Arc::new(config), transport);

            let _ledger = self.run(ctx).await?;

            futures::future::pending::<()>().await;
            Ok::<(), RunnerError>(())
        })
    }
}

impl NodeRunner for ProductionRunner {
    type Transport = NetworkTransport<Peer, tokio::Context>;
    type Handle = LedgerService;
    type Error = RunnerError;

    async fn run(&self, ctx: NodeRunContext<Self::Transport>) -> Result<Self::Handle, Self::Error> {
        let (context, config, mut transport) = ctx.into_parts();

        info!(chain_id = self.chain_id, "Starting production validator");

        let validators = self.scheme.participants().clone();
        let secondary = Set::from_iter_dedup(self.secondary_peers.iter().cloned());
        let secondary_count = secondary.len();
        transport.oracle.track(0, TrackedPeers::new(validators, secondary)).await;
        info!(
            validators = self.scheme.participants().len(),
            secondary_peers = secondary_count,
            "Registered primary and secondary peers with oracle"
        );

        let page_cache = default_page_cache(&context);
        let block_cfg = block_codec_cfg();
        let partition_prefix = &self.partition_prefix;

        <ThresholdScheme as commonware_cryptography::certificate::Scheme>::certificate_codec_config_unbounded();
        let finalizations_by_height = ArchiveInitializer::init::<_, ConsensusDigest, CertArchive>(
            context.with_label("finalizations_by_height"),
            format!("{partition_prefix}-finalizations-by-height"),
            (),
        )
        .await
        .context("init finalizations archive")?;

        let finalized_blocks = ArchiveInitializer::init::<_, ConsensusDigest, Block>(
            context.with_label("finalized_blocks"),
            format!("{partition_prefix}-finalized-blocks"),
            block_cfg,
        )
        .await
        .context("init blocks archive")?;

        let has_finalized_history = finalized_blocks.last_index().is_some();
        let state = LedgerView::init_with_genesis(
            context.with_label("state"),
            format!("{}-qmdb", self.partition_prefix),
            self.bootstrap.genesis_alloc.clone(),
            !has_finalized_history,
        )
        .await
        .context("init qmdb")?;

        let block_index =
            self.rpc_config.as_ref().map(|_| Arc::new(kora_indexer::BlockIndex::new()));
        let ledger = LedgerService::new(state.clone());
        spawn_ledger_observers(ledger.clone(), context.clone());

        let executor = RevmExecutor::new(self.chain_id);
        let context_provider = RevmContextProvider { gas_limit: self.gas_limit };
        recover_finalized_state(
            &ledger,
            block_index.as_ref(),
            &finalized_blocks,
            &finalizations_by_height,
            &context_provider,
        )
        .await
        .context("recover finalized state")?;

        if let Some((node_state, addr)) = &self.rpc_config {
            let qmdb_state = state.qmdb_state().await;
            let rpc_executor = Arc::new(RevmExecutor::new(self.chain_id));
            let indexed_provider = kora_rpc::IndexedStateProvider::new(
                block_index.clone().expect("block index is initialized with RPC"),
                qmdb_state,
                rpc_executor,
            );
            let tx_ledger = ledger.clone();
            let tx_state = state.qmdb_state().await;
            let chain_id = self.chain_id;
            let tx_submit: kora_rpc::TxSubmitCallback = Arc::new(move |data| {
                let ledger = tx_ledger.clone();
                let state = tx_state.clone();
                Box::pin(async move {
                    let tx = Tx::new(data);
                    let tx_id = tx.id();
                    let validator =
                        TransactionValidator::new(chain_id, state, PoolConfig::default());
                    validator.validate(tx.clone()).await.map_err(|err| {
                        warn!(?tx_id, error = %err, "rpc submit: validator rejected tx");
                        kora_rpc::RpcError::InvalidTransaction(err.to_string())
                    })?;
                    if ledger.submit_tx(tx).await {
                        debug!(?tx_id, "rpc submit: tx inserted into mempool");
                        Ok(())
                    } else {
                        warn!(
                            ?tx_id,
                            "rpc submit: ledger.submit_tx returned false (duplicate or pool error)"
                        );
                        Err(kora_rpc::RpcError::InvalidTransaction(
                            "transaction rejected by mempool".to_string(),
                        ))
                    }
                })
            });
            let rpc = kora_rpc::RpcServer::with_state_provider(
                node_state.clone(),
                *addr,
                self.chain_id,
                indexed_provider,
            )
            .with_tx_submit(tx_submit)
            .with_peer_count(self.scheme.participants().len().saturating_sub(1) as u64);
            drop(rpc.start());
            info!(addr = %addr, "RPC server started with live state provider");
        }

        let validator_key = config
            .validator_key()
            .map_err(|e| anyhow::anyhow!("failed to load validator key: {}", e))?;
        let my_pk = commonware_cryptography::Signer::public_key(&validator_key);

        let mut finalized_reporter =
            FinalizedReporter::new(ledger.clone(), context.clone(), executor, context_provider);
        if let Some(block_index) = block_index {
            finalized_reporter = finalized_reporter.with_block_index(block_index);
        }

        let scheme_provider = ConstantSchemeProvider::from(self.scheme.clone());

        let resolver = PeerInitializer::init::<_, _, _, Block, _, _, _>(
            &context.with_label("resolver"),
            my_pk.clone(),
            transport.oracle.clone(),
            transport.oracle.clone(),
            transport.marshal.backfill,
        );

        let (broadcast_engine, buffer) = BroadcastInitializer::init::<_, Peer, Block, _>(
            context.with_label("broadcast"),
            my_pk.clone(),
            transport.oracle.clone(),
            block_cfg,
        );
        broadcast_engine.start(transport.marshal.blocks);

        let (actor, marshal_mailbox, _last_processed_height) =
            kora_marshal::ActorInitializer::init::<_, Block, _, _, _, Exact>(
                context.clone(),
                finalizations_by_height,
                finalized_blocks,
                scheme_provider,
                page_cache.clone(),
                block_cfg,
            )
            .await;
        actor.start(finalized_reporter, buffer, resolver);

        let epocher = FixedEpocher::new(NZU64!(EPOCH_LENGTH));
        let executor = RevmExecutor::new(self.chain_id);
        let mut app = RevmApplication::<ThresholdScheme, _>::new(
            ledger.clone(),
            executor,
            block_cfg.max_txs,
            self.gas_limit,
        );
        if let Some((state, _)) = &self.rpc_config {
            app = app.with_node_state(state.clone());
        }
        let marshaled =
            Inline::new(context.with_label("marshaled"), app, marshal_mailbox.clone(), epocher);

        let seed_reporter = SeedReporter::<MinSig>::new(ledger.clone());
        let node_state_reporter = self
            .rpc_config
            .as_ref()
            .map(|(state, _)| NodeStateReporter::<ThresholdScheme>::new(state.clone()));
        let inner_reporters: Reporters<_, MarshalMailbox, Option<NodeStateRptr>> =
            Reporters::from((marshal_mailbox.clone(), node_state_reporter));
        let reporter = Reporters::from((seed_reporter, inner_reporters));

        for tx in &self.bootstrap.bootstrap_txs {
            let _ = ledger.submit_tx(tx.clone()).await;
        }

        let engine = simplex::Engine::new(
            context.with_label("engine"),
            simplex::Config {
                scheme: self.scheme.clone(),
                elector: Random,
                blocker: transport.oracle.clone(),
                automaton: marshaled.clone(),
                relay: marshaled,
                reporter,
                strategy: Sequential,
                partition: self.partition_prefix.clone(),
                mailbox_size: MAILBOX_SIZE,
                epoch: Epoch::zero(),
                replay_buffer: NZUsize!(16 * 1024 * 1024),
                write_buffer: NZUsize!(16 * 1024 * 1024),
                leader_timeout: Duration::from_secs(5),
                certification_timeout: Duration::from_secs(10),
                timeout_retry: Duration::from_secs(2),
                fetch_timeout: Duration::from_secs(5),
                activity_timeout: ViewDelta::new(20),
                skip_timeout: ViewDelta::new(10),
                fetch_concurrent: 8,
                page_cache,
                forwarding: simplex::ForwardingPolicy::Disabled,
            },
        );
        engine.start(transport.simplex.votes, transport.simplex.certs, transport.simplex.resolver);

        info!("Validator started successfully");
        Ok(ledger)
    }
}
