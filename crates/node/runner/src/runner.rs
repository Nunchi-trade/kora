use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

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
use commonware_cryptography::{Committable as _, bls12381::primitives::variant::MinSig, ed25519};
use commonware_p2p::{Manager, TrackedPeers};
use commonware_runtime::{
    Clock as _, Handle as RuntimeHandle, Metrics as _, Spawner, ThreadPooler as _,
    buffer::paged::CacheRef, tokio as cw_tokio,
};
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
use kora_txpool::{PoolConfig, TransactionPool, TransactionValidator};
use tracing::{debug, error, info, trace, warn};

use crate::{RevmApplication, RunnerError, scheme::ThresholdScheme};

const EPOCH_LENGTH: u64 = u64::MAX;
const PARTITION_PREFIX: &str = "kora";
const TXPOOL_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const RUNTIME_DIR_ENV: &str = "KORA_RUNTIME_DIR";

type Peer = ed25519::PublicKey;
type CertArchive = Finalization<ThresholdScheme, ConsensusDigest>;
type MarshalMailbox = Mailbox<ThresholdScheme, Standard<Block>>;
type NodeStateRptr = NodeStateReporter<ThresholdScheme>;

fn default_page_cache(context: &cw_tokio::Context) -> CacheRef {
    DefaultPool::init(context)
}

/// Resolve the storage directory used by the Commonware runtime.
///
/// By default this lives under `data_dir/runtime` so validator state survives
/// restarts. Local devnets can set `KORA_RUNTIME_DIR` to put consensus journals
/// on tmpfs and avoid Docker-volume fsync latency.
#[must_use]
pub fn runtime_storage_directory(data_dir: &Path) -> PathBuf {
    runtime_storage_directory_from(data_dir, std::env::var_os(RUNTIME_DIR_ENV))
}

fn runtime_storage_directory_from(data_dir: &Path, override_dir: Option<OsString>) -> PathBuf {
    match override_dir {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => data_dir.join("runtime"),
    }
}

const fn block_codec_cfg(config: &kora_config::ConsensusBlockCodecConfig) -> BlockCfg {
    BlockCfg {
        max_txs: config.max_txs.get(),
        tx: TxCfg { max_tx_bytes: config.max_tx_bytes.get() },
    }
}

fn seed_genesis_block_index(index: &BlockIndex, genesis: &Block, gas_limit: u64) {
    index.insert_block(
        IndexedBlock {
            hash: genesis.id().0,
            number: 0,
            parent_hash: genesis.parent.0,
            state_root: genesis.state_root.0,
            timestamp: 0,
            gas_limit,
            gas_used: 0,
            base_fee_per_gas: Some(0),
            transaction_hashes: Vec::new(),
        },
        Vec::new(),
        Vec::new(),
    );
}

fn seed_hash(seed: impl commonware_codec::Encode) -> B256 {
    keccak256(seed.encode())
}

fn index_recovered_block(
    index: &kora_indexer::BlockIndex,
    block: &Block,
    provider: &RevmContextProvider,
) {
    let block_context = provider.context(block);
    let transaction_hashes = block.txs.iter().map(|tx| keccak256(&tx.bytes)).collect();
    let indexed_block = kora_indexer::IndexedBlock {
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
    block_index: &Arc<kora_indexer::BlockIndex>,
    finalized_blocks: &FB,
    finalizations_by_height: &FC,
    provider: &RevmContextProvider,
    data_dir: &Path,
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

            index_recovered_block(block_index, &block, provider);
            head = Some(block);
            recovered += 1;
        }
    }

    if let Some(ref head) = head {
        // Validate the commit marker against the archive head to detect
        // potential QMDB inconsistencies from a previous crash.
        validate_commit_marker(data_dir, head);

        ledger.restore_persisted_snapshot(head).await;
        info!(
            height = head.height,
            blocks = recovered,
            "recovered finalized ledger head from archive"
        );
    }

    Ok(())
}

/// Compare the on-disk commit marker against the archive head block.
///
/// This is a best-effort diagnostic check. A missing marker (fresh node or
/// upgrade from a pre-marker build) is benign and logged at info level. A
/// mismatch means QMDB may not contain the state corresponding to the
/// archive head and is logged as a warning so operators can investigate.
fn validate_commit_marker(data_dir: &Path, archive_head: &Block) {
    let marker_digest = crate::commit_marker::read_commit_marker(data_dir);
    let head_digest = archive_head.commitment();

    match marker_digest {
        None => {
            info!(
                archive_head_height = archive_head.height,
                "no commit marker found; this is expected for fresh nodes or \
                 first startup after upgrade"
            );
        }
        Some(marker) if marker == head_digest => {
            info!(
                archive_head_height = archive_head.height,
                "commit marker matches archive head; QMDB state is consistent"
            );
        }
        Some(marker) => {
            warn!(
                archive_head_height = archive_head.height,
                marker_digest = %hex::encode(marker.as_ref()),
                head_digest = %hex::encode(head_digest.as_ref()),
                "commit marker does not match archive head; QMDB may be behind \
                 or inconsistent. The node will proceed but state may diverge. \
                 Consider re-syncing from a trusted snapshot if issues arise."
            );
        }
    }
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
    block_index: Arc<BlockIndex>,
}

impl RevmContextProvider {
    /// Collect recent block hashes from the block index for the BLOCKHASH opcode.
    fn recent_block_hashes(&self, current_height: u64) -> std::collections::HashMap<u64, B256> {
        self.block_index.recent_block_hashes(current_height)
    }
}

impl BlockContextProvider for RevmContextProvider {
    fn context(&self, block: &Block) -> BlockContext {
        let header = Header {
            number: block.height,
            timestamp: block.timestamp,
            gas_limit: self.gas_limit,
            beneficiary: Address::ZERO,
            base_fee_per_gas: Some(0),
            ..Default::default()
        };
        let recent_hashes = self.recent_block_hashes(block.height);
        BlockContext::new(header, B256::ZERO, block.prevrandao)
            .with_recent_block_hashes(recent_hashes)
    }
}

fn spawn_ledger_observers<S: Spawner>(service: LedgerService, spawner: S, data_dir: PathBuf) {
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
                    if let Err(e) = crate::commit_marker::write_commit_marker(&data_dir, &digest) {
                        warn!(
                            error = %e,
                            ?digest,
                            "failed to write commit marker after persist"
                        );
                    }
                }
            }
        }
    });
}

fn spawn_txpool_cleanup(pool: TransactionPool, context: cw_tokio::Context) {
    context.with_label("txpool-cleanup").shared(false).spawn(move |ctx| async move {
        loop {
            ctx.sleep(TXPOOL_CLEANUP_INTERVAL).await;
            let removed = pool.cleanup();
            if removed > 0 {
                debug!(removed, "expired transactions cleaned from txpool");
            }
        }
    });
}

/// Monitor critical consensus infrastructure tasks for unexpected termination.
///
/// Each of the three handles (`engine`, `marshal`, `broadcast`) wraps a
/// long-lived actor that must never exit while the node is running.  If any of
/// them resolves it means the actor either panicked (the commonware runtime
/// catches panics and returns [`commonware_runtime::Error::Exited`]) or the
/// runtime context was shut down.  In either case the node can no longer make
/// progress on consensus, so we log an error and abort the process.
fn spawn_consensus_monitor(
    context: cw_tokio::Context,
    engine_handle: RuntimeHandle<()>,
    marshal_handle: RuntimeHandle<()>,
    broadcast_handle: RuntimeHandle<()>,
) {
    spawn_task_watchdog(&context, "consensus_engine", engine_handle);
    spawn_task_watchdog(&context, "marshal_actor", marshal_handle);
    spawn_task_watchdog(&context, "broadcast_engine", broadcast_handle);
}

/// Spawn a watchdog that awaits a critical task handle and aborts the process
/// if the task ever terminates.  Under normal operation the handle never
/// resolves; if it does, consensus is irrecoverably broken.
fn spawn_task_watchdog(context: &cw_tokio::Context, name: &'static str, handle: RuntimeHandle<()>) {
    context.with_label(name).shared(true).spawn(move |_| async move {
        match handle.await {
            Ok(()) => {
                error!(task = name, "critical task exited cleanly — this should never happen for a long-lived consensus actor");
            }
            Err(commonware_runtime::Error::Exited) => {
                error!(task = name, "critical task panicked (runtime caught panic and returned Error::Exited)");
            }
            Err(commonware_runtime::Error::Closed) => {
                warn!(task = name, "critical task terminated because the runtime context was shut down");
            }
            Err(ref e) => {
                error!(task = name, error = %e, error_debug = ?e, "critical task failed with unexpected error");
            }
        }
        error!(
            task = name,
            "consensus infrastructure is dead, aborting process for supervisor restart"
        );
        std::process::abort();
    });
}

/// Production validator node runner.
#[derive(Clone, Debug)]
pub struct ProductionRunner {
    /// Threshold signing scheme.
    pub scheme: ThresholdScheme,
    /// Chain ID.
    pub chain_id: u64,
    /// Bootstrap configuration.
    pub bootstrap: BootstrapConfig,
    /// Storage partition prefix.
    pub partition_prefix: String,
    /// Optional RPC configuration (state, bind address).
    pub rpc_config: Option<(kora_rpc::NodeState, std::net::SocketAddr)>,
    /// Optional Prometheus metrics server address.
    pub metrics_addr: Option<std::net::SocketAddr>,
    /// Secondary peers authorized to follow validator traffic without participating in consensus.
    pub secondary_peers: Vec<Peer>,
}

impl ProductionRunner {
    /// Create a new production runner.
    ///
    /// The gas limit is sourced exclusively from `config.execution.gas_limit`
    /// at runtime, so it is not accepted here.
    pub fn new(scheme: ThresholdScheme, chain_id: u64, bootstrap: BootstrapConfig) -> Self {
        Self {
            scheme,
            chain_id,
            bootstrap,
            partition_prefix: PARTITION_PREFIX.to_string(),
            rpc_config: None,
            metrics_addr: None,
            secondary_peers: Vec::new(),
        }
    }

    /// Configure RPC server.
    #[must_use]
    pub fn with_rpc(mut self, state: kora_rpc::NodeState, addr: std::net::SocketAddr) -> Self {
        self.rpc_config = Some((state, addr));
        self
    }

    /// Configure Prometheus metrics server address.
    #[must_use]
    pub const fn with_metrics_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.metrics_addr = Some(addr);
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

        let runtime_dir = runtime_storage_directory(&config.data_dir);
        info!(runtime_dir = %runtime_dir.display(), "Starting Commonware runtime");
        let executor =
            cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(runtime_dir));
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

            tokio::signal::ctrl_c().await.ok();
            info!("Received shutdown signal, stopping...");
            Ok::<(), RunnerError>(())
        })
    }
}

impl NodeRunner for ProductionRunner {
    type Transport = NetworkTransport<Peer, cw_tokio::Context>;
    type Handle = LedgerService;
    type Error = RunnerError;

    async fn run(&self, ctx: NodeRunContext<Self::Transport>) -> Result<Self::Handle, Self::Error> {
        let (context, config, mut transport) = ctx.into_parts();
        let gas_limit = config.execution.gas_limit;
        let simplex_config = config.consensus.simplex;

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
        let block_cfg = block_codec_cfg(&config.consensus.block_codec);
        let partition_prefix = &self.partition_prefix;
        let strategy = context
            .create_strategy(NZUsize!(2))
            .map_err(|e| anyhow::anyhow!("failed to create signature strategy: {e}"))?;

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
        let state = LedgerView::init_with_genesis_options(
            context.with_label("state"),
            format!("{}-qmdb", self.partition_prefix),
            self.bootstrap.genesis_alloc.clone(),
            !has_finalized_history,
            self.bootstrap.genesis_timestamp,
        )
        .await
        .context("init qmdb")?;

        let pending_tx_broadcast =
            self.rpc_config.as_ref().map(|_| kora_rpc::pending_tx_channel().0);
        let mempool_broadcast =
            self.rpc_config.as_ref().map(|_| kora_rpc::mempool_event_channel().0);
        let ledger = LedgerService::new(state.clone());
        let block_index = Arc::new(BlockIndex::new());
        seed_genesis_block_index(&block_index, &ledger.genesis_block(), gas_limit);
        spawn_ledger_observers(ledger.clone(), context.clone(), config.data_dir.clone());
        let txpool = ledger.txpool().await;
        spawn_txpool_cleanup(txpool.clone(), context.clone());

        let context_provider = RevmContextProvider { gas_limit, block_index: block_index.clone() };
        recover_finalized_state(
            &ledger,
            &block_index,
            &finalized_blocks,
            &finalizations_by_height,
            &context_provider,
            &config.data_dir,
        )
        .await
        .context("recover finalized state")?;

        if let Some((node_state, addr)) = &self.rpc_config {
            let peer_count = self.scheme.participants().len().saturating_sub(1) as u64;
            node_state.set_peer_count(peer_count);

            let qmdb_state = state.qmdb_state().await;
            let rpc_executor = Arc::new(RevmExecutor::new(self.chain_id));
            let indexed_provider =
                kora_rpc::IndexedStateProvider::new(block_index.clone(), qmdb_state, rpc_executor);
            let tx_ledger = ledger.clone();
            let tx_state = state.qmdb_state().await;
            let chain_id = self.chain_id;
            let tx_pool = txpool.clone();
            let tx_submit: kora_rpc::TxSubmitCallback = Arc::new(move |data| {
                let ledger = tx_ledger.clone();
                let state = tx_state.clone();
                let pool = tx_pool.clone();
                Box::pin(async move {
                    let tx = Tx::new(data);
                    let tx_id = tx.id();
                    let validator =
                        TransactionValidator::new(chain_id, state, PoolConfig::default())
                            .with_pool(pool);
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
            let mut rpc = kora_rpc::RpcServer::with_state_provider(
                node_state.clone(),
                *addr,
                self.chain_id,
                indexed_provider,
            )
            .with_tx_submit(tx_submit)
            .with_txpool(txpool.clone())
            .with_peer_count(peer_count);
            if let Some(sender) = pending_tx_broadcast.clone() {
                rpc = rpc.with_pending_tx_broadcast(sender);
            }
            if let Some(sender) = mempool_broadcast.clone() {
                rpc = rpc.with_mempool_broadcast(sender);
            }
            drop(rpc.start());
            info!(addr = %addr, "RPC server started with live state provider");
        }

        if let Some(metrics_addr) = self.metrics_addr {
            let metrics_context = context.clone();
            context.with_label("metrics").shared(true).spawn(move |_| async move {
                let app = axum::Router::new().route(
                    "/metrics",
                    axum::routing::get(move || {
                        let body = metrics_context.encode();
                        async move {
                            (
                                axum::http::StatusCode::OK,
                                [(
                                    axum::http::header::CONTENT_TYPE,
                                    "application/openmetrics-text; version=1.0.0; charset=utf-8",
                                )],
                                body,
                            )
                        }
                    }),
                );

                let listener = match tokio::net::TcpListener::bind(metrics_addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        error!(addr = %metrics_addr, error = %e, "Failed to bind metrics server");
                        return;
                    }
                };

                info!(addr = %metrics_addr, "Starting metrics server");
                if let Err(e) = axum::serve(listener, app).await {
                    error!(error = %e, "Metrics server error");
                }
            });
        }

        let validator_key = config
            .validator_key()
            .map_err(|e| anyhow::anyhow!("failed to load validator key: {}", e))?;
        let my_pk = commonware_cryptography::Signer::public_key(&validator_key);

        let finalized_executor = RevmExecutor::new(self.chain_id);
        let mut finalized_reporter = FinalizedReporter::new(
            ledger.clone(),
            context.clone(),
            finalized_executor,
            context_provider,
        )
        .with_block_index(block_index);
        if let Some(sender) = mempool_broadcast {
            finalized_reporter = finalized_reporter.with_mempool_broadcast(sender);
        }

        // Initialize the selfdestruct GC log for tracking orphaned storage.
        match kora_reporters::SelfdestructGcLog::open(&config.data_dir) {
            Ok(gc_log) => {
                info!(
                    path = %config.data_dir.display(),
                    "Opened selfdestruct GC log for tracking orphaned storage"
                );
                finalized_reporter = finalized_reporter.with_gc_log(Arc::new(gc_log));
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to open selfdestruct GC log; selfdestructed addresses will not be tracked"
                );
            }
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
        let broadcast_handle = broadcast_engine.start(transport.marshal.blocks);

        let (actor, marshal_mailbox, _last_processed_height) =
            kora_marshal::ActorInitializer::init_with_strategy::<_, Block, _, _, _, Exact, _>(
                context.clone(),
                finalizations_by_height,
                finalized_blocks,
                scheme_provider,
                page_cache.clone(),
                block_cfg,
                strategy.clone(),
            )
            .await;
        let marshal_handle = actor.start(finalized_reporter, buffer, resolver);

        let epocher = FixedEpocher::new(NZU64!(EPOCH_LENGTH));
        let executor = RevmExecutor::new(self.chain_id);
        let mut app = RevmApplication::<ThresholdScheme, _>::new(
            ledger.clone(),
            executor,
            block_cfg.max_txs,
            gas_limit,
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
            if !ledger.submit_tx(tx.clone()).await {
                warn!("failed to submit bootstrap transaction to mempool");
            }
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
                strategy,
                partition: self.partition_prefix.clone(),
                mailbox_size: MAILBOX_SIZE,
                epoch: Epoch::zero(),
                replay_buffer: simplex_config.replay_buffer_bytes,
                write_buffer: simplex_config.write_buffer_bytes,
                leader_timeout: Duration::from_secs(simplex_config.leader_timeout_secs.get()),
                certification_timeout: Duration::from_secs(
                    simplex_config.certification_timeout_secs.get(),
                ),
                timeout_retry: Duration::from_secs(simplex_config.timeout_retry_secs.get()),
                fetch_timeout: Duration::from_secs(simplex_config.fetch_timeout_secs.get()),
                activity_timeout: ViewDelta::new(simplex_config.activity_timeout_views.get()),
                skip_timeout: ViewDelta::new(simplex_config.skip_timeout_views.get()),
                fetch_concurrent: simplex_config.fetch_concurrent.get(),
                page_cache,
                forwarding: simplex::ForwardingPolicy::SilentLeader,
            },
        );
        let engine_handle = engine.start(
            transport.simplex.votes,
            transport.simplex.certs,
            transport.simplex.resolver,
        );

        spawn_consensus_monitor(context, engine_handle, marshal_handle, broadcast_handle);

        info!("Validator started successfully");
        Ok(ledger)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use kora_config::ConsensusBlockCodecConfig;
    use kora_domain::{BlockId, StateRoot};

    use super::*;

    #[test]
    fn seed_genesis_block_index_indexes_real_genesis_metadata() {
        let index = BlockIndex::new();
        let genesis = Block {
            parent: BlockId(B256::repeat_byte(0x11)),
            height: 0,
            timestamp: 0,
            prevrandao: B256::repeat_byte(0x22),
            state_root: StateRoot(B256::repeat_byte(0x33)),
            txs: Vec::new(),
        };
        let gas_limit = 45_000_000;

        seed_genesis_block_index(&index, &genesis, gas_limit);

        let indexed = index.get_block_by_number(0).expect("genesis indexed");
        assert_eq!(indexed.hash, genesis.id().0);
        assert_eq!(indexed.number, 0);
        assert_eq!(indexed.parent_hash, genesis.parent.0);
        assert_eq!(indexed.state_root, genesis.state_root.0);
        assert_eq!(indexed.timestamp, 0);
        assert_eq!(indexed.gas_limit, gas_limit);
        assert_eq!(indexed.gas_used, 0);
        assert_eq!(indexed.transaction_hashes, Vec::<B256>::new());
        assert_eq!(index.get_block_by_hash(&genesis.id().0).expect("genesis by hash").number, 0);
    }

    #[test]
    fn block_codec_cfg_uses_consensus_config() {
        let config = ConsensusBlockCodecConfig {
            max_txs: NonZeroUsize::new(512).unwrap(),
            max_tx_bytes: NonZeroUsize::new(4096).unwrap(),
        };

        let block_cfg = block_codec_cfg(&config);

        assert_eq!(block_cfg.max_txs, 512);
        assert_eq!(block_cfg.tx.max_tx_bytes, 4096);
    }

    #[test]
    fn runtime_storage_directory_defaults_under_data_dir() {
        let data_dir = PathBuf::from("/var/lib/kora");

        assert_eq!(
            runtime_storage_directory_from(&data_dir, None),
            PathBuf::from("/var/lib/kora/runtime")
        );
    }

    #[test]
    fn runtime_storage_directory_ignores_empty_override() {
        let data_dir = PathBuf::from("/var/lib/kora");

        assert_eq!(
            runtime_storage_directory_from(&data_dir, Some(OsString::new())),
            PathBuf::from("/var/lib/kora/runtime")
        );
    }

    #[test]
    fn runtime_storage_directory_uses_override() {
        let data_dir = PathBuf::from("/var/lib/kora");

        assert_eq!(
            runtime_storage_directory_from(&data_dir, Some(OsString::from("/runtime"))),
            PathBuf::from("/runtime")
        );
    }
}
