//! REVM-based consensus application implementation.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, UNIX_EPOCH},
};

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes};
use commonware_consensus::{
    Application, Block as _, VerifyingApplication,
    marshal::ancestry::{AncestorStream, BlockProvider},
    simplex::types::Context,
};
use commonware_cryptography::{Committable as _, certificate::Scheme as CertScheme};
use commonware_runtime::{Clock, Metrics, Spawner};
use futures::StreamExt;
use kora_consensus::{BlockExecution, SnapshotStore, components::InMemorySnapshotStore};
use kora_domain::{Block, ConsensusDigest};
use kora_executor::{BlockContext, BlockExecutor};
use kora_ledger::LedgerService;
use kora_metrics::AppMetrics;
use kora_overlay::OverlayState;
use kora_qmdb_ledger::QmdbState;
use kora_rpc::NodeState;
use rand::Rng;
use tracing::{debug, error, trace, warn};

/// Maximum number of attempts to poll for a parent snapshot before giving up.
///
/// Each attempt sleeps for [`SNAPSHOT_POLL_INTERVAL`], so the total wait is at
/// most `SNAPSHOT_POLL_ATTEMPTS * SNAPSHOT_POLL_INTERVAL` (50 ms by default).
const SNAPSHOT_POLL_ATTEMPTS: u32 = 5;

/// Duration to sleep between successive parent-snapshot poll attempts.
const SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Maximum number of unfinalized blocks a leader may be ahead of the last
/// finalized height before it voluntarily skips its proposal turn.  This
/// prevents a single fast leader from racing too far ahead of finalization,
/// which can cascade into snapshot-miss failures for other validators.
const MAX_PROPOSAL_LAG: u64 = 8;

fn unix_timestamp_secs<Env: Clock>(env: &Env) -> u64 {
    env.current().duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).unwrap_or(0)
}

/// Number of blocks behind the tip at which we consider the node to be
/// "catching up" and allow verify_block to trust finalized blocks without
/// re-executing them against a parent snapshot.
const CATCH_UP_THRESHOLD: u64 = 2;

/// REVM-based consensus application.
#[derive(Clone)]
pub struct RevmApplication<S, E> {
    ledger: LedgerService,
    executor: E,
    max_txs: usize,
    gas_limit: u64,
    node_state: Option<NodeState>,
    metrics: Option<AppMetrics>,
    /// Height of the HEAD block that was restored from the archive during
    /// startup recovery. Used to detect whether the node is still catching
    /// up: if a block's height is significantly greater than this value and
    /// its parent snapshot is missing, we trust the finality certificate
    /// instead of returning `false` (which the resolver would interpret as
    /// "malicious peer" and permanently block them).
    recovered_height: Arc<AtomicU64>,
    _scheme: std::marker::PhantomData<S>,
}

impl<S, E> std::fmt::Debug for RevmApplication<S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RevmApplication")
            .field("max_txs", &self.max_txs)
            .field("gas_limit", &self.gas_limit)
            .field("metrics", &self.metrics.is_some())
            .field("recovered_height", &self.recovered_height.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<S, E> RevmApplication<S, E>
where
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes> + Clone,
{
    /// Create a new REVM application.
    pub fn new(ledger: LedgerService, executor: E, max_txs: usize, gas_limit: u64) -> Self {
        Self {
            ledger,
            executor,
            max_txs,
            gas_limit,
            node_state: None,
            metrics: None,
            recovered_height: Arc::new(AtomicU64::new(0)),
            _scheme: std::marker::PhantomData,
        }
    }

    /// Set the node state for tracking proposal metrics.
    #[must_use]
    pub fn with_node_state(mut self, state: NodeState) -> Self {
        self.node_state = Some(state);
        self
    }

    /// Attach application-level metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: AppMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set the height of the HEAD block that was recovered from the archive.
    ///
    /// This is used to detect catch-up mode: when the node is behind the
    /// network and parent snapshots are unavailable, blocks whose height
    /// exceeds this value by more than [`CATCH_UP_THRESHOLD`] are trusted
    /// based on their finality certificate rather than being rejected.
    #[must_use]
    pub fn with_recovered_height(self, height: u64) -> Self {
        self.recovered_height.store(height, Ordering::Relaxed);
        self
    }

    fn block_context(&self, height: u64, timestamp: u64, prevrandao: B256) -> BlockContext {
        let header = Header {
            number: height,
            timestamp,
            gas_limit: self.gas_limit,
            beneficiary: Address::ZERO,
            base_fee_per_gas: Some(kora_config::INITIAL_BASE_FEE),
            ..Default::default()
        };
        BlockContext::new(header, B256::ZERO, prevrandao)
    }

    async fn get_prevrandao(&self, parent_digest: ConsensusDigest) -> B256 {
        self.ledger.seed_for_parent(parent_digest).await.unwrap_or(B256::ZERO)
    }

    async fn build_block(&self, parent: &Block, timestamp: u64) -> Option<Block> {
        use kora_consensus::Mempool as _;

        let start = Instant::now();
        let parent_digest = parent.commitment();

        // Wait briefly for the parent snapshot to become available.
        //
        // Consensus can advance views faster than the execution layer
        // produces snapshots.  Rather than immediately returning `None`
        // (which nullifies the view), we poll for up to
        // `SNAPSHOT_POLL_ATTEMPTS * SNAPSHOT_POLL_INTERVAL` (50 ms).
        // In the common case the snapshot arrives within the first few
        // milliseconds, converting what would have been a nullified view
        // into a successful proposal.
        let parent_snapshot = {
            let mut snap = self.ledger.parent_snapshot(parent_digest).await;
            let mut poll_count = 0u32;
            let poll_start = Instant::now();
            while snap.is_none() && poll_count < SNAPSHOT_POLL_ATTEMPTS {
                tokio::time::sleep(SNAPSHOT_POLL_INTERVAL).await;
                poll_count += 1;
                snap = self.ledger.parent_snapshot(parent_digest).await;
            }
            match snap {
                Some(s) => {
                    if poll_count > 0 {
                        debug!(
                            parent_height = parent.height,
                            ?parent_digest,
                            poll_count,
                            wait_ms = poll_start.elapsed().as_millis(),
                            "build_block: parent snapshot arrived after polling"
                        );
                    }
                    s
                }
                None => {
                    warn!(
                        parent_height = parent.height,
                        ?parent_digest,
                        poll_count,
                        wait_ms = poll_start.elapsed().as_millis(),
                        "build_block: parent snapshot not found after polling — \
                         node has not yet processed this parent block"
                    );
                    return None;
                }
            }
        };
        let snapshot_elapsed = start.elapsed();

        let (_, mempool, snapshots) = self.ledger.proposal_components().await;
        let excluded = match self.collect_pending_tx_ids(&snapshots, parent_digest) {
            Some(ids) => ids,
            None => {
                // The snapshot chain has a gap — we cannot determine which
                // transactions were already included in recent blocks.
                // Building with an incomplete excluded set risks duplicate
                // transactions, so we nullify this round instead.
                return None;
            }
        };
        let mempool_len = mempool.len();
        let excluded_len = excluded.len();
        let txs = mempool.build(self.max_txs, &excluded);

        // Diagnostic: when the producer builds an empty block while there are
        // unincluded txs in the mempool, something is wrong (e.g. RPC tx_submit
        // not wired, the excluded set over-collecting, or max_txs misconfigured).
        // Log enough state to tell which.
        if txs.is_empty() && mempool_len > excluded_len {
            warn!(
                mempool_len,
                excluded_len,
                max_txs = self.max_txs,
                "build_block: mempool has unincluded txs but produced empty block"
            );
        } else {
            trace!(
                mempool_len,
                excluded_len,
                drained = txs.len(),
                max_txs = self.max_txs,
                "build_block: mempool drain"
            );
        }

        let prevrandao = self.get_prevrandao(parent_digest).await;
        let height = parent.height + 1;
        let context = self.block_context(height, timestamp, prevrandao);
        let txs_bytes: Vec<Bytes> = txs.iter().map(|tx| tx.bytes.clone()).collect();

        let exec_start = Instant::now();
        let outcome = match self.executor.execute(&parent_snapshot.state, &context, &txs_bytes) {
            Ok(outcome) => outcome,
            Err(err) => {
                error!(
                    parent = ?parent_digest,
                    height,
                    txs = txs.len(),
                    gas_limit = self.gas_limit,
                    error = %err,
                    error_debug = ?err,
                    "build_block: block execution failed — \
                     this may indicate a bad transaction, OOM, or state corruption"
                );
                return None;
            }
        };
        let exec_elapsed = exec_start.elapsed();

        let root_start = Instant::now();
        let state_root =
            match self.ledger.compute_root_from_store(parent_digest, outcome.changes.clone()).await
            {
                Ok(root) => root,
                Err(err) => {
                    error!(
                        parent = ?parent_digest,
                        height,
                        error = %err,
                        error_debug = ?err,
                        "build_block: QMDB state root computation failed — \
                         this may indicate a storage I/O error or inconsistent state"
                    );
                    return None;
                }
            };
        let root_elapsed = root_start.elapsed();

        let block = Block { parent: parent.id(), height, timestamp, prevrandao, state_root, txs };

        let block_digest = block.commitment();

        let total_elapsed = start.elapsed();

        if let Some(ref m) = self.metrics {
            m.block_build_time.observe(total_elapsed.as_secs_f64());
            m.block_txs_included.set(block.txs.len() as i64);
        }

        debug!(
            ?block_digest,
            height,
            timestamp,
            txs = block.txs.len(),
            snapshot_ms = snapshot_elapsed.as_millis(),
            exec_ms = exec_elapsed.as_millis(),
            root_ms = root_elapsed.as_millis(),
            total_ms = total_elapsed.as_millis(),
            "built block"
        );
        Some(block)
    }

    /// Check whether the node is in catch-up mode.
    ///
    /// Returns `true` when the requested block height is far enough ahead of
    /// the height we recovered from the archive, indicating that we are still
    /// syncing up to the live network.
    fn is_catching_up(&self, block_height: u64) -> bool {
        let recovered = self.recovered_height.load(Ordering::Relaxed);
        // If recovered_height is 0 we have never recovered (fresh node), so
        // we are not catching up.
        recovered > 0 && block_height > recovered.saturating_add(CATCH_UP_THRESHOLD)
    }

    async fn verify_block(&self, block: &Block) -> bool {
        let start = Instant::now();
        let digest = block.commitment();
        let parent_digest = block.parent();

        if self.ledger.query_state_root(digest).await.is_some() {
            trace!(?digest, "block already verified");
            return true;
        }

        let parent_snapshot = match self.ledger.parent_snapshot(parent_digest).await {
            Some(snap) => snap,
            None => {
                // Parent snapshot is missing. During normal operation this
                // means we received a genuinely invalid or out-of-order
                // block. But after a restart the snapshot cache only
                // contains the HEAD, so blocks whose parent we haven't
                // processed yet will fail here.
                //
                // If we are still catching up (block height is well ahead
                // of our recovered height), trust the finality certificate
                // and restore the block as a persisted snapshot so that
                // subsequent blocks can find their parent.
                if self.is_catching_up(block.height) {
                    warn!(
                        ?digest,
                        ?parent_digest,
                        height = block.height,
                        recovered_height = self.recovered_height.load(Ordering::Relaxed),
                        "verify_block: parent snapshot missing during catch-up; \
                         trusting finality certificate"
                    );
                    // Create a persisted snapshot for this block using the
                    // current QMDB state. This is safe because the block
                    // was already finalized by consensus (it has a valid
                    // finality certificate verified by the resolver).
                    // The FinalizedReporter will re-execute and properly
                    // persist the block when it arrives through the
                    // finalization pipeline.
                    self.ledger.restore_persisted_snapshot(block).await;
                    // Update recovered_height so the node eventually exits
                    // catch-up mode once it has caught up.
                    self.recovered_height.fetch_max(block.height, Ordering::Relaxed);
                    return true;
                }

                warn!(?digest, ?parent_digest, height = block.height, "missing parent snapshot");
                return false;
            }
        };
        let snapshot_elapsed = start.elapsed();

        let context = self.block_context(block.height, block.timestamp, block.prevrandao);
        let exec_start = Instant::now();
        let execution =
            match BlockExecution::execute(&parent_snapshot, &self.executor, &context, &block.txs)
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    warn!(?digest, error = ?err, "execution failed");
                    return false;
                }
            };
        let exec_elapsed = exec_start.elapsed();

        let root_start = Instant::now();
        let state_root = match self
            .ledger
            .compute_root_from_store(parent_digest, execution.outcome.changes.clone())
            .await
        {
            Ok(root) => root,
            Err(err) => {
                warn!(?digest, error = ?err, "compute root failed");
                return false;
            }
        };
        let root_elapsed = root_start.elapsed();

        if state_root != block.state_root {
            warn!(
                ?digest,
                expected = ?block.state_root,
                computed = ?state_root,
                "state root mismatch"
            );
            return false;
        }

        let merged_changes = parent_snapshot.state.merge_changes(execution.outcome.changes.clone());
        let next_state = OverlayState::new(parent_snapshot.state.base(), merged_changes);

        self.ledger
            .insert_snapshot(
                digest,
                parent_digest,
                next_state,
                state_root,
                execution.outcome.changes,
                &block.txs,
            )
            .await;

        // Once we successfully verify a block, update the recovered height
        // so the catch-up window advances with normal progress.
        self.recovered_height.fetch_max(block.height, Ordering::Relaxed);

        let total_elapsed = start.elapsed();
        debug!(
            ?digest,
            height = block.height,
            txs = block.txs.len(),
            snapshot_ms = snapshot_elapsed.as_millis(),
            exec_ms = exec_elapsed.as_millis(),
            root_ms = root_elapsed.as_millis(),
            total_ms = total_elapsed.as_millis(),
            "verified block"
        );
        true
    }

    /// Collect transaction IDs from unpersisted ancestor snapshots.
    ///
    /// Returns `None` if the snapshot chain has a gap (a snapshot was evicted
    /// before we could read it). In that case the caller **must not** build a
    /// block, because we cannot guarantee the excluded set is complete and
    /// would risk including duplicate transactions.
    fn collect_pending_tx_ids(
        &self,
        snapshots: &InMemorySnapshotStore<OverlayState<QmdbState>>,
        from: ConsensusDigest,
    ) -> Option<BTreeSet<kora_consensus::TxId>> {
        let mut excluded = BTreeSet::new();
        let mut current = Some(from);

        while let Some(digest) = current {
            if snapshots.is_persisted(&digest) {
                break;
            }
            let Some(snapshot) = snapshots.get(&digest) else {
                warn!(
                    ?digest,
                    collected_so_far = excluded.len(),
                    "snapshot chain gap during tx exclusion collection — \
                     refusing to build block to prevent duplicate transactions"
                );
                return None;
            };
            excluded.extend(snapshot.tx_ids.iter().copied());
            current = snapshot.parent;
        }

        Some(excluded)
    }
}

impl<Env, S, E> Application<Env> for RevmApplication<S, E>
where
    Env: Rng + Spawner + Metrics + Clock,
    S: CertScheme + Send + Sync + 'static,
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes> + Clone + Send + Sync + 'static,
{
    type SigningScheme = S;
    type Context = Context<ConsensusDigest, S::PublicKey>;
    type Block = Block;

    fn genesis(&mut self) -> impl std::future::Future<Output = Self::Block> + Send {
        async move { self.ledger.genesis_block() }
    }

    fn propose<A>(
        &mut self,
        context: (Env, Self::Context),
        mut ancestry: AncestorStream<A, Self::Block>,
    ) -> impl std::future::Future<Output = Option<Self::Block>> + Send
    where
        A: BlockProvider<Block = Self::Block>,
    {
        let node_state = self.node_state.clone();
        let env = context.0;
        async move {
            let start = Instant::now();
            let parent = ancestry.next().await?;
            let ancestry_elapsed = start.elapsed();

            // Proposal lag guard: if the tip is too far ahead of the last
            // finalized height, skip this proposal to let finalization catch
            // up.  This prevents a fast leader from building an unbounded
            // chain of unfinalized snapshots that other validators cannot
            // verify in time.
            if let Some(ref state) = node_state {
                let finalized = state.finalized_height();
                if parent.height > finalized + MAX_PROPOSAL_LAG {
                    warn!(
                        parent_height = parent.height,
                        finalized_height = finalized,
                        max_lag = MAX_PROPOSAL_LAG,
                        "skipping proposal: parent too far ahead of finalized height"
                    );
                    return None;
                }
            }

            let now_secs = unix_timestamp_secs(&env);
            let timestamp = match Block::next_timestamp(now_secs, parent.timestamp) {
                Some(ts) => ts,
                None => {
                    tracing::error!(
                        parent_timestamp = parent.timestamp,
                        "timestamp overflow: cannot produce a timestamp after parent"
                    );
                    return None;
                }
            };

            let build_start = Instant::now();
            let block = self.build_block(&parent, timestamp).await;
            let build_elapsed = build_start.elapsed();

            match block {
                Some(ref b) => {
                    if let Some(ref state) = node_state {
                        state.inc_proposed();
                    }
                    debug!(
                        height = b.height,
                        timestamp = b.timestamp,
                        ancestry_ms = ancestry_elapsed.as_millis(),
                        build_ms = build_elapsed.as_millis(),
                        total_ms = start.elapsed().as_millis(),
                        "propose complete"
                    );
                }
                None => {
                    warn!(
                        parent_height = parent.height,
                        parent_digest = ?parent.commitment(),
                        build_ms = build_elapsed.as_millis(),
                        "propose failed: build_block returned None \
                         (likely missing parent snapshot — node may still be catching up)"
                    );
                }
            }

            block
        }
    }
}

impl<Env, S, E> VerifyingApplication<Env> for RevmApplication<S, E>
where
    Env: Rng + Spawner + Metrics + Clock,
    S: CertScheme + Send + Sync + 'static,
    E: BlockExecutor<OverlayState<QmdbState>, Tx = Bytes> + Clone + Send + Sync + 'static,
{
    fn verify<A>(
        &mut self,
        _context: (Env, Self::Context),
        mut ancestry: AncestorStream<A, Self::Block>,
    ) -> impl std::future::Future<Output = bool> + Send
    where
        A: BlockProvider<Block = Self::Block>,
    {
        async move {
            let start = Instant::now();

            // The ancestry stream yields tip-first (newest → oldest).
            // We only need to verify blocks that we haven't seen yet.
            // Collect blocks until we hit one we've already verified.
            let mut blocks_to_verify = Vec::new();
            while let Some(block) = ancestry.next().await {
                let digest = block.commitment();
                // Stop if we've already verified this block
                if self.ledger.query_state_root(digest).await.is_some() {
                    break;
                }
                blocks_to_verify.push(block);
            }
            let ancestry_elapsed = start.elapsed();

            if blocks_to_verify.is_empty() {
                // All blocks already verified
                trace!(ancestry_ms = ancestry_elapsed.as_millis(), "all blocks already verified");
                return true;
            }

            let block_count = blocks_to_verify.len();
            let tip_height = blocks_to_verify.first().map(|b| b.height).unwrap_or(0);

            // Verify from oldest (parent) to newest (tip)
            let verify_start = Instant::now();
            for block in blocks_to_verify.into_iter().rev() {
                if !self.verify_block(&block).await {
                    return false;
                }
            }
            let verify_elapsed = verify_start.elapsed();
            let total_elapsed = start.elapsed();

            debug!(
                tip_height,
                block_count,
                ancestry_ms = ancestry_elapsed.as_millis(),
                verify_ms = verify_elapsed.as_millis(),
                total_ms = total_elapsed.as_millis(),
                "verify complete"
            );

            true
        }
    }
}
