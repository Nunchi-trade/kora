//! Application-level Prometheus metrics for Kora nodes.
//!
//! Provides counters, gauges, and histograms for txpool, block building,
//! finalization, and RPC instrumentation. All metrics are registered with
//! the commonware runtime's `Metrics` registry so they appear on the
//! existing `/metrics` endpoint alongside SDK metrics.
#![doc(issue_tracker_base_url = "https://github.com/refcell/kora/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use prometheus_client::metrics::{
    counter::Counter, family::Family, gauge::Gauge, histogram::Histogram,
};

/// Default histogram buckets for block build time (seconds).
const BLOCK_BUILD_BUCKETS: [f64; 9] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// Default histogram buckets for EVM execution time (seconds).
///
/// Captures the time spent in the EVM executor (`BlockExecutor::execute`)
/// excluding proposal overhead (snapshot lookup, tx selection, state root
/// computation).  Most executions complete in under 10 ms; the higher
/// buckets detect pathological transactions or state-cache misses.
const EVM_EXEC_BUCKETS: [f64; 9] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// Default histogram buckets for snapshot poll wait time (seconds).
///
/// Captures the delay between "leader needs parent snapshot" and "snapshot
/// available".  Most waits resolve in under 5 ms; the higher buckets detect
/// CPU-contention-related stalls.
const SNAPSHOT_POLL_BUCKETS: [f64; 8] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15];

/// Default histogram buckets for QMDB persist duration (seconds).
const PERSIST_DURATION_BUCKETS: [f64; 9] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// Default histogram buckets for transactions included per block.
const BLOCK_TXS_BUCKETS: [f64; 10] = [0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0];

/// Application-level metrics for a Kora node.
///
/// Create with [`AppMetrics::new`] and register with
/// [`AppMetrics::register`] against any `commonware_runtime::Metrics`
/// implementor.
#[derive(Debug, Clone)]
pub struct AppMetrics {
    // -- Transaction Pool --
    /// Current total number of transactions in the pool.
    pub txpool_size: Gauge,
    /// Current number of pending (executable) transactions.
    pub txpool_pending: Gauge,
    /// Current number of queued (future-nonce) transactions.
    pub txpool_queued: Gauge,
    /// Total rejected transactions, labelled by reason.
    pub txpool_rejected: Family<ReasonLabel, Counter>,

    // -- Block Building --
    /// Histogram of block build durations in seconds.
    pub block_build_time: Histogram,
    /// Distribution of transactions included per block.
    pub block_txs_included: Histogram,
    /// Gas used in the most recently built block.
    pub block_gas_used: Gauge,

    // -- Proposal health --
    /// Total proposals skipped because the parent snapshot was not ready
    /// after the full poll window.  A rising count indicates the execution
    /// layer is consistently slower than the consensus layer.
    pub proposal_snapshot_misses: Counter,
    /// Total proposals skipped because the tip was too far ahead of the
    /// last finalized height (proposal lag guard).  A rising count means
    /// finalization is not keeping up with block production.
    pub proposal_lag_skips: Counter,
    /// Histogram of time spent waiting for the parent snapshot to become
    /// available during `build_block`, in seconds.  Only recorded when at
    /// least one poll attempt was needed (i.e. the snapshot was not
    /// immediately available).
    pub snapshot_poll_wait: Histogram,

    // -- Finalization --
    /// Total number of finalization failures.
    pub finalization_failures: Counter,
    /// Total number of blocks successfully finalized.
    pub blocks_finalized: Counter,
    /// Histogram of QMDB persist duration in seconds.
    pub persist_duration_seconds: Histogram,

    // -- Consensus State --
    /// Latest finalized block height.
    pub finalized_height: Gauge,
    /// Current consensus view number.
    pub current_view: Gauge,
    /// Total number of nullified consensus rounds.
    pub nullifications_total: Counter,

    // -- Network --
    /// Number of currently connected peers.
    pub peer_count: Gauge,

    // -- EVM Execution --
    /// Histogram of EVM execution time in seconds (excluding proposal
    /// overhead such as snapshot lookup, tx selection, and state root
    /// computation).  Recorded in both `build_block` and `verify_block`.
    pub evm_execution_seconds: Histogram,

    // -- RPC --
    /// Total number of JSON-RPC requests received (including rate-limited).
    pub rpc_requests_total: Counter,

    // -- Snapshot Store --
    /// Number of snapshots that have not yet been persisted to QMDB.
    ///
    /// A rising value under steady-state operation indicates the persistence
    /// pipeline is falling behind block production, which leads to unbounded
    /// memory growth and increasingly expensive chain walks.
    pub unpersisted_snapshot_depth: Gauge,
    /// Total number of snapshots currently held in the in-memory store
    /// (both persisted and unpersisted).
    pub snapshot_store_total: Gauge,

    // -- Transaction Gossip --
    /// Total transactions broadcast to peers via gossip.
    pub gossip_tx_broadcast: Counter,
    /// Total transactions received from peers via gossip.
    pub gossip_tx_received: Counter,
    /// Total gossip broadcast failures (send errors).
    pub gossip_tx_broadcast_failed: Counter,
    /// Total gossip transactions that failed validation.
    pub gossip_tx_invalid: Counter,

    // -- Equivocation --
    /// Total equivocation events detected, labelled by type
    /// (`conflicting_notarize`, `conflicting_finalize`, `nullify_finalize`).
    pub equivocations: Family<EquivocationTypeLabel, Counter>,
}

/// Label set for metrics that carry a `reason` dimension.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct ReasonLabel {
    /// The rejection / error reason.
    pub reason: String,
}

/// Label set for equivocation metrics, distinguishing the type of Byzantine fault.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct EquivocationTypeLabel {
    /// The equivocation type (`conflicting_notarize`, `conflicting_finalize`,
    /// `nullify_finalize`).
    pub r#type: String,
}

impl AppMetrics {
    /// Create a new set of application metrics (unregistered).
    #[must_use]
    pub fn new() -> Self {
        Self {
            txpool_size: Gauge::default(),
            txpool_pending: Gauge::default(),
            txpool_queued: Gauge::default(),
            txpool_rejected: Family::default(),
            block_build_time: Histogram::new(BLOCK_BUILD_BUCKETS),
            block_txs_included: Histogram::new(BLOCK_TXS_BUCKETS),
            block_gas_used: Gauge::default(),
            proposal_snapshot_misses: Counter::default(),
            proposal_lag_skips: Counter::default(),
            snapshot_poll_wait: Histogram::new(SNAPSHOT_POLL_BUCKETS),
            finalization_failures: Counter::default(),
            blocks_finalized: Counter::default(),
            persist_duration_seconds: Histogram::new(PERSIST_DURATION_BUCKETS),
            finalized_height: Gauge::default(),
            current_view: Gauge::default(),
            nullifications_total: Counter::default(),
            peer_count: Gauge::default(),
            evm_execution_seconds: Histogram::new(EVM_EXEC_BUCKETS),
            rpc_requests_total: Counter::default(),
            unpersisted_snapshot_depth: Gauge::default(),
            snapshot_store_total: Gauge::default(),
            gossip_tx_broadcast: Counter::default(),
            gossip_tx_received: Counter::default(),
            gossip_tx_broadcast_failed: Counter::default(),
            gossip_tx_invalid: Counter::default(),
            equivocations: Family::default(),
        }
    }

    /// Register all metrics with a commonware runtime `Metrics` provider.
    ///
    /// Call this once during node startup so that the metrics appear on the
    /// `/metrics` endpoint.
    pub fn register<M: MetricsRegister>(&self, registry: &M) {
        registry.register(
            "kora_txpool_size",
            "Current number of transactions in the pool",
            self.txpool_size.clone(),
        );
        registry.register(
            "kora_txpool_pending",
            "Current number of pending (executable) transactions",
            self.txpool_pending.clone(),
        );
        registry.register(
            "kora_txpool_queued",
            "Current number of queued (future-nonce) transactions",
            self.txpool_queued.clone(),
        );
        // NOTE: Do not add a `_total` suffix to counter names here.
        // The prometheus_client crate automatically appends `_total` to
        // counters per the OpenMetrics specification.
        registry.register(
            "kora_txpool_rejected",
            "Total rejected transactions by reason",
            self.txpool_rejected.clone(),
        );
        registry.register(
            "kora_block_build_time_seconds",
            "Block build duration in seconds",
            self.block_build_time.clone(),
        );
        registry.register(
            "kora_block_txs_included",
            "Distribution of transactions included per block",
            self.block_txs_included.clone(),
        );
        registry.register(
            "kora_block_gas_used",
            "Gas used in the most recently built block",
            self.block_gas_used.clone(),
        );
        registry.register(
            "kora_proposal_snapshot_misses",
            "Proposals skipped due to missing parent snapshot",
            self.proposal_snapshot_misses.clone(),
        );
        registry.register(
            "kora_proposal_lag_skips",
            "Proposals skipped due to finalization lag guard",
            self.proposal_lag_skips.clone(),
        );
        registry.register(
            "kora_snapshot_poll_wait_seconds",
            "Time waiting for parent snapshot during block build",
            self.snapshot_poll_wait.clone(),
        );
        registry.register(
            "kora_finalization_failures",
            "Total finalization failures",
            self.finalization_failures.clone(),
        );
        registry.register(
            "kora_blocks_finalized",
            "Total blocks successfully finalized",
            self.blocks_finalized.clone(),
        );
        registry.register(
            "kora_persist_duration_seconds",
            "QMDB persist duration in seconds",
            self.persist_duration_seconds.clone(),
        );
        registry.register(
            "kora_finalized_height",
            "Latest finalized block height",
            self.finalized_height.clone(),
        );
        registry.register(
            "kora_current_view",
            "Current consensus view number",
            self.current_view.clone(),
        );
        registry.register(
            "kora_nullifications",
            "Total nullified consensus rounds",
            self.nullifications_total.clone(),
        );
        registry.register(
            "kora_peer_count",
            "Number of currently connected peers",
            self.peer_count.clone(),
        );
        registry.register(
            "kora_evm_execution_seconds",
            "EVM execution time per block in seconds",
            self.evm_execution_seconds.clone(),
        );
        registry.register(
            "kora_rpc_requests",
            "Total JSON-RPC requests received",
            self.rpc_requests_total.clone(),
        );
        registry.register(
            "kora_unpersisted_snapshot_depth",
            "Number of in-memory snapshots not yet persisted to QMDB",
            self.unpersisted_snapshot_depth.clone(),
        );
        registry.register(
            "kora_snapshot_store_total",
            "Total snapshots currently held in the in-memory store",
            self.snapshot_store_total.clone(),
        );
        registry.register(
            "kora_gossip_tx_broadcast",
            "Total transactions broadcast to peers via gossip",
            self.gossip_tx_broadcast.clone(),
        );
        registry.register(
            "kora_gossip_tx_received",
            "Total transactions received from peers via gossip",
            self.gossip_tx_received.clone(),
        );
        registry.register(
            "kora_gossip_tx_broadcast_failed",
            "Total gossip broadcast failures",
            self.gossip_tx_broadcast_failed.clone(),
        );
        registry.register(
            "kora_gossip_tx_invalid",
            "Total gossip transactions that failed validation",
            self.gossip_tx_invalid.clone(),
        );
        registry.register(
            "kora_equivocations",
            "Total equivocation events detected by type",
            self.equivocations.clone(),
        );
    }
}

impl Default for AppMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait abstracting the `register` method from `commonware_runtime::Metrics`.
///
/// This avoids pulling the entire commonware-runtime dependency into this
/// leaf crate. The runtime context already implements this via the `Metrics`
/// trait; callers just need to provide a thin adapter (or use the blanket
/// implementation below).
pub trait MetricsRegister {
    /// Register a single metric.
    fn register<N: Into<String>, H: Into<String>>(
        &self,
        name: N,
        help: H,
        metric: impl prometheus_client::registry::Metric,
    );
}
