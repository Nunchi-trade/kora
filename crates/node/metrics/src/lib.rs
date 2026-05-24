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
    /// Number of transactions included in the most recently built block.
    pub block_txs_included: Gauge,

    // -- Finalization --
    /// Total number of finalization failures.
    pub finalization_failures: Counter,
    /// Total number of blocks successfully finalized.
    pub blocks_finalized: Counter,
}

/// Label set for metrics that carry a `reason` dimension.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct ReasonLabel {
    /// The rejection / error reason.
    pub reason: String,
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
            block_txs_included: Gauge::default(),
            finalization_failures: Counter::default(),
            blocks_finalized: Counter::default(),
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
            "Transactions in the most recently built block",
            self.block_txs_included.clone(),
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
