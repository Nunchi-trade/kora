use std::fmt;

use kora_domain::ConsensusDigest;
use kora_executor::ExecutionError;
use kora_ledger::LedgerError;
use thiserror::Error;

/// Errors that can occur when building a block proposal.
///
/// This type belongs in the runner crate because block building is an
/// application-layer concern that references runner-specific types such as
/// [`ExecutionError`] and [`LedgerError`].  Placing it in `kora_consensus`
/// would create a backwards dependency from the domain-agnostic consensus
/// crate onto application-specific types.
///
/// The variants distinguish three fundamentally different failure modes that
/// were previously all returned as `None` from `build_block()`, making it
/// impossible to distinguish expected behavior (catching up) from critical
/// errors (execution failure, storage I/O) in logs, metrics, or calling code.
#[derive(Debug, Error)]
pub enum BuildBlockError {
    /// The parent snapshot is not available in the in-memory store.
    ///
    /// This is expected during catch-up when the node has not yet processed
    /// the parent block.  It is transient and should resolve as the node
    /// catches up to the network.  Callers should log this at `debug` level,
    /// not `warn`, since it is a normal operational state.
    #[error(
        "parent snapshot not found (catching up): parent={parent_digest:?} height={parent_height}"
    )]
    CatchingUp {
        /// Digest of the parent block whose snapshot was not found.
        parent_digest: ConsensusDigest,
        /// Height of the parent block.
        parent_height: u64,
    },

    /// Block execution against the parent state failed.
    ///
    /// This could be transient (OOM) or indicate a problem with the
    /// transaction set (poisoned mempool entry) or state corruption.
    #[error("block execution failed at height {height}: {source}")]
    ExecutionFailed {
        /// The underlying executor error.
        #[source]
        source: ExecutionError,
        /// Digest of the parent block.
        parent_digest: ConsensusDigest,
        /// Height of the block being built.
        height: u64,
        /// Number of transactions in the attempted block.
        tx_count: usize,
    },

    /// The execution task panicked or was cancelled by the Tokio runtime.
    ///
    /// This wraps a `JoinError` from `tokio::task::spawn_blocking` and
    /// indicates a runtime-level failure rather than an application error.
    #[error("execution task join error at height {height}: {message}")]
    ExecutionJoinError {
        /// Human-readable description of the join error.
        message: String,
        /// Digest of the parent block.
        parent_digest: ConsensusDigest,
        /// Height of the block being built.
        height: u64,
    },

    /// Computing the QMDB state root from the execution changes failed.
    ///
    /// This typically indicates a ledger or storage-layer I/O error.
    #[error("state root computation failed at height {height}: {source}")]
    RootComputationFailed {
        /// The underlying ledger error.
        #[source]
        source: LedgerError,
        /// Digest of the parent block.
        parent_digest: ConsensusDigest,
        /// Height of the block being built.
        height: u64,
    },
}

impl BuildBlockError {
    /// Returns `true` if this error is an expected/transient condition (the
    /// node is catching up) that should be logged at `debug` level.
    pub const fn is_catching_up(&self) -> bool {
        matches!(self, Self::CatchingUp { .. })
    }

    /// Returns a static label suitable for use as a Prometheus metric label.
    ///
    /// Designed to complement Issue 19 (application-level metrics) so that
    /// `kora_proposal_failure_total{cause=<label>}` can distinguish catch-up
    /// from genuine errors.
    pub const fn metric_label(&self) -> &'static str {
        match self {
            Self::CatchingUp { .. } => "catching_up",
            Self::ExecutionFailed { .. } => "execution_failed",
            Self::ExecutionJoinError { .. } => "execution_join_error",
            Self::RootComputationFailed { .. } => "root_computation_failed",
        }
    }
}

/// Error type for node runner operations.
#[derive(Debug)]
pub struct RunnerError(pub anyhow::Error);

impl fmt::Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RunnerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<anyhow::Error> for RunnerError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use kora_consensus::ConsensusError;
    use kora_domain::ConsensusDigest;

    use super::*;

    fn dummy_digest() -> ConsensusDigest {
        ConsensusDigest::from([0u8; 32])
    }

    fn dummy_ledger_error() -> LedgerError {
        LedgerError::Consensus(ConsensusError::SnapshotNotFound(dummy_digest()))
    }

    // ── RunnerError tests ──────────────────────────────────────

    #[test]
    fn runner_error_display_shows_inner_message() {
        let inner = anyhow::anyhow!("test error message");
        let error = RunnerError(inner);
        assert_eq!(format!("{error}"), "test error message");
    }

    #[test]
    fn runner_error_debug_contains_runner_error() {
        let inner = anyhow::anyhow!("debug test");
        let error = RunnerError(inner);
        let debug_str = format!("{error:?}");
        assert!(debug_str.contains("RunnerError"));
    }

    #[test]
    fn runner_error_from_anyhow_preserves_message() {
        let inner = anyhow::anyhow!("original message");
        let error: RunnerError = inner.into();
        assert_eq!(format!("{error}"), "original message");
    }

    #[test]
    fn runner_error_source_returns_none_for_simple_error() {
        let inner = anyhow::anyhow!("simple error");
        let error = RunnerError(inner);
        assert!(error.source().is_none());
    }

    #[test]
    fn runner_error_source_delegates_to_anyhow() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let inner = anyhow::Error::from(io_err);
        let inner_source_is_some = inner.source().is_some();
        let error = RunnerError(inner);
        assert_eq!(error.source().is_some(), inner_source_is_some);
    }

    // ── BuildBlockError tests ──────────────────────────────────

    #[test]
    fn build_block_error_catching_up_is_catching_up() {
        let err = BuildBlockError::CatchingUp { parent_digest: dummy_digest(), parent_height: 42 };
        assert!(err.is_catching_up());
    }

    #[test]
    fn build_block_error_execution_failed_is_not_catching_up() {
        let err = BuildBlockError::ExecutionFailed {
            source: kora_executor::ExecutionError::TxExecution("test".into()),
            parent_digest: dummy_digest(),
            height: 1,
            tx_count: 0,
        };
        assert!(!err.is_catching_up());
    }

    #[test]
    fn build_block_error_root_computation_failed_is_not_catching_up() {
        let err = BuildBlockError::RootComputationFailed {
            source: dummy_ledger_error(),
            parent_digest: dummy_digest(),
            height: 1,
        };
        assert!(!err.is_catching_up());
    }

    #[test]
    fn build_block_error_execution_join_error_is_not_catching_up() {
        let err = BuildBlockError::ExecutionJoinError {
            message: "task panicked".into(),
            parent_digest: dummy_digest(),
            height: 1,
        };
        assert!(!err.is_catching_up());
    }

    #[test]
    fn build_block_error_metric_labels_are_distinct() {
        let labels: std::collections::HashSet<&'static str> = [
            BuildBlockError::CatchingUp { parent_digest: dummy_digest(), parent_height: 0 }
                .metric_label(),
            BuildBlockError::ExecutionFailed {
                source: kora_executor::ExecutionError::TxExecution("x".into()),
                parent_digest: dummy_digest(),
                height: 0,
                tx_count: 0,
            }
            .metric_label(),
            BuildBlockError::ExecutionJoinError {
                message: "x".into(),
                parent_digest: dummy_digest(),
                height: 0,
            }
            .metric_label(),
            BuildBlockError::RootComputationFailed {
                source: dummy_ledger_error(),
                parent_digest: dummy_digest(),
                height: 0,
            }
            .metric_label(),
        ]
        .into_iter()
        .collect();
        assert_eq!(labels.len(), 4, "metric_label() must be unique per variant");
    }

    #[test]
    fn build_block_error_catching_up_display_includes_height() {
        let err = BuildBlockError::CatchingUp { parent_digest: dummy_digest(), parent_height: 99 };
        let msg = format!("{err}");
        assert!(msg.contains("99"), "got: {msg}");
    }

    #[test]
    fn build_block_error_execution_failed_display_includes_height() {
        let err = BuildBlockError::ExecutionFailed {
            source: kora_executor::ExecutionError::TxExecution("boom".into()),
            parent_digest: dummy_digest(),
            height: 77,
            tx_count: 3,
        };
        let msg = format!("{err}");
        assert!(msg.contains("77"), "got: {msg}");
    }

    #[test]
    fn build_block_error_is_error_trait() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<BuildBlockError>();
    }

    #[test]
    fn build_block_error_execution_failed_has_source() {
        let err = BuildBlockError::ExecutionFailed {
            source: kora_executor::ExecutionError::TxExecution("inner".into()),
            parent_digest: dummy_digest(),
            height: 1,
            tx_count: 0,
        };
        assert!(err.source().is_some());
    }

    #[test]
    fn build_block_error_catching_up_has_no_source() {
        let err = BuildBlockError::CatchingUp { parent_digest: dummy_digest(), parent_height: 0 };
        assert!(err.source().is_none());
    }
}
