//! Shared block execution helpers.

use alloy_primitives::Bytes;
use kora_domain::Tx;
use kora_executor::{BlockContext, BlockExecutor, ExecutionOutcome};
use kora_traits::StateDb;

use crate::{ConsensusError, Snapshot};

/// Result of executing a block against a parent snapshot.
#[derive(Debug)]
pub struct BlockExecution {
    /// Execution outcome, including changes and receipts.
    pub outcome: ExecutionOutcome,
}

impl BlockExecution {
    /// Execute a block's transactions against a parent snapshot.
    ///
    /// This helper runs the executor on a dedicated blocking thread via
    /// [`tokio::task::spawn_blocking`] so that the synchronous EVM execution
    /// does not occupy an async worker thread.  The executor, state, context,
    /// and transactions are cloned into the blocking closure (all clones are
    /// cheap -- Arc bumps or small structs).
    pub async fn execute<S, E>(
        parent_snapshot: &Snapshot<S>,
        executor: &E,
        context: &BlockContext,
        txs: &[Tx],
    ) -> Result<Self, ConsensusError>
    where
        S: StateDb,
        E: BlockExecutor<S, Tx = Bytes>,
    {
        let executor = executor.clone();
        let state = parent_snapshot.state.clone();
        let context = context.clone();
        let txs_bytes: Vec<Bytes> = txs.iter().map(|tx| tx.bytes.clone()).collect();

        let outcome =
            tokio::task::spawn_blocking(move || executor.execute(&state, &context, &txs_bytes))
                .await
                .map_err(|e| ConsensusError::Execution(format!("spawn_blocking join error: {e}")))?
                .map_err(|e| ConsensusError::Execution(e.to_string()))?;

        Ok(Self { outcome })
    }
}
