//! Core execution traits.

use alloy_consensus::Header;
use kora_qmdb::ChangeSet;
use kora_traits::StateDb;

use crate::{BlockContext, ExecutionError, ExecutionOutcome, ExecutionReceipt};

/// Executes transactions against a state database.
///
/// Abstracts the EVM execution layer to allow different backends.
pub trait BlockExecutor<S: StateDb>: Clone + Send + Sync + 'static {
    /// Transaction type accepted for execution.
    type Tx: Clone + Send + Sync + 'static;

    /// Called before transaction execution to apply protocol-level state
    /// modifications (e.g. beacon-chain system calls, epoch transitions).
    ///
    /// Returns any state changes that should be included in the block's
    /// changeset. The default implementation is a no-op that returns an
    /// empty changeset.
    fn pre_execute(
        &self,
        _context: &BlockContext,
        _state: &S,
    ) -> Result<ChangeSet, ExecutionError> {
        Ok(ChangeSet::new())
    }

    /// Execute a batch of transactions against the given state.
    ///
    /// Returns the execution outcome containing state changes and receipts.
    fn execute(
        &self,
        state: &S,
        context: &BlockContext,
        txs: &[Self::Tx],
    ) -> Result<ExecutionOutcome, ExecutionError>;

    /// Called after transaction execution to apply protocol-level state
    /// modifications (e.g. block rewards, fee burns, validator payouts).
    ///
    /// Receives the block context and the receipts produced by transaction
    /// execution so that reward logic can inspect gas usage. Returns any
    /// additional state changes. The default implementation is a no-op that
    /// returns an empty changeset.
    fn post_execute(
        &self,
        _context: &BlockContext,
        _state: &S,
        _receipts: &[ExecutionReceipt],
    ) -> Result<ChangeSet, ExecutionError> {
        Ok(ChangeSet::new())
    }

    /// Validate a block header.
    fn validate_header(&self, header: &Header) -> Result<(), ExecutionError>;
}
