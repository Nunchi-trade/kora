//! Execution outcome types.

use alloy_consensus::{Eip658Value, Receipt};
use alloy_primitives::{Address, B256, Log};
use kora_qmdb::ChangeSet;

/// Result of executing a block's transactions.
#[derive(Clone, Debug, Default)]
pub struct ExecutionOutcome {
    /// State changes from execution.
    pub changes: ChangeSet,
    /// Transaction receipts.
    pub receipts: Vec<ExecutionReceipt>,
    /// Total gas used by all transactions.
    pub gas_used: u64,
    /// Number of input transactions that were included in execution.
    ///
    /// This is equal to `receipts.len()` for normal execution. It can be
    /// smaller than the proposed transaction slice when the block gas limit
    /// truncates a trailing suffix before execution.
    pub included_tx_count: usize,
    /// Addresses that were selfdestructed during block execution.
    ///
    /// These addresses had their code and balance removed, but their storage
    /// entries in QMDB become orphaned (keyed by the old generation). A
    /// future garbage collector can use this list to reclaim dead storage
    /// once Commonware supports prefix scanning.
    pub selfdestructed_addresses: Vec<Address>,
}

impl ExecutionOutcome {
    /// Create a new empty execution outcome.
    #[must_use]
    pub fn new() -> Self {
        Self {
            changes: ChangeSet::new(),
            receipts: Vec::new(),
            gas_used: 0,
            included_tx_count: 0,
            selfdestructed_addresses: Vec::new(),
        }
    }
}

/// Receipt for a single transaction execution.
///
/// Wraps [`alloy_consensus::Receipt`] with additional execution metadata
/// that is not part of the consensus receipt (tx hash, per-tx gas, contract address).
#[derive(Clone, Debug)]
pub struct ExecutionReceipt {
    /// Transaction hash.
    pub tx_hash: B256,
    /// The consensus receipt containing status, cumulative gas, and logs.
    pub receipt: Receipt<Log>,
    /// Gas used by this transaction alone (not cumulative).
    pub gas_used: u64,
    /// Contract address if this was a contract creation.
    pub contract_address: Option<Address>,
}

impl ExecutionReceipt {
    /// Create a new execution receipt.
    pub const fn new(
        tx_hash: B256,
        success: bool,
        gas_used: u64,
        cumulative_gas_used: u64,
        logs: Vec<Log>,
        contract_address: Option<Address>,
    ) -> Self {
        Self {
            tx_hash,
            receipt: Receipt { status: Eip658Value::Eip658(success), cumulative_gas_used, logs },
            gas_used,
            contract_address,
        }
    }

    /// Returns whether the transaction succeeded.
    pub const fn success(&self) -> bool {
        self.receipt.status.coerce_status()
    }

    /// Returns the cumulative gas used up to and including this transaction.
    pub const fn cumulative_gas_used(&self) -> u64 {
        self.receipt.cumulative_gas_used
    }

    /// Returns the logs emitted during execution.
    pub fn logs(&self) -> &[Log] {
        &self.receipt.logs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_outcome_default() {
        let outcome = ExecutionOutcome::new();
        assert!(outcome.changes.is_empty());
        assert!(outcome.receipts.is_empty());
        assert_eq!(outcome.gas_used, 0);
        assert_eq!(outcome.included_tx_count, 0);
        assert!(outcome.selfdestructed_addresses.is_empty());
    }
}
