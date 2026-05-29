//! Indexed types for blocks, transactions, receipts, and logs.

use alloy_primitives::{Address, B256, Bloom, Bytes, U256, b256};

/// The root hash of an empty Merkle Patricia Trie.
///
/// This is the keccak256 hash of the RLP encoding of an empty string, which is
/// the expected value for `transactionsRoot` and `receiptsRoot` in blocks that
/// contain no transactions.
///
/// Equal to `keccak256(rlp(""))` =
/// `0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421`.
pub const EMPTY_ROOT_HASH: B256 =
    b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

/// An indexed block containing header information and transaction hashes.
#[derive(Debug, Clone)]
pub struct IndexedBlock {
    /// Block hash.
    pub hash: B256,
    /// Block number.
    pub number: u64,
    /// Parent block hash.
    pub parent_hash: B256,
    /// State root after executing this block.
    pub state_root: B256,
    /// Transactions trie root (MPT root of RLP-encoded transactions).
    pub transactions_root: B256,
    /// Receipts trie root (MPT root of RLP-encoded receipts).
    pub receipts_root: B256,
    /// Block timestamp.
    pub timestamp: u64,
    /// Gas limit for this block.
    pub gas_limit: u64,
    /// Gas used by all transactions in this block.
    pub gas_used: u64,
    /// Base fee per gas (EIP-1559).
    pub base_fee_per_gas: Option<u64>,
    /// Mix hash / prevrandao value for this block.
    pub mix_hash: B256,
    /// Approximate block size in bytes (header overhead + sum of raw tx sizes).
    pub size: u64,
    /// Hashes of transactions included in this block.
    pub transaction_hashes: Vec<B256>,
}

/// An indexed transaction with full details.
#[derive(Debug, Clone)]
pub struct IndexedTransaction {
    /// Transaction hash.
    pub hash: B256,
    /// Hash of the block containing this transaction.
    pub block_hash: B256,
    /// Number of the block containing this transaction.
    pub block_number: u64,
    /// Index of the transaction within the block.
    pub index: u64,
    /// Sender address.
    pub from: Address,
    /// Recipient address (None for contract creation).
    pub to: Option<Address>,
    /// Value transferred.
    pub value: U256,
    /// Gas limit for this transaction.
    pub gas_limit: u64,
    /// Gas price.
    pub gas_price: u128,
    /// EIP-2718 transaction type.
    pub tx_type: u8,
    /// Chain ID.
    pub chain_id: Option<u64>,
    /// Max fee per gas (EIP-1559 and later typed transactions).
    pub max_fee_per_gas: Option<u128>,
    /// Max priority fee per gas (EIP-1559 and later typed transactions).
    pub max_priority_fee_per_gas: Option<u128>,
    /// V component of the transaction signature (u128 to represent the full EIP-155 range).
    pub v: u128,
    /// R component of the transaction signature.
    pub r: U256,
    /// S component of the transaction signature.
    pub s: U256,
    /// Input data.
    pub input: Bytes,
    /// Sender nonce.
    pub nonce: u64,
}

/// An indexed transaction receipt.
#[derive(Debug, Clone)]
pub struct IndexedReceipt {
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Hash of the block containing this transaction.
    pub block_hash: B256,
    /// Number of the block containing this transaction.
    pub block_number: u64,
    /// Index of the transaction within the block.
    pub transaction_index: u64,
    /// Sender address.
    pub from: Address,
    /// Recipient address (None for contract creation).
    pub to: Option<Address>,
    /// Cumulative gas used in the block up to and including this transaction.
    pub cumulative_gas_used: u64,
    /// Gas used by this transaction.
    pub gas_used: u64,
    /// Contract address created (if contract creation transaction).
    pub contract_address: Option<Address>,
    /// Logs emitted by this transaction.
    pub logs: Vec<IndexedLog>,
    /// Logs bloom filter for this receipt.
    pub logs_bloom: Bloom,
    /// EIP-2718 transaction type.
    pub tx_type: u8,
    /// Effective gas price paid by this transaction.
    pub effective_gas_price: u128,
    /// Transaction status (true = success, false = revert).
    pub status: bool,
}

/// An indexed log entry.
#[derive(Debug, Clone)]
pub struct IndexedLog {
    /// Address of the contract that emitted the log.
    pub address: Address,
    /// Indexed topics.
    pub topics: Vec<B256>,
    /// Non-indexed data.
    pub data: Bytes,
    /// Log index within the block.
    pub log_index: u64,
    /// Number of the block containing this log.
    pub block_number: u64,
    /// Hash of the block containing this log.
    pub block_hash: B256,
    /// Hash of the transaction that emitted this log.
    pub transaction_hash: B256,
    /// Index of the transaction that emitted this log.
    pub transaction_index: u64,
}

/// Statistics about the block index.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexStats {
    /// Total number of indexed blocks.
    pub block_count: usize,
    /// Total number of indexed transactions.
    pub transaction_count: usize,
    /// Total number of indexed receipts.
    pub receipt_count: usize,
    /// Current head block number.
    pub head_block_number: u64,
}
