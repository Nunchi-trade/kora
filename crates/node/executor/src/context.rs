//! Block execution context.

use std::collections::HashMap;

use alloy_consensus::Header;
use alloy_primitives::B256;

/// Maximum number of recent block hashes retained for the BLOCKHASH opcode.
const MAX_BLOCK_HASHES: usize = 256;

/// Context for block execution.
///
/// Contains the block header and additional execution parameters.
#[derive(Clone, Debug)]
pub struct BlockContext {
    /// Block header.
    pub header: Header,
    /// Parent block hash.
    pub parent_hash: B256,
    /// Previous block's randomness (prevrandao).
    pub prevrandao: B256,
    /// Blob base fee for Cancun+ (EIP-4844).
    pub blob_base_fee: Option<u128>,
    /// Recent block hashes keyed by block number for the BLOCKHASH opcode.
    /// Contains up to the last 256 block hashes.
    pub recent_block_hashes: HashMap<u64, B256>,
}

impl BlockContext {
    /// Create a new block context.
    #[must_use]
    pub fn new(header: Header, parent_hash: B256, prevrandao: B256) -> Self {
        Self {
            header,
            parent_hash,
            prevrandao,
            blob_base_fee: None,
            recent_block_hashes: HashMap::new(),
        }
    }

    /// Set the blob base fee.
    #[must_use]
    pub const fn with_blob_base_fee(mut self, blob_base_fee: u128) -> Self {
        self.blob_base_fee = Some(blob_base_fee);
        self
    }

    /// Set the recent block hashes for BLOCKHASH opcode support.
    ///
    /// Retains at most 256 entries (the EVM BLOCKHASH depth limit).
    #[must_use]
    pub fn with_recent_block_hashes(mut self, hashes: HashMap<u64, B256>) -> Self {
        if hashes.len() > MAX_BLOCK_HASHES {
            self.recent_block_hashes = hashes.into_iter().take(MAX_BLOCK_HASHES).collect();
        } else {
            self.recent_block_hashes = hashes;
        }
        self
    }

    /// Get the base fee from the header.
    pub fn base_fee(&self) -> u64 {
        self.header.base_fee_per_gas.unwrap_or_default()
    }
}

/// Parent block info for header validation.
#[derive(Clone, Debug)]
pub struct ParentBlock {
    /// Parent block hash.
    pub hash: B256,
    /// Parent block number.
    pub number: u64,
    /// Parent block timestamp.
    pub timestamp: u64,
    /// Parent gas limit.
    pub gas_limit: u64,
    /// Parent gas used.
    pub gas_used: u64,
    /// Parent base fee per gas (EIP-1559).
    pub base_fee_per_gas: Option<u64>,
}

impl ParentBlock {
    /// Create parent block info from a header.
    pub const fn from_header(header: &Header, hash: B256) -> Self {
        Self {
            hash,
            number: header.number,
            timestamp: header.timestamp,
            gas_limit: header.gas_limit,
            gas_used: header.gas_used,
            base_fee_per_gas: header.base_fee_per_gas,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_context_new() {
        let header = Header::default();
        let parent_hash = B256::repeat_byte(1);
        let prevrandao = B256::ZERO;
        let context = BlockContext::new(header, parent_hash, prevrandao);
        assert_eq!(context.prevrandao, B256::ZERO);
        assert_eq!(context.parent_hash, parent_hash);
        assert!(context.blob_base_fee.is_none());
        assert!(context.recent_block_hashes.is_empty());
    }

    #[test]
    fn block_context_with_blob_base_fee() {
        let header = Header::default();
        let context = BlockContext::new(header, B256::ZERO, B256::ZERO).with_blob_base_fee(1000);
        assert_eq!(context.blob_base_fee, Some(1000));
    }

    #[test]
    fn block_context_with_recent_block_hashes() {
        let header = Header::default();
        let mut hashes = HashMap::new();
        hashes.insert(10, B256::repeat_byte(0x10));
        hashes.insert(11, B256::repeat_byte(0x11));
        let context =
            BlockContext::new(header, B256::ZERO, B256::ZERO).with_recent_block_hashes(hashes);
        assert_eq!(context.recent_block_hashes.len(), 2);
        assert_eq!(context.recent_block_hashes[&10], B256::repeat_byte(0x10));
    }

    #[test]
    fn block_context_with_recent_block_hashes_truncates() {
        let header = Header::default();
        let hashes: HashMap<u64, B256> =
            (0..300).map(|i| (i, B256::repeat_byte(i as u8))).collect();
        assert_eq!(hashes.len(), 300);
        let context =
            BlockContext::new(header, B256::ZERO, B256::ZERO).with_recent_block_hashes(hashes);
        assert_eq!(context.recent_block_hashes.len(), MAX_BLOCK_HASHES);
    }

    #[test]
    fn parent_block_from_header() {
        let header = Header {
            number: 100,
            timestamp: 1234567890,
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            base_fee_per_gas: Some(1000),
            ..Header::default()
        };

        let hash = B256::repeat_byte(0xab);
        let parent = ParentBlock::from_header(&header, hash);

        assert_eq!(parent.hash, hash);
        assert_eq!(parent.number, 100);
        assert_eq!(parent.timestamp, 1234567890);
        assert_eq!(parent.gas_limit, 30_000_000);
        assert_eq!(parent.gas_used, 15_000_000);
        assert_eq!(parent.base_fee_per_gas, Some(1000));
    }
}
