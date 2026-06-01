use alloy_primitives::B256;
use commonware_cryptography::{Hasher as _, Sha256};
use kora_domain::ConsensusDigest;
use kora_executor::{BaseFeeParams, calculate_base_fee};
use kora_indexer::{BlockIndex, IndexedBlock};
use tracing::warn;

/// Compute the consensus digest for a block hash (BlockId).
///
/// Mirrors `digest_for_block_id` in `kora_domain::block` which is private.
pub(crate) fn consensus_digest_for_hash(block_hash: B256) -> ConsensusDigest {
    let mut hasher = Sha256::default();
    hasher.update(block_hash.as_slice());
    hasher.finalize()
}

pub(crate) fn next_base_fee(parent: &IndexedBlock) -> u64 {
    calculate_base_fee(
        parent.base_fee_per_gas.unwrap_or(kora_config::INITIAL_BASE_FEE),
        parent.gas_used,
        parent.gas_limit,
        &BaseFeeParams::DEFAULT,
    )
}

/// Look up a parent block in the index and compute the next base fee.
///
/// The index is keyed by height for canonical RPC lookups, but consensus
/// verification knows the parent by digest. Do not use a height match unless
/// the indexed block hash maps to the expected consensus digest.
pub(crate) fn indexed_parent_base_fee(
    block_index: &BlockIndex,
    parent_digest: ConsensusDigest,
    parent_height: u64,
) -> Option<u64> {
    let parent = block_index.get_block_by_number(parent_height)?;
    let indexed_digest = consensus_digest_for_hash(parent.hash);
    if indexed_digest != parent_digest {
        warn!(
            parent_height,
            expected_digest = %hex::encode(parent_digest.as_ref()),
            indexed_digest = %hex::encode(indexed_digest.as_ref()),
            indexed_hash = %parent.hash,
            "ignoring block-index base-fee fallback for non-matching parent digest"
        );
        return None;
    }
    Some(next_base_fee(&parent))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Bloom};
    use kora_indexer::IndexedBlock;

    use super::*;

    fn indexed_block(number: u64, hash: B256, gas_used: u64, base_fee: u64) -> IndexedBlock {
        IndexedBlock {
            hash,
            number,
            parent_hash: B256::ZERO,
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            timestamp: 1_700_000_000 + number,
            gas_limit: 30_000_000,
            gas_used,
            base_fee_per_gas: Some(base_fee),
            mix_hash: B256::ZERO,
            logs_bloom: Bloom::ZERO,
            size: 508,
            transaction_hashes: Vec::new(),
        }
    }

    #[test]
    fn indexed_parent_base_fee_requires_matching_digest() {
        let index = BlockIndex::new();
        let indexed_hash = B256::repeat_byte(0x11);
        index.insert_block(
            indexed_block(7, indexed_hash, 21_000, 1_000_000_000),
            Vec::new(),
            Vec::new(),
        );

        let other_digest = consensus_digest_for_hash(B256::repeat_byte(0x22));

        assert_eq!(indexed_parent_base_fee(&index, other_digest, 7), None);
    }

    #[test]
    fn indexed_parent_base_fee_uses_matching_indexed_parent() {
        let index = BlockIndex::new();
        let indexed_hash = B256::repeat_byte(0x11);
        let parent = indexed_block(7, indexed_hash, 30_000_000, 1_000_000_000);
        let expected = next_base_fee(&parent);
        index.insert_block(parent, Vec::new(), Vec::new());

        let digest = consensus_digest_for_hash(indexed_hash);

        assert_eq!(indexed_parent_base_fee(&index, digest, 7), Some(expected));
    }
}
