//! Block types

use std::sync::OnceLock;

use alloy_evm::revm::primitives::{B256, keccak256};
use bytes::{Buf, BufMut};
use commonware_codec::{Encode, EncodeSize, Error as CodecError, RangeCfg, Read, ReadExt, Write};
use commonware_cryptography::{Committable, Digestible, Hasher as _, Sha256};

use crate::{BlockId, Idents, StateRoot, Tx, TxCfg};

#[derive(Clone, Copy, Debug)]
/// Configuration used when decoding blocks and their transactions.
pub struct BlockCfg {
    /// Maximum number of transactions that can be encoded in a block.
    pub max_txs: usize,
    /// Per-transaction codec configuration.
    pub tx: TxCfg,
}

/// Block type agreed on by consensus (via its digest).
///
/// The block identifier (keccak256 of the encoded block) is cached on first
/// access via [`OnceLock`] to avoid redundant serialization and hashing on
/// the hot path where `id()`, `digest()`, and `commitment()` are called
/// multiple times per consensus round.
pub struct Block {
    /// Identifier of the parent block.
    pub parent: BlockId,
    /// Block height (number of committed ancestors).
    pub height: u64,
    /// Unix timestamp for this block, in seconds.
    pub timestamp: u64,
    /// Seed-derived randomness used for future prevrandao.
    pub prevrandao: B256,
    /// State commitment resulting from this block (pre-commit QMDB root).
    pub state_root: StateRoot,
    /// Transactions included in the block.
    pub txs: Vec<Tx>,

    /// Cached block identifier, computed lazily on first call to [`Self::id`].
    ///
    /// Excluded from equality comparisons, debug output, and codec encoding.
    cached_id: OnceLock<BlockId>,
}

impl Clone for Block {
    fn clone(&self) -> Self {
        Self {
            parent: self.parent,
            height: self.height,
            timestamp: self.timestamp,
            prevrandao: self.prevrandao,
            state_root: self.state_root,
            txs: self.txs.clone(),
            // Propagate the cached ID if already computed.
            cached_id: self.cached_id.get().map_or_else(OnceLock::new, |id| {
                let lock = OnceLock::new();
                let _ = lock.set(*id);
                lock
            }),
        }
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("parent", &self.parent)
            .field("height", &self.height)
            .field("timestamp", &self.timestamp)
            .field("prevrandao", &self.prevrandao)
            .field("state_root", &self.state_root)
            .field("txs", &self.txs)
            .finish()
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.parent == other.parent
            && self.height == other.height
            && self.timestamp == other.timestamp
            && self.prevrandao == other.prevrandao
            && self.state_root == other.state_root
            && self.txs == other.txs
    }
}

impl Eq for Block {}

impl Block {
    /// Construct a new block.
    ///
    /// Prefer this over struct-literal syntax; it properly initializes the
    /// internal [`OnceLock`] cache (lazily populated on first call to
    /// [`Self::id`]).
    #[must_use]
    pub const fn new(
        parent: BlockId,
        height: u64,
        timestamp: u64,
        prevrandao: B256,
        state_root: StateRoot,
        txs: Vec<Tx>,
    ) -> Self {
        Self { parent, height, timestamp, prevrandao, state_root, txs, cached_id: OnceLock::new() }
    }

    /// Compute the block identifier from its encoded contents.
    ///
    /// The result is cached internally so that repeated calls (e.g. from
    /// [`Digestible::digest`] and [`Committable::commitment`]) do not
    /// re-serialize and re-hash the block.
    pub fn id(&self) -> BlockId {
        *self.cached_id.get_or_init(|| BlockId(keccak256(self.encode())))
    }

    /// Choose a block timestamp that tracks wall-clock time without going backwards.
    ///
    /// `now_secs` is the current wall-clock time in seconds since the Unix
    /// epoch. When blocks are produced faster than one per second, multiple
    /// consecutive blocks may share the same timestamp.
    pub const fn next_timestamp(now_secs: u64, parent_timestamp: u64) -> Option<u64> {
        if now_secs > parent_timestamp { Some(now_secs) } else { Some(parent_timestamp) }
    }
}

fn digest_for_block_id(id: &BlockId) -> crate::ConsensusDigest {
    let mut hasher = Sha256::default();
    hasher.update(id.0.as_slice());
    hasher.finalize()
}

impl Digestible for Block {
    type Digest = crate::ConsensusDigest;

    fn digest(&self) -> Self::Digest {
        digest_for_block_id(&self.id())
    }
}

impl Committable for Block {
    type Commitment = crate::ConsensusDigest;

    fn commitment(&self) -> Self::Commitment {
        digest_for_block_id(&self.id())
    }
}

impl commonware_consensus::Heightable for Block {
    fn height(&self) -> commonware_consensus::types::Height {
        commonware_consensus::types::Height::new(self.height)
    }
}

impl commonware_consensus::Block for Block {
    fn parent(&self) -> Self::Digest {
        digest_for_block_id(&self.parent)
    }
}

impl Write for Block {
    fn write(&self, buf: &mut impl BufMut) {
        self.parent.write(buf);
        self.height.write(buf);
        self.timestamp.write(buf);
        Idents::write_b256(&self.prevrandao, buf);
        self.state_root.write(buf);
        self.txs.write(buf);
    }
}

impl EncodeSize for Block {
    fn encode_size(&self) -> usize {
        self.parent.encode_size()
            + self.height.encode_size()
            + self.timestamp.encode_size()
            + 32
            + self.state_root.encode_size()
            + self.txs.encode_size()
    }
}

impl Read for Block {
    type Cfg = BlockCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> Result<Self, CodecError> {
        let parent = BlockId::read(buf)?;
        let height = u64::read(buf)?;
        let timestamp = u64::read(buf)?;
        let prevrandao = Idents::read_b256(buf)?;
        let state_root = StateRoot::read(buf)?;
        let txs = Vec::<Tx>::read_cfg(buf, &(RangeCfg::new(0..=cfg.max_txs), cfg.tx))?;
        Ok(Self::new(parent, height, timestamp, prevrandao, state_root, txs))
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Bytes;
    use commonware_codec::Decode;
    use commonware_cryptography::Committable as _;

    use super::*;

    fn default_block_cfg() -> BlockCfg {
        BlockCfg { max_txs: 100, tx: TxCfg { max_tx_bytes: 131072 } }
    }

    fn sample_block() -> Block {
        Block::new(
            BlockId(B256::repeat_byte(0x01)),
            42,
            1_700_000_042,
            B256::repeat_byte(0xab),
            StateRoot(B256::repeat_byte(0xcd)),
            vec![Tx::new(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]))],
        )
    }

    #[test]
    fn block_id_is_deterministic() {
        let block = sample_block();
        let id1 = block.id();
        let id2 = block.id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn block_id_differs_by_height() {
        let block1 = sample_block();
        let mut block2 = sample_block();
        block2.height = 100;
        assert_ne!(block1.id(), block2.id());
    }

    #[test]
    fn block_id_differs_by_timestamp() {
        let block1 = sample_block();
        let mut block2 = sample_block();
        block2.timestamp += 1;
        assert_ne!(block1.id(), block2.id());
        assert_ne!(block1.commitment(), block2.commitment());
    }

    #[test]
    fn block_id_differs_by_parent() {
        let block1 = sample_block();
        let mut block2 = sample_block();
        block2.parent = BlockId(B256::repeat_byte(0xff));
        assert_ne!(block1.id(), block2.id());
    }

    #[test]
    fn block_id_differs_by_txs() {
        let block1 = sample_block();
        let mut block2 = sample_block();
        block2.txs = vec![];
        assert_ne!(block1.id(), block2.id());
    }

    #[test]
    fn block_commitment_matches_digest() {
        let block = sample_block();
        assert_eq!(block.commitment(), block.digest());
    }

    #[test]
    fn block_encode_decode_roundtrip() {
        let block = sample_block();
        let encoded = block.encode();
        let decoded = Block::decode_cfg(encoded, &default_block_cfg()).expect("decode");
        assert_eq!(block, decoded);
    }

    #[test]
    fn block_encode_size_matches_encoded() {
        let block = sample_block();
        assert_eq!(block.encode_size(), block.encode().len());
    }

    #[test]
    fn empty_block_roundtrip() {
        let block =
            Block::new(BlockId(B256::ZERO), 0, 0, B256::ZERO, StateRoot(B256::ZERO), vec![]);
        let encoded = block.encode();
        let decoded = Block::decode_cfg(encoded, &default_block_cfg()).expect("decode");
        assert_eq!(block, decoded);
    }

    #[test]
    fn block_heightable() {
        use commonware_consensus::Heightable as _;
        let block = sample_block();
        assert_eq!(block.height().get(), 42);
    }

    #[test]
    fn next_timestamp_uses_clock_when_ahead() {
        assert_eq!(Block::next_timestamp(1_700_000_100, 1_700_000_042), Some(1_700_000_100));
    }

    #[test]
    fn next_timestamp_allows_same_second_blocks_when_clock_lags() {
        assert_eq!(Block::next_timestamp(1_700_000_042, 1_700_000_042), Some(1_700_000_042));
        assert_eq!(Block::next_timestamp(1_700_000_000, 1_700_000_042), Some(1_700_000_042));
    }

    #[test]
    fn next_timestamp_handles_u64_max() {
        assert_eq!(Block::next_timestamp(0, u64::MAX), Some(u64::MAX));
        assert_eq!(Block::next_timestamp(u64::MAX, u64::MAX), Some(u64::MAX));
    }

    #[test]
    fn block_parent_commitment() {
        use commonware_consensus::Block as _;
        let block = sample_block();
        let parent_commitment = block.parent();
        let expected = digest_for_block_id(&block.parent);
        assert_eq!(parent_commitment, expected);
    }
}
