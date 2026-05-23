//! Contains the [`ArchiveInitializer`] which initializes immutable archive storage.

use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};

use commonware_codec::Codec;
use commonware_consensus::{
    Block,
    marshal::store::{Blocks, Certificates},
    simplex::types::Finalization,
    types::Height,
};
use commonware_cryptography::{Digest, Digestible, certificate::Scheme};
use commonware_runtime::{BufferPooler, Clock, Metrics, Spawner, Storage, buffer::paged::CacheRef};
use commonware_storage::archive::{
    Archive as ArchiveTrait, Error as ArchiveError, Identifier,
    immutable::{Archive, Config},
};
use commonware_utils::{NZU16, NZU64, NZUsize, sequence::Array};

/// Immutable archive wrapper that only durably syncs on checkpoint boundaries.
///
/// `put` still updates the in-memory archive immediately, so marshal can serve
/// and query freshly finalized blocks. `sync` is forwarded to disk only when the
/// highest dirty height is divisible by `checkpoint_interval`.
#[derive(Debug)]
pub struct CheckpointedArchive<A> {
    inner: A,
    checkpoint_interval: u64,
    highest_dirty: Option<u64>,
}

impl<A> CheckpointedArchive<A> {
    /// Create a checkpointed archive around an existing archive.
    pub const fn new(inner: A, checkpoint_interval: u64) -> Self {
        Self { inner, checkpoint_interval, highest_dirty: None }
    }

    fn mark_dirty(&mut self, height: u64) {
        self.highest_dirty =
            Some(self.highest_dirty.map_or(height, |existing| existing.max(height)));
    }

    fn should_sync(&self) -> bool
    where
        A: ArchiveTrait,
    {
        match self.highest_dirty {
            Some(height) if self.checkpoint_interval <= 1 => self.is_contiguous_through(height),
            Some(height) => {
                height % self.checkpoint_interval == 0 && self.is_contiguous_through(height)
            }
            None => false,
        }
    }

    fn is_contiguous_through(&self, target: u64) -> bool
    where
        A: ArchiveTrait,
    {
        let mut expected_start = None;

        for (start, end) in self.inner.ranges() {
            let Some(expected) = expected_start else {
                if start > target {
                    return false;
                }
                if end >= target {
                    return true;
                }
                expected_start = end.checked_add(1);
                continue;
            };

            if start > expected {
                return false;
            }
            if end >= target {
                return true;
            }
            expected_start = end.checked_add(1);
        }

        false
    }
}

impl<A> ArchiveTrait for CheckpointedArchive<A>
where
    A: ArchiveTrait + Sync,
{
    type Key = A::Key;
    type Value = A::Value;

    async fn put(
        &mut self,
        index: u64,
        key: Self::Key,
        value: Self::Value,
    ) -> Result<(), ArchiveError> {
        self.inner.put(index, key, value).await?;
        self.mark_dirty(index);
        Ok(())
    }

    async fn get<'a>(
        &'a self,
        identifier: Identifier<'a, Self::Key>,
    ) -> Result<Option<Self::Value>, ArchiveError> {
        self.inner.get(identifier).await
    }

    async fn has<'a>(
        &'a self,
        identifier: Identifier<'a, Self::Key>,
    ) -> Result<bool, ArchiveError> {
        self.inner.has(identifier).await
    }

    fn next_gap(&self, index: u64) -> (Option<u64>, Option<u64>) {
        self.inner.next_gap(index)
    }

    fn missing_items(&self, index: u64, max: usize) -> Vec<u64> {
        self.inner.missing_items(index, max)
    }

    fn ranges(&self) -> impl Iterator<Item = (u64, u64)> {
        self.inner.ranges()
    }

    fn ranges_from(&self, from: u64) -> impl Iterator<Item = (u64, u64)> {
        self.inner.ranges_from(from)
    }

    fn first_index(&self) -> Option<u64> {
        self.inner.first_index()
    }

    fn last_index(&self) -> Option<u64> {
        self.inner.last_index()
    }

    async fn sync(&mut self) -> Result<(), ArchiveError> {
        if self.should_sync() {
            self.inner.sync().await?;
            self.highest_dirty = None;
        }
        Ok(())
    }

    async fn destroy(self) -> Result<(), ArchiveError> {
        self.inner.destroy().await
    }
}

impl<A, B, C, S> Certificates for CheckpointedArchive<A>
where
    A: ArchiveTrait<Key = B, Value = Finalization<S, C>> + Send + Sync + 'static,
    B: Digest,
    C: Digest,
    S: Scheme,
{
    type BlockDigest = B;
    type Commitment = C;
    type Scheme = S;
    type Error = ArchiveError;

    async fn put(
        &mut self,
        height: Height,
        digest: Self::BlockDigest,
        finalization: Finalization<Self::Scheme, Self::Commitment>,
    ) -> Result<(), Self::Error> {
        ArchiveTrait::put(self, height.get(), digest, finalization).await
    }

    async fn sync(&mut self) -> Result<(), Self::Error> {
        ArchiveTrait::sync(self).await
    }

    async fn get(
        &self,
        id: Identifier<'_, Self::BlockDigest>,
    ) -> Result<Option<Finalization<Self::Scheme, Self::Commitment>>, Self::Error> {
        ArchiveTrait::get(self, id).await
    }

    async fn prune(&mut self, _: Height) -> Result<(), Self::Error> {
        Ok(())
    }

    fn last_index(&self) -> Option<Height> {
        ArchiveTrait::last_index(self).map(Height::new)
    }

    fn ranges_from(&self, from: Height) -> impl Iterator<Item = (Height, Height)> {
        ArchiveTrait::ranges_from(self, from.get())
            .map(|(start, end)| (Height::new(start), Height::new(end)))
    }
}

impl<A, B> Blocks for CheckpointedArchive<A>
where
    A: ArchiveTrait<Key = B::Digest, Value = B> + Send + Sync + 'static,
    B: Block,
{
    type Block = B;
    type Error = ArchiveError;

    async fn put(&mut self, block: Self::Block) -> Result<(), Self::Error> {
        ArchiveTrait::put(self, block.height().get(), block.digest(), block).await
    }

    async fn sync(&mut self) -> Result<(), Self::Error> {
        ArchiveTrait::sync(self).await
    }

    async fn get(
        &self,
        id: Identifier<'_, <Self::Block as Digestible>::Digest>,
    ) -> Result<Option<Self::Block>, Self::Error> {
        ArchiveTrait::get(self, id).await
    }

    async fn prune(&mut self, _: Height) -> Result<(), Self::Error> {
        Ok(())
    }

    fn missing_items(&self, start: Height, max: usize) -> Vec<Height> {
        ArchiveTrait::missing_items(self, start.get(), max).into_iter().map(Height::new).collect()
    }

    fn next_gap(&self, value: Height) -> (Option<Height>, Option<Height>) {
        let (current, next) = ArchiveTrait::next_gap(self, value.get());
        (current.map(Height::new), next.map(Height::new))
    }

    fn last_index(&self) -> Option<Height> {
        ArchiveTrait::last_index(self).map(Height::new)
    }
}

/// Initializes immutable archive storage with sensible defaults.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveInitializer;

impl ArchiveInitializer {
    /// The default freezer table initial size.
    pub const DEFAULT_FREEZER_TABLE_INITIAL_SIZE: u32 = 2_097_152;

    /// The default freezer table resize frequency.
    pub const DEFAULT_FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;

    /// The default freezer table resize chunk size.
    pub const DEFAULT_FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 65_536;

    /// The default freezer value target size.
    pub const DEFAULT_FREEZER_VALUE_TARGET_SIZE: u64 = 1024 * 1024 * 1024;

    /// The default compression level (zstd level 3).
    pub const DEFAULT_COMPRESSION_LEVEL: Option<u8> = Some(3);

    /// The default items per section.
    pub const DEFAULT_ITEMS_PER_SECTION: NonZeroU64 = NZU64!(262_144);

    /// The default write buffer size.
    pub const DEFAULT_WRITE_BUFFER: NonZeroUsize = NZUsize!(1024 * 1024);

    /// The default replay buffer size.
    pub const DEFAULT_REPLAY_BUFFER: NonZeroUsize = NZUsize!(8 * 1024 * 1024);

    /// The default page size.
    pub const DEFAULT_PAGE_SIZE: NonZeroU16 = NZU16!(4_096);

    /// The default page cache size.
    pub const DEFAULT_PAGE_CACHE_SIZE: NonZeroUsize = NZUsize!(8_192);

    /// The default partition prefix for finalizations archive.
    pub const DEFAULT_FINALIZATIONS_PREFIX: &'static str = "finalizations";

    /// The default partition prefix for blocks archive.
    pub const DEFAULT_BLOCKS_PREFIX: &'static str = "blocks";
}

impl ArchiveInitializer {
    /// Initializes an immutable archive with a custom partition prefix.
    ///
    /// The `partition_prefix` is used to namespace all storage partitions.
    /// The `codec_config` configures serialization for stored values.
    ///
    /// Type parameters:
    /// - `E`: Runtime context (must implement `Spawner + Storage + Metrics + Clock`)
    /// - `K`: Key type (must implement `Array`)
    /// - `V`: Value type (must implement `Codec`)
    pub async fn init<E, K, V>(
        ctx: E,
        partition_prefix: impl Into<String>,
        codec_config: V::Cfg,
    ) -> Result<Archive<E, K, V>, commonware_storage::archive::Error>
    where
        E: BufferPooler + Spawner + Storage + Metrics + Clock + Clone,
        K: Array,
        V: Codec + Send + Sync,
    {
        let prefix = partition_prefix.into();
        let config = Config {
            metadata_partition: format!("{prefix}-metadata"),
            freezer_table_partition: format!("{prefix}-freezer-table"),
            freezer_table_initial_size: Self::DEFAULT_FREEZER_TABLE_INITIAL_SIZE,
            freezer_table_resize_frequency: Self::DEFAULT_FREEZER_TABLE_RESIZE_FREQUENCY,
            freezer_table_resize_chunk_size: Self::DEFAULT_FREEZER_TABLE_RESIZE_CHUNK_SIZE,
            freezer_key_partition: format!("{prefix}-freezer-key"),
            freezer_key_page_cache: CacheRef::from_pooler(
                &ctx,
                Self::DEFAULT_PAGE_SIZE,
                Self::DEFAULT_PAGE_CACHE_SIZE,
            ),
            freezer_value_partition: format!("{prefix}-freezer-value"),
            freezer_value_target_size: Self::DEFAULT_FREEZER_VALUE_TARGET_SIZE,
            freezer_value_compression: Self::DEFAULT_COMPRESSION_LEVEL,
            ordinal_partition: format!("{prefix}-ordinal"),
            items_per_section: Self::DEFAULT_ITEMS_PER_SECTION,
            freezer_key_write_buffer: Self::DEFAULT_WRITE_BUFFER,
            freezer_value_write_buffer: Self::DEFAULT_WRITE_BUFFER,
            ordinal_write_buffer: Self::DEFAULT_WRITE_BUFFER,
            replay_buffer: Self::DEFAULT_REPLAY_BUFFER,
            codec_config,
        };
        Archive::init(ctx, config).await
    }

    /// Initializes an immutable archive wrapped with checkpointed sync behavior.
    pub async fn init_checkpointed<E, K, V>(
        ctx: E,
        partition_prefix: impl Into<String>,
        codec_config: V::Cfg,
        checkpoint_interval: u64,
    ) -> Result<CheckpointedArchive<Archive<E, K, V>>, commonware_storage::archive::Error>
    where
        E: BufferPooler + Spawner + Storage + Metrics + Clock + Clone,
        K: Array,
        V: Codec + Send + Sync,
    {
        let archive = Self::init(ctx, partition_prefix, codec_config).await?;
        Ok(CheckpointedArchive::new(archive, checkpoint_interval))
    }

    /// Initializes a finalizations archive with the default prefix.
    ///
    /// Uses [`DEFAULT_FINALIZATIONS_PREFIX`](Self::DEFAULT_FINALIZATIONS_PREFIX) as the partition prefix.
    pub async fn init_finalizations<E, K, V>(
        ctx: E,
        codec_config: V::Cfg,
    ) -> Result<Archive<E, K, V>, commonware_storage::archive::Error>
    where
        E: BufferPooler + Spawner + Storage + Metrics + Clock + Clone,
        K: Array,
        V: Codec + Send + Sync,
    {
        Self::init(ctx, Self::DEFAULT_FINALIZATIONS_PREFIX, codec_config).await
    }

    /// Initializes a blocks archive with the default prefix.
    ///
    /// Uses [`DEFAULT_BLOCKS_PREFIX`](Self::DEFAULT_BLOCKS_PREFIX) as the partition prefix.
    pub async fn init_blocks<E, K, V>(
        ctx: E,
        codec_config: V::Cfg,
    ) -> Result<Archive<E, K, V>, commonware_storage::archive::Error>
    where
        E: BufferPooler + Spawner + Storage + Metrics + Clock + Clone,
        K: Array,
        V: Codec + Send + Sync,
    {
        Self::init(ctx, Self::DEFAULT_BLOCKS_PREFIX, codec_config).await
    }
}

#[cfg(test)]
mod tests {
    use commonware_utils::sequence::Unit;

    use super::*;

    #[derive(Debug)]
    struct FakeArchive {
        ranges: Vec<(u64, u64)>,
    }

    impl ArchiveTrait for FakeArchive {
        type Key = Unit;
        type Value = u64;

        async fn put(
            &mut self,
            index: u64,
            _: Self::Key,
            _: Self::Value,
        ) -> Result<(), ArchiveError> {
            self.ranges.push((index, index));
            Ok(())
        }

        async fn get<'a>(
            &'a self,
            _: Identifier<'a, Self::Key>,
        ) -> Result<Option<Self::Value>, ArchiveError> {
            Ok(None)
        }

        async fn has<'a>(&'a self, _: Identifier<'a, Self::Key>) -> Result<bool, ArchiveError> {
            Ok(false)
        }

        fn next_gap(&self, _: u64) -> (Option<u64>, Option<u64>) {
            (None, None)
        }

        fn missing_items(&self, _: u64, _: usize) -> Vec<u64> {
            Vec::new()
        }

        fn ranges(&self) -> impl Iterator<Item = (u64, u64)> {
            self.ranges.clone().into_iter()
        }

        fn ranges_from(&self, from: u64) -> impl Iterator<Item = (u64, u64)> {
            self.ranges.clone().into_iter().filter(move |(_, end)| *end >= from)
        }

        fn first_index(&self) -> Option<u64> {
            self.ranges.first().map(|(start, _)| *start)
        }

        fn last_index(&self) -> Option<u64> {
            self.ranges.last().map(|(_, end)| *end)
        }

        async fn sync(&mut self) -> Result<(), ArchiveError> {
            Ok(())
        }

        async fn destroy(self) -> Result<(), ArchiveError> {
            Ok(())
        }
    }

    #[test]
    fn test_defaults() {
        assert_eq!(ArchiveInitializer::DEFAULT_FREEZER_TABLE_INITIAL_SIZE, 2_097_152);
        assert_eq!(ArchiveInitializer::DEFAULT_FREEZER_TABLE_RESIZE_FREQUENCY, 4);
        assert_eq!(ArchiveInitializer::DEFAULT_FREEZER_TABLE_RESIZE_CHUNK_SIZE, 65_536);
        assert_eq!(ArchiveInitializer::DEFAULT_FREEZER_VALUE_TARGET_SIZE, 1024 * 1024 * 1024);
        assert_eq!(ArchiveInitializer::DEFAULT_COMPRESSION_LEVEL, Some(3));
        assert_eq!(ArchiveInitializer::DEFAULT_ITEMS_PER_SECTION.get(), 262_144);
        assert_eq!(ArchiveInitializer::DEFAULT_WRITE_BUFFER.get(), 1024 * 1024);
        assert_eq!(ArchiveInitializer::DEFAULT_REPLAY_BUFFER.get(), 8 * 1024 * 1024);
        assert_eq!(ArchiveInitializer::DEFAULT_PAGE_SIZE.get(), 4_096);
        assert_eq!(ArchiveInitializer::DEFAULT_PAGE_CACHE_SIZE.get(), 8_192);
        assert_eq!(ArchiveInitializer::DEFAULT_FINALIZATIONS_PREFIX, "finalizations");
        assert_eq!(ArchiveInitializer::DEFAULT_BLOCKS_PREFIX, "blocks");
    }

    #[test]
    fn checkpointed_archive_syncs_only_on_boundary() {
        let inner = FakeArchive { ranges: vec![(1, 64)] };
        let mut archive = CheckpointedArchive::new(inner, 64);

        assert!(!archive.should_sync());

        archive.mark_dirty(63);
        assert!(!archive.should_sync());

        archive.mark_dirty(64);
        assert!(archive.should_sync());
    }

    #[test]
    fn checkpointed_archive_interval_one_preserves_default_sync_behavior() {
        let inner = FakeArchive { ranges: vec![(1, 7)] };
        let mut archive = CheckpointedArchive::new(inner, 1);

        assert!(!archive.should_sync());

        archive.mark_dirty(7);
        assert!(archive.should_sync());
    }

    #[test]
    fn checkpointed_archive_does_not_sync_sparse_boundary() {
        let inner = FakeArchive { ranges: vec![(1, 32), (34, 64)] };
        let mut archive = CheckpointedArchive::new(inner, 64);

        archive.mark_dirty(64);
        assert!(!archive.should_sync());
    }
}
