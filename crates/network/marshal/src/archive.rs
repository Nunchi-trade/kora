//! Contains the [`ArchiveInitializer`] which initializes archive storage, and
//! the [`CheckpointedArchive`] wrapper that batches syncs to checkpoint boundaries.

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
use commonware_storage::{
    archive::{
        Archive as ArchiveTrait, Error as ArchiveError, Identifier,
        immutable::{Archive, Config},
        prunable::{Archive as PrunableArchive, Config as PrunableConfig},
    },
    translator::{EightCap, Translator},
};
use commonware_utils::{NZU16, NZU64, NZUsize, sequence::Array};
use tracing::warn;

/// Trait for archive backends that support pruning old entries.
///
/// This enables [`CheckpointedArchive`] to forward `prune` calls from the
/// marshal's [`Blocks`] and [`Certificates`] stores to the underlying archive.
pub trait Prunable {
    /// Remove all entries with index strictly below `min`.
    fn prune(
        &mut self,
        min: u64,
    ) -> impl std::future::Future<Output = Result<(), ArchiveError>> + Send;
}

impl<T, E, K, V> Prunable for PrunableArchive<T, E, K, V>
where
    T: Translator,
    E: BufferPooler + Storage + Metrics + Send,
    K: Array,
    V: Codec + Send + Sync,
{
    async fn prune(&mut self, min: u64) -> Result<(), ArchiveError> {
        Self::prune(self, min).await
    }
}

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
    ///
    /// A `checkpoint_interval` of 0 is clamped to 1 to prevent
    /// division-by-zero in [`should_sync`].  This matches the guards in
    /// `NoSyncStorage::new()` (`.max(1)`) and
    /// `FinalizedReporter::with_checkpoint_interval()` (`if 0 then 1`).
    pub const fn new(inner: A, checkpoint_interval: u64) -> Self {
        let interval = if checkpoint_interval == 0 { 1 } else { checkpoint_interval };
        Self { inner, checkpoint_interval: interval, highest_dirty: None }
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
                // Compute the highest checkpoint boundary at or below the
                // dirty height.  This handles out-of-order insertion: even if
                // highest_dirty overshoots a boundary (e.g. 65 with interval
                // 64), we recognise that the boundary at 64 has been reached
                // and sync when the archive is contiguous through it.  The
                // inner archive's sync() flushes ALL in-memory data, so
                // blocks above the boundary are also persisted.
                let boundary = (height / self.checkpoint_interval) * self.checkpoint_interval;
                boundary > 0 && self.is_contiguous_through(boundary)
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
    A: ArchiveTrait<Key = B, Value = Finalization<S, C>> + Prunable + Send + Sync + 'static,
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

    async fn prune(&mut self, min: Height) -> Result<(), Self::Error> {
        self.inner.prune(min.get()).await
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
    A: ArchiveTrait<Key = B::Digest, Value = B> + Prunable + Send + Sync + 'static,
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

    async fn prune(&mut self, min: Height) -> Result<(), Self::Error> {
        self.inner.prune(min.get()).await
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

/// Initializes archive storage with sensible defaults.
///
/// Provides both immutable (append-only) and prunable archive backends.
/// Production deployments should use the prunable variants
/// ([`init_prunable`](Self::init_prunable),
/// [`init_prunable_checkpointed`](Self::init_prunable_checkpointed))
/// so the marshal can reclaim disk space for old finalized blocks and
/// certificates via the [`Prunable`] trait.
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

    /// The default prunable items per section.
    ///
    /// Pruning operates at section granularity -- items are only freed when an
    /// entire section falls below the retention window. A smaller section size
    /// (256) makes pruning more responsive and reduces peak disk usage.
    pub const DEFAULT_PRUNABLE_ITEMS_PER_SECTION: NonZeroU64 = NZU64!(256);

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
        E: BufferPooler + Spawner + Storage + Metrics + Clock,
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
        E: BufferPooler + Spawner + Storage + Metrics + Clock,
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
        E: BufferPooler + Spawner + Storage + Metrics + Clock,
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
        E: BufferPooler + Spawner + Storage + Metrics + Clock,
        K: Array,
        V: Codec + Send + Sync,
    {
        Self::init(ctx, Self::DEFAULT_BLOCKS_PREFIX, codec_config).await
    }

    /// Initializes a prunable archive with a custom partition prefix.
    ///
    /// Unlike [`init`](Self::init), this creates a [`prunable::Archive`] that
    /// supports removing old entries via [`Prunable::prune`]. Uses [`EightCap`]
    /// as the key translator, which takes the first 8 bytes of each key digest
    /// for hash-table indexing.
    ///
    /// [`prunable::Archive`]: commonware_storage::archive::prunable::Archive
    pub async fn init_prunable<E, K, V>(
        ctx: E,
        partition_prefix: impl Into<String>,
        codec_config: V::Cfg,
    ) -> Result<PrunableArchive<EightCap, E, K, V>, commonware_storage::archive::Error>
    where
        E: BufferPooler + Spawner + Storage + Metrics + Clock,
        K: Array,
        V: Codec + Send + Sync,
    {
        let prefix = partition_prefix.into();
        let config = PrunableConfig {
            translator: EightCap,
            key_partition: format!("{prefix}-key"),
            key_page_cache: CacheRef::from_pooler(
                &ctx,
                Self::DEFAULT_PAGE_SIZE,
                Self::DEFAULT_PAGE_CACHE_SIZE,
            ),
            value_partition: format!("{prefix}-value"),
            compression: Self::DEFAULT_COMPRESSION_LEVEL,
            codec_config,
            items_per_section: Self::DEFAULT_PRUNABLE_ITEMS_PER_SECTION,
            key_write_buffer: Self::DEFAULT_WRITE_BUFFER,
            value_write_buffer: Self::DEFAULT_WRITE_BUFFER,
            replay_buffer: Self::DEFAULT_REPLAY_BUFFER,
        };
        PrunableArchive::init(ctx, config).await
    }

    /// Initializes a prunable archive wrapped with checkpointed sync behavior.
    ///
    /// Combines [`init_prunable`](Self::init_prunable) with
    /// [`CheckpointedArchive`] so that syncs are batched to `checkpoint_interval`
    /// boundaries while pruning remains fully functional.
    pub async fn init_prunable_checkpointed<E, K, V>(
        ctx: E,
        partition_prefix: impl Into<String>,
        codec_config: V::Cfg,
        checkpoint_interval: u64,
    ) -> Result<
        CheckpointedArchive<PrunableArchive<EightCap, E, K, V>>,
        commonware_storage::archive::Error,
    >
    where
        E: BufferPooler + Spawner + Storage + Metrics + Clock,
        K: Array,
        V: Codec + Send + Sync,
    {
        let archive = Self::init_prunable(ctx, partition_prefix, codec_config).await?;
        Ok(CheckpointedArchive::new(archive, checkpoint_interval))
    }

    /// Partition suffixes used by the old `immutable::Archive` backend.
    ///
    /// When migrating from immutable to prunable archives, these partitions
    /// contain orphaned data that will never be read by the new backend.
    const LEGACY_IMMUTABLE_SUFFIXES: &'static [&'static str] =
        &["-metadata", "-freezer-table", "-freezer-key", "-freezer-value", "-ordinal"];

    /// Detect and remove legacy immutable archive partitions for a given prefix.
    ///
    /// The old `immutable::Archive` backend used five partitions per archive
    /// (`{prefix}-metadata`, `{prefix}-freezer-table`, `{prefix}-freezer-key`,
    /// `{prefix}-freezer-value`, `{prefix}-ordinal`). The new `prunable::Archive`
    /// backend uses different partition names (`{prefix}-key`, `{prefix}-value`),
    /// so upgrading silently orphans the old data on disk.
    ///
    /// This method scans for legacy partitions and removes any that contain
    /// data, logging a warning for each one removed. Call this before
    /// [`init_prunable`](Self::init_prunable) or
    /// [`init_prunable_checkpointed`](Self::init_prunable_checkpointed) to
    /// ensure a clean migration.
    ///
    /// Returns the number of legacy partitions that were detected and removed.
    pub async fn migrate_from_immutable<E>(ctx: &E, partition_prefix: &str) -> usize
    where
        E: Storage,
    {
        let mut removed = 0;
        for suffix in Self::LEGACY_IMMUTABLE_SUFFIXES {
            let partition_name = format!("{partition_prefix}{suffix}");
            match ctx.scan(&partition_name).await {
                Ok(blobs) if !blobs.is_empty() => {
                    warn!(
                        partition = %partition_name,
                        blobs = blobs.len(),
                        "removing legacy immutable archive partition \
                         (replaced by prunable backend)"
                    );
                    if let Err(e) = ctx.remove(&partition_name, None).await {
                        warn!(
                            partition = %partition_name,
                            error = %e,
                            "failed to remove legacy immutable archive partition"
                        );
                    } else {
                        removed += 1;
                    }
                }
                Ok(_) => {
                    // Partition exists but is empty, or doesn't exist -- nothing to do.
                }
                Err(e) => {
                    warn!(
                        partition = %partition_name,
                        error = %e,
                        "failed to scan for legacy immutable archive partition"
                    );
                }
            }
        }
        if removed > 0 {
            warn!(
                prefix = %partition_prefix,
                removed,
                "cleaned up legacy immutable archive partitions; \
                 archive history has been reset with the new prunable backend"
            );
        }
        removed
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
        assert_eq!(ArchiveInitializer::DEFAULT_PRUNABLE_ITEMS_PER_SECTION.get(), 256);
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

    #[test]
    fn checkpointed_archive_syncs_when_dirty_past_boundary() {
        // Simulate out-of-order: block 65 arrives, then 64.
        // highest_dirty = 65, but the boundary at 64 should still trigger sync.
        let inner = FakeArchive { ranges: vec![(1, 65)] };
        let mut archive = CheckpointedArchive::new(inner, 64);

        archive.mark_dirty(65);
        // 65 is past the boundary at 64, and archive is contiguous through 64
        assert!(archive.should_sync());
    }

    #[test]
    fn checkpointed_archive_no_sync_before_first_boundary() {
        let inner = FakeArchive { ranges: vec![(1, 63)] };
        let mut archive = CheckpointedArchive::new(inner, 64);

        archive.mark_dirty(63);
        // 63 / 64 = 0, boundary = 0, which is not > 0
        assert!(!archive.should_sync());
    }

    #[test]
    fn checkpointed_archive_zero_interval_behaves_as_one() {
        let inner = FakeArchive { ranges: vec![(1, 3)] };
        let mut archive_zero = CheckpointedArchive::new(inner, 0);
        archive_zero.mark_dirty(3);
        assert!(archive_zero.should_sync());

        let inner = FakeArchive { ranges: vec![(1, 3)] };
        let mut archive_one = CheckpointedArchive::new(inner, 1);
        archive_one.mark_dirty(3);
        assert!(archive_one.should_sync());
    }
}
