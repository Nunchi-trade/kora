//! In-memory seed tracker implementation.

use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::B256;
use parking_lot::RwLock;

use crate::traits::{Digest, SeedTracker};

/// Maximum number of seed entries retained before pruning.
///
/// Only the most recent parent's seed is ever queried, so older entries
/// serve no purpose. 256 matches the snapshot store's default retention
/// and caps memory at approximately 25 KB.
const MAX_SEED_ENTRIES: usize = 256;

/// Simple in-memory seed tracker with bounded retention.
#[derive(Debug, Clone)]
pub struct InMemorySeedTracker {
    inner: Arc<RwLock<BTreeMap<Digest, B256>>>,
}

impl InMemorySeedTracker {
    /// Create a new seed tracker with genesis seed.
    #[must_use]
    pub fn new(genesis_digest: Digest) -> Self {
        let mut seeds = BTreeMap::new();
        seeds.insert(genesis_digest, B256::ZERO);
        Self { inner: Arc::new(RwLock::new(seeds)) }
    }

    /// Create an empty seed tracker.
    #[must_use]
    pub fn empty() -> Self {
        Self { inner: Arc::new(RwLock::new(BTreeMap::new())) }
    }

    /// Return the current number of tracked seeds.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Return true if the tracker contains no seeds.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for InMemorySeedTracker {
    fn default() -> Self {
        Self::empty()
    }
}

impl SeedTracker for InMemorySeedTracker {
    fn get(&self, digest: &Digest) -> Option<B256> {
        self.inner.read().get(digest).copied()
    }

    fn insert(&self, digest: Digest, seed: B256) {
        let mut inner = self.inner.write();
        inner.insert(digest, seed);
        // Prune oldest entries to bound memory usage.
        while inner.len() > MAX_SEED_ENTRIES {
            inner.pop_first();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_tracker_insert_and_get() {
        let tracker = InMemorySeedTracker::empty();

        let digest = Digest::from([0x01u8; 32]);
        let seed = B256::repeat_byte(0x02);

        assert!(tracker.get(&digest).is_none());

        tracker.insert(digest, seed);
        assert_eq!(tracker.get(&digest), Some(seed));
    }

    #[test]
    fn seed_tracker_genesis() {
        let genesis = Digest::from([0xABu8; 32]);
        let tracker = InMemorySeedTracker::new(genesis);

        assert_eq!(tracker.get(&genesis), Some(B256::ZERO));
    }

    #[test]
    fn seed_tracker_prunes_oldest_entries() {
        let tracker = InMemorySeedTracker::empty();

        // Insert MAX_SEED_ENTRIES + 10 entries.
        for i in 0..=(MAX_SEED_ENTRIES + 10) as u16 {
            let mut bytes = [0u8; 32];
            bytes[0] = (i >> 8) as u8;
            bytes[1] = (i & 0xff) as u8;
            let digest = Digest::from(bytes);
            tracker.insert(digest, B256::repeat_byte(i as u8));
        }

        // Should be bounded at MAX_SEED_ENTRIES.
        assert_eq!(tracker.len(), MAX_SEED_ENTRIES);
    }
}
