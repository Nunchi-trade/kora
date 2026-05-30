//! In-memory seed tracker implementation.

use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::B256;
use parking_lot::RwLock;

use crate::traits::{Digest, SeedTracker};

/// Maximum number of seed entries retained in memory.
///
/// This bounds memory growth to roughly `MAX_SEEDS * 96 bytes` (~6 MB).
/// The value covers at least 2 checkpoint intervals (512 blocks) plus
/// margin for concurrent consensus views.
const MAX_SEEDS: usize = 1024;

/// Simple in-memory seed tracker with bounded capacity.
///
/// Entries beyond [`MAX_SEEDS`] are evicted in insertion order (oldest first
/// by [`BTreeMap`] key ordering).
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
        let mut map = self.inner.write();
        map.insert(digest, seed);
        // Evict oldest entries when the map exceeds the capacity bound.
        while map.len() > MAX_SEEDS {
            map.pop_first();
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
}
