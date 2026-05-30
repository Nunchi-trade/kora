//! In-memory seed tracker implementation.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

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
/// Entries beyond [`MAX_SEEDS`] are evicted in insertion order (oldest first).
#[derive(Debug, Clone)]
pub struct InMemorySeedTracker {
    inner: Arc<RwLock<SeedEntries>>,
}

#[derive(Debug, Default)]
struct SeedEntries {
    values: BTreeMap<Digest, B256>,
    insertion_order: VecDeque<Digest>,
}

impl SeedEntries {
    fn insert(&mut self, digest: Digest, seed: B256) {
        if !self.values.contains_key(&digest) {
            self.insertion_order.push_back(digest);
        }
        self.values.insert(digest, seed);

        while self.values.len() > MAX_SEEDS {
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            self.values.remove(&oldest);
        }
    }

    fn get(&self, digest: &Digest) -> Option<B256> {
        self.values.get(digest).copied()
    }
}

impl InMemorySeedTracker {
    /// Create a new seed tracker with genesis seed.
    #[must_use]
    pub fn new(genesis_digest: Digest) -> Self {
        let mut seeds = SeedEntries::default();
        seeds.insert(genesis_digest, B256::ZERO);
        Self { inner: Arc::new(RwLock::new(seeds)) }
    }

    /// Create an empty seed tracker.
    #[must_use]
    pub fn empty() -> Self {
        Self { inner: Arc::new(RwLock::new(SeedEntries::default())) }
    }
}

impl Default for InMemorySeedTracker {
    fn default() -> Self {
        Self::empty()
    }
}

impl SeedTracker for InMemorySeedTracker {
    fn get(&self, digest: &Digest) -> Option<B256> {
        self.inner.read().get(digest)
    }

    fn insert(&self, digest: Digest, seed: B256) {
        self.inner.write().insert(digest, seed);
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
    fn seed_tracker_evicts_by_insertion_order_not_digest_order() {
        let tracker = InMemorySeedTracker::empty();
        let first_inserted = Digest::from([0xFFu8; 32]);

        tracker.insert(first_inserted, B256::repeat_byte(0xAA));
        for i in 0..MAX_SEEDS {
            let mut bytes = [0u8; 32];
            bytes[30..].copy_from_slice(&(i as u16).to_be_bytes());
            tracker.insert(Digest::from(bytes), B256::repeat_byte(0xBB));
        }

        assert_eq!(tracker.get(&first_inserted), None);
        assert_eq!(tracker.get(&Digest::from([0u8; 32])), Some(B256::repeat_byte(0xBB)));
    }
}
