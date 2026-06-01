//! In-memory seed tracker implementation.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

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
    inner: Arc<RwLock<SeedState>>,
}

#[derive(Debug, Default)]
struct SeedState {
    seeds: BTreeMap<Digest, B256>,
    order: VecDeque<Digest>,
}

impl InMemorySeedTracker {
    /// Create a new seed tracker with genesis seed.
    #[must_use]
    pub fn new(genesis_digest: Digest) -> Self {
        let mut state = SeedState::default();
        state.seeds.insert(genesis_digest, B256::ZERO);
        state.order.push_back(genesis_digest);
        Self { inner: Arc::new(RwLock::new(state)) }
    }

    /// Create an empty seed tracker.
    #[must_use]
    pub fn empty() -> Self {
        Self { inner: Arc::new(RwLock::new(SeedState::default())) }
    }

    /// Return the current number of tracked seeds.
    pub fn len(&self) -> usize {
        self.inner.read().seeds.len()
    }

    /// Return true if the tracker contains no seeds.
    pub fn is_empty(&self) -> bool {
        self.inner.read().seeds.is_empty()
    }
}

impl Default for InMemorySeedTracker {
    fn default() -> Self {
        Self::empty()
    }
}

impl SeedTracker for InMemorySeedTracker {
    fn get(&self, digest: &Digest) -> Option<B256> {
        self.inner.read().seeds.get(digest).copied()
    }

    fn insert(&self, digest: Digest, seed: B256) {
        let mut inner = self.inner.write();
        if !inner.seeds.contains_key(&digest) {
            inner.order.push_back(digest);
        }
        inner.seeds.insert(digest, seed);
        while inner.seeds.len() > MAX_SEED_ENTRIES {
            let Some(oldest) = inner.order.pop_front() else {
                break;
            };
            inner.seeds.remove(&oldest);
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

    #[test]
    fn seed_tracker_prunes_by_insertion_order_not_digest_order() {
        let tracker = InMemorySeedTracker::empty();
        let oldest = Digest::from([0xffu8; 32]);
        tracker.insert(oldest, B256::repeat_byte(0xff));

        for i in 0..MAX_SEED_ENTRIES {
            let mut bytes = [0u8; 32];
            bytes[30] = (i >> 8) as u8;
            bytes[31] = (i & 0xff) as u8;
            tracker.insert(Digest::from(bytes), B256::repeat_byte(i as u8));
        }

        assert_eq!(tracker.len(), MAX_SEED_ENTRIES);
        assert!(tracker.get(&oldest).is_none());
        assert_eq!(tracker.get(&Digest::from([0u8; 32])), Some(B256::ZERO));
    }

    #[test]
    fn seed_tracker_duplicate_insert_does_not_duplicate_retention_order() {
        let tracker = InMemorySeedTracker::empty();
        let digest = Digest::from([0x01u8; 32]);

        tracker.insert(digest, B256::repeat_byte(0x01));
        tracker.insert(digest, B256::repeat_byte(0x02));

        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.get(&digest), Some(B256::repeat_byte(0x02)));
    }
}
