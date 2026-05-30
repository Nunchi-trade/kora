//! In-memory Ethereum filter state.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::B256;
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::{Mutex, MutexGuard};

use crate::types::{RpcLog, RpcLogFilter};

/// Default lifetime for inactive HTTP filters.
pub(crate) const DEFAULT_FILTER_TTL: Duration = Duration::from_secs(5 * 60);

/// Default maximum number of active HTTP filters.
pub(crate) const DEFAULT_MAX_FILTERS: usize = 1024;

/// Unique server-local filter identifier.
pub(crate) type FilterId = u64;

/// Response payload for `eth_getFilterChanges`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum FilterChanges {
    /// Log filter changes.
    Logs(Vec<RpcLog>),
    /// Block or pending transaction hash changes.
    Hashes(Vec<B256>),
}

/// Server-side Ethereum filter cursor.
#[derive(Debug)]
pub(crate) enum Filter {
    /// Log filter cursor.
    Log {
        /// Log matching criteria supplied at filter creation.
        criteria: RpcLogFilter,
        /// Last block included by `eth_getFilterChanges`.
        /// `None` means no blocks have been polled yet (first poll starts
        /// from the filter's `from_block`).
        last_poll_block: Option<u64>,
    },
    /// Block filter cursor.
    Block {
        /// Last block included by `eth_getFilterChanges`.
        last_poll_block: u64,
    },
    /// Pending transaction filter cursor.
    PendingTransaction {
        /// Snapshot index into the shared insertion-order vec at the time
        /// of last poll (or filter creation). New hashes are those at
        /// indices >= this value.
        last_seen_index: usize,
    },
}

/// A single filter entry plus its TTL bookkeeping.
#[derive(Debug)]
pub(crate) struct FilterEntry {
    filter: Mutex<Filter>,
    last_poll_time: RwLock<Instant>,
}

impl FilterEntry {
    fn new(filter: Filter) -> Self {
        Self { filter: Mutex::new(filter), last_poll_time: RwLock::new(Instant::now()) }
    }

    #[cfg(test)]
    fn new_at(filter: Filter, last_poll_time: Instant) -> Self {
        Self { filter: Mutex::new(filter), last_poll_time: RwLock::new(last_poll_time) }
    }

    pub(crate) async fn lock(&self) -> MutexGuard<'_, Filter> {
        self.filter.lock().await
    }

    pub(crate) fn touch(&self) {
        *self.last_poll_time.write() = Instant::now();
    }

    fn last_poll_time(&self) -> Instant {
        *self.last_poll_time.read()
    }
}

/// Bounded in-memory store for active Ethereum HTTP filters.
#[derive(Debug)]
pub(crate) struct FilterStore {
    filters: RwLock<HashMap<FilterId, Arc<FilterEntry>>>,
    next_id: AtomicU64,
    max_filters: usize,
    ttl: Duration,
}

impl Default for FilterStore {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FILTERS, DEFAULT_FILTER_TTL)
    }
}

impl FilterStore {
    /// Create a store with a maximum entry count and inactive-entry TTL.
    pub(crate) fn new(max_filters: usize, ttl: Duration) -> Self {
        assert!(max_filters > 0, "filter store must allow at least one filter");
        Self { filters: RwLock::new(HashMap::new()), next_id: AtomicU64::new(1), max_filters, ttl }
    }

    /// Insert a filter and return its id.
    pub(crate) fn create(&self, filter: Filter) -> FilterId {
        self.cleanup_expired();

        let mut id = self.next_filter_id();
        let mut filters = self.filters.write();
        while filters.contains_key(&id) {
            id = self.next_filter_id();
        }
        if filters.len() >= self.max_filters {
            Self::evict_oldest(&mut filters);
        }
        filters.insert(id, Arc::new(FilterEntry::new(filter)));
        id
    }

    /// Return a filter entry if it exists and has not expired.
    pub(crate) fn get(&self, id: FilterId) -> Option<Arc<FilterEntry>> {
        self.cleanup_expired();
        self.filters.read().get(&id).cloned()
    }

    /// Remove a filter by id.
    pub(crate) fn remove(&self, id: FilterId) -> bool {
        self.filters.write().remove(&id).is_some()
    }

    /// Remove filters that have not been polled within the TTL.
    pub(crate) fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut filters = self.filters.write();
        let before = filters.len();
        filters.retain(|_, entry| now.duration_since(entry.last_poll_time()) < self.ttl);
        before - filters.len()
    }

    fn next_filter_id(&self) -> FilterId {
        loop {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
        }
    }

    fn evict_oldest(filters: &mut HashMap<FilterId, Arc<FilterEntry>>) {
        if let Some(id) =
            filters.iter().min_by_key(|(_, entry)| entry.last_poll_time()).map(|(id, _)| *id)
        {
            filters.remove(&id);
        }
    }

    #[cfg(test)]
    fn create_at(&self, filter: Filter, last_poll_time: Instant) -> FilterId {
        let id = self.next_filter_id();
        self.filters.write().insert(id, Arc::new(FilterEntry::new_at(filter, last_poll_time)));
        id
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.filters.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_filter(last_poll_block: u64) -> Filter {
        Filter::Block { last_poll_block }
    }

    #[test]
    fn filter_store_create_and_get() {
        let store = FilterStore::new(16, Duration::from_secs(300));
        let id = store.create(block_filter(10));

        assert!(store.get(id).is_some());
        assert!(store.get(id + 999).is_none());
    }

    #[test]
    fn filter_store_remove() {
        let store = FilterStore::new(16, Duration::from_secs(300));
        let id = store.create(block_filter(0));

        assert!(store.remove(id));
        assert!(!store.remove(id));
        assert!(store.get(id).is_none());
    }

    #[test]
    fn filter_store_cleanup_expired() {
        let store = FilterStore::new(16, Duration::from_millis(50));
        let expired = store.create_at(block_filter(0), Instant::now() - Duration::from_millis(100));
        let fresh = store.create_at(block_filter(0), Instant::now());

        assert_eq!(store.cleanup_expired(), 1);
        assert!(store.get(expired).is_none());
        assert!(store.get(fresh).is_some());
    }

    #[test]
    fn filter_store_evicts_oldest_when_bounded() {
        let store = FilterStore::new(1, Duration::from_secs(300));
        let first = store.create(block_filter(0));
        let second = store.create(block_filter(1));

        assert!(store.get(first).is_none());
        assert!(store.get(second).is_some());
        assert_eq!(store.len(), 1);
    }
}
