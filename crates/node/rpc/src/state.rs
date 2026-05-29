//! Node state management for RPC endpoints.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Default validator count used by tests and legacy callers.
pub(crate) const DEFAULT_VALIDATOR_COUNT: u32 = 4;

/// Network partition status derived from peer connectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionStatus {
    /// All expected peers are connected.
    Healthy,
    /// Some peers are missing but quorum is still possible.
    Degraded,
    /// Too few peers for BFT quorum (fewer than n-f).
    Partitioned,
}

impl PartitionStatus {
    /// Derive partition status from the number of connected peers and total
    /// expected peers (i.e. `validator_count - 1`).
    ///
    /// Commonware simplex uses an N3f1 quorum model: with `n` validators and
    /// `f = (n-1)/3` maximum Byzantine faults, quorum requires `n - f`
    /// participants.  A node needs at least `n - f - 1` *other* peers to form
    /// quorum (since it counts itself as one of the `n - f` participants).
    const fn from_peer_counts(connected_peers: u64, total_expected_peers: u64) -> Self {
        if connected_peers >= total_expected_peers {
            Self::Healthy
        } else {
            // total_validators = total_expected_peers + 1 (include self)
            let total_validators = total_expected_peers + 1;
            // f = (n-1) / 3, quorum = n - f, peers needed = quorum - 1 (self)
            let f = (total_validators.saturating_sub(1)) / 3;
            let quorum_peers_needed = total_validators - f - 1; // (n - f) - 1 for self
            if connected_peers >= quorum_peers_needed { Self::Degraded } else { Self::Partitioned }
        }
    }
}

/// Shared node state that can be updated by the consensus engine.
#[derive(Debug, Clone)]
pub struct NodeState {
    inner: Arc<NodeStateInner>,
}

#[derive(Debug)]
struct NodeStateInner {
    chain_id: u64,
    validator_index: u32,
    validator_count: NonZeroU32,
    started_at: Instant,
    current_view: AtomicU64,
    finalized_count: AtomicU64,
    finalized_height: AtomicU64,
    proposed_count: AtomicU64,
    nullified_count: AtomicU64,
    equivocation_count: AtomicU64,
    peer_count: AtomicU64,
    is_leader: RwLock<bool>,
}

impl NodeState {
    /// Create a new node state.
    ///
    /// Uses the historical four-validator leader schedule. Validator mode should prefer
    /// [`Self::with_validator_count`] so leadership follows the configured validator set.
    #[must_use]
    pub fn new(chain_id: u64, validator_index: u32) -> Self {
        Self::with_validator_count(chain_id, validator_index, DEFAULT_VALIDATOR_COUNT)
    }

    /// Create a new node state with an explicit validator count.
    ///
    /// # Panics
    ///
    /// Panics if `validator_count` is zero or if `validator_index >= validator_count`.
    #[must_use]
    pub fn with_validator_count(chain_id: u64, validator_index: u32, validator_count: u32) -> Self {
        let validator_count =
            NonZeroU32::new(validator_count).expect("validator count must be non-zero");

        assert!(
            validator_index < validator_count.get(),
            "validator_index ({validator_index}) must be less than validator_count ({validator_count})",
        );

        Self {
            inner: Arc::new(NodeStateInner {
                chain_id,
                validator_index,
                validator_count,
                started_at: Instant::now(),
                current_view: AtomicU64::new(0),
                finalized_count: AtomicU64::new(0),
                finalized_height: AtomicU64::new(0),
                proposed_count: AtomicU64::new(0),
                nullified_count: AtomicU64::new(0),
                equivocation_count: AtomicU64::new(0),
                peer_count: AtomicU64::new(0),
                is_leader: RwLock::new(false),
            }),
        }
    }

    /// Update the current view.
    pub fn set_view(&self, view: u64) {
        self.inner.current_view.store(view, Ordering::Relaxed);
        let leader_index = (view % u64::from(self.inner.validator_count.get())) as u32;
        let is_leader = leader_index == self.inner.validator_index;
        *self.inner.is_leader.write() = is_leader;
    }

    /// Increment finalized block count.
    pub fn inc_finalized(&self) {
        self.inner.finalized_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update the latest finalized block height.
    ///
    /// Uses `fetch_max` so that out-of-order updates never regress the value.
    pub fn set_finalized_height(&self, height: u64) {
        self.inner.finalized_height.fetch_max(height, Ordering::Relaxed);
    }

    /// Return the latest finalized block height.
    pub fn finalized_height(&self) -> u64 {
        self.inner.finalized_height.load(Ordering::Relaxed)
    }

    /// Increment proposed block count.
    pub fn inc_proposed(&self) {
        self.inner.proposed_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment nullified round count.
    pub fn inc_nullified(&self) {
        self.inner.nullified_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment equivocation event count.
    pub fn inc_equivocations(&self) {
        self.inner.equivocation_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update peer count.
    pub fn set_peer_count(&self, count: u64) {
        self.inner.peer_count.store(count, Ordering::Relaxed);
    }

    /// Get current node status.
    pub fn status(&self) -> NodeStatus {
        let peer_count = self.inner.peer_count.load(Ordering::Relaxed);
        let total_expected_peers = u64::from(self.inner.validator_count.get()).saturating_sub(1);
        let partition_status = PartitionStatus::from_peer_counts(peer_count, total_expected_peers);

        NodeStatus {
            chain_id: self.inner.chain_id,
            validator_index: self.inner.validator_index,
            uptime_secs: self.inner.started_at.elapsed().as_secs(),
            current_view: self.inner.current_view.load(Ordering::Relaxed),
            finalized_count: self.inner.finalized_count.load(Ordering::Relaxed),
            proposed_count: self.inner.proposed_count.load(Ordering::Relaxed),
            nullified_count: self.inner.nullified_count.load(Ordering::Relaxed),
            equivocation_count: self.inner.equivocation_count.load(Ordering::Relaxed),
            peer_count,
            total_expected_peers,
            partition_status,
            is_leader: *self.inner.is_leader.read(),
        }
    }
}

/// Serializable node status for RPC responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    /// Chain ID.
    pub chain_id: u64,
    /// This validator's index.
    pub validator_index: u32,
    /// Seconds since node started.
    pub uptime_secs: u64,
    /// Current consensus view number.
    pub current_view: u64,
    /// Number of finalized blocks.
    pub finalized_count: u64,
    /// Number of blocks proposed by this node.
    pub proposed_count: u64,
    /// Number of nullified rounds.
    pub nullified_count: u64,
    /// Number of equivocation events detected (Byzantine behavior).
    pub equivocation_count: u64,
    /// Number of connected peers.
    pub peer_count: u64,
    /// Total number of expected peers (validator_count - 1).
    pub total_expected_peers: u64,
    /// Network partition status derived from peer connectivity.
    pub partition_status: PartitionStatus,
    /// Whether this node is the current leader.
    pub is_leader: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_status_serde_roundtrip() {
        let status = NodeStatus {
            chain_id: 1337,
            validator_index: 2,
            uptime_secs: 3600,
            current_view: 100,
            finalized_count: 50,
            proposed_count: 10,
            nullified_count: 5,
            equivocation_count: 2,
            peer_count: 3,
            total_expected_peers: 3,
            partition_status: PartitionStatus::Healthy,
            is_leader: true,
        };

        let json = serde_json::to_string(&status).unwrap();
        let parsed: NodeStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(status.chain_id, parsed.chain_id);
        assert_eq!(status.validator_index, parsed.validator_index);
        assert_eq!(status.uptime_secs, parsed.uptime_secs);
        assert_eq!(status.current_view, parsed.current_view);
        assert_eq!(status.finalized_count, parsed.finalized_count);
        assert_eq!(status.proposed_count, parsed.proposed_count);
        assert_eq!(status.nullified_count, parsed.nullified_count);
        assert_eq!(status.equivocation_count, parsed.equivocation_count);
        assert_eq!(status.peer_count, parsed.peer_count);
        assert_eq!(status.total_expected_peers, parsed.total_expected_peers);
        assert_eq!(status.partition_status, parsed.partition_status);
        assert_eq!(status.is_leader, parsed.is_leader);
    }

    #[test]
    fn node_status_json_uses_camel_case() {
        let status = NodeStatus {
            chain_id: 1,
            validator_index: 0,
            uptime_secs: 0,
            current_view: 0,
            finalized_count: 0,
            proposed_count: 0,
            nullified_count: 0,
            equivocation_count: 0,
            peer_count: 0,
            total_expected_peers: 3,
            partition_status: PartitionStatus::Partitioned,
            is_leader: false,
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("chainId"));
        assert!(json.contains("validatorIndex"));
        assert!(json.contains("uptimeSecs"));
        assert!(json.contains("currentView"));
        assert!(json.contains("finalizedCount"));
        assert!(json.contains("proposedCount"));
        assert!(json.contains("nullifiedCount"));
        assert!(json.contains("equivocationCount"));
        assert!(json.contains("peerCount"));
        assert!(json.contains("totalExpectedPeers"));
        assert!(json.contains("partitionStatus"));
        assert!(json.contains("isLeader"));
    }

    #[test]
    fn node_state_new() {
        let state = NodeState::new(1337, 2);
        let status = state.status();
        assert_eq!(status.chain_id, 1337);
        assert_eq!(status.validator_index, 2);
        assert!(!status.is_leader);
    }

    #[test]
    fn node_state_set_view() {
        let state = NodeState::new(1, 0);
        state.set_view(4);
        let status = state.status();
        assert_eq!(status.current_view, 4);
        assert!(status.is_leader);
    }

    #[test]
    fn node_state_leadership_uses_validator_count() {
        let state = NodeState::with_validator_count(1, 4, 5);

        state.set_view(4);
        assert!(state.status().is_leader);

        state.set_view(5);
        assert!(!state.status().is_leader);

        state.set_view(9);
        assert!(state.status().is_leader);
    }

    #[test]
    fn node_state_leadership_supports_non_four_validator_sets() {
        let state = NodeState::with_validator_count(1, 2, 3);

        state.set_view(2);
        assert!(state.status().is_leader);

        state.set_view(3);
        assert!(!state.status().is_leader);

        state.set_view(5);
        assert!(state.status().is_leader);
    }

    #[test]
    #[should_panic(expected = "validator count must be non-zero")]
    fn node_state_validator_count_must_be_nonzero() {
        let _ = NodeState::with_validator_count(1, 0, 0);
    }

    #[test]
    #[should_panic(expected = "validator_index (5) must be less than validator_count (4)")]
    fn node_state_validator_index_must_be_in_range() {
        let _ = NodeState::with_validator_count(1, 5, 4);
    }

    #[test]
    fn node_state_inc_counters() {
        let state = NodeState::new(1, 0);
        state.inc_finalized();
        state.inc_finalized();
        state.inc_proposed();
        state.inc_nullified();
        state.inc_equivocations();
        state.inc_equivocations();
        state.inc_equivocations();

        let status = state.status();
        assert_eq!(status.finalized_count, 2);
        assert_eq!(status.proposed_count, 1);
        assert_eq!(status.nullified_count, 1);
        assert_eq!(status.equivocation_count, 3);
    }

    #[test]
    fn node_state_set_peer_count() {
        let state = NodeState::new(1, 0);
        state.set_peer_count(5);
        assert_eq!(state.status().peer_count, 5);
    }

    #[test]
    fn node_state_finalized_height() {
        let state = NodeState::new(1, 0);
        assert_eq!(state.finalized_height(), 0);

        state.set_finalized_height(42);
        assert_eq!(state.finalized_height(), 42);

        // fetch_max ensures height never regresses
        state.set_finalized_height(10);
        assert_eq!(state.finalized_height(), 42);

        state.set_finalized_height(100);
        assert_eq!(state.finalized_height(), 100);
    }

    // -- PartitionStatus tests --

    #[test]
    fn partition_status_healthy_when_all_peers_connected() {
        // 4 validators: 3 expected peers, 3 connected
        assert_eq!(PartitionStatus::from_peer_counts(3, 3), PartitionStatus::Healthy);
    }

    #[test]
    fn partition_status_degraded_when_one_peer_missing() {
        // 4 validators (f=1): quorum = n-f = 3, need 2 peers + self
        assert_eq!(PartitionStatus::from_peer_counts(2, 3), PartitionStatus::Degraded);
    }

    #[test]
    fn partition_status_partitioned_when_below_quorum() {
        // 4 validators (f=1): quorum = n-f = 3, need 2 peers + self, have 1
        assert_eq!(PartitionStatus::from_peer_counts(1, 3), PartitionStatus::Partitioned);
    }

    #[test]
    fn partition_status_partitioned_when_no_peers() {
        assert_eq!(PartitionStatus::from_peer_counts(0, 3), PartitionStatus::Partitioned);
    }

    #[test]
    fn partition_status_seven_validators() {
        // 7 validators (f=2): quorum = n-f = 5, need 4 peers + self
        assert_eq!(PartitionStatus::from_peer_counts(6, 6), PartitionStatus::Healthy);
        assert_eq!(PartitionStatus::from_peer_counts(5, 6), PartitionStatus::Degraded);
        assert_eq!(PartitionStatus::from_peer_counts(4, 6), PartitionStatus::Degraded);
        assert_eq!(PartitionStatus::from_peer_counts(3, 6), PartitionStatus::Partitioned);
    }

    #[test]
    fn partition_status_fifteen_validators() {
        // 15 validators (f=4): quorum = n-f = 11, need 10 peers + self
        // This is the case where the old 2f formula diverged from n-f.
        assert_eq!(PartitionStatus::from_peer_counts(14, 14), PartitionStatus::Healthy);
        assert_eq!(PartitionStatus::from_peer_counts(10, 14), PartitionStatus::Degraded);
        assert_eq!(PartitionStatus::from_peer_counts(9, 14), PartitionStatus::Partitioned);
        assert_eq!(PartitionStatus::from_peer_counts(8, 14), PartitionStatus::Partitioned);
    }

    #[test]
    fn partition_status_serializes_lowercase() {
        let healthy = serde_json::to_string(&PartitionStatus::Healthy).unwrap();
        assert_eq!(healthy, "\"healthy\"");
        let degraded = serde_json::to_string(&PartitionStatus::Degraded).unwrap();
        assert_eq!(degraded, "\"degraded\"");
        let partitioned = serde_json::to_string(&PartitionStatus::Partitioned).unwrap();
        assert_eq!(partitioned, "\"partitioned\"");
    }

    #[test]
    fn partition_status_included_in_node_status() {
        // With 4 validators (default), peer_count=0 should be partitioned
        let state = NodeState::new(1, 0);
        let status = state.status();
        assert_eq!(status.total_expected_peers, 3);
        assert_eq!(status.partition_status, PartitionStatus::Partitioned);

        // Set all peers connected
        state.set_peer_count(3);
        let status = state.status();
        assert_eq!(status.partition_status, PartitionStatus::Healthy);

        // One peer missing
        state.set_peer_count(2);
        let status = state.status();
        assert_eq!(status.partition_status, PartitionStatus::Degraded);
    }
}
