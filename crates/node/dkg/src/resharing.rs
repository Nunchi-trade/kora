//! DKG resharing interface for dynamic validator set management.
//!
//! This module defines the trait interface for DKG resharing -- the process of
//! redistributing threshold key shares to a new validator set without changing
//! the group public key. This is required for:
//!
//! - **Validator rotation**: adding or removing validators after genesis
//! - **Key rotation**: refreshing key shares to limit exposure from long-term compromise
//! - **Liveness recovery**: replacing permanently failed validators
//!
//! # Architecture (planned)
//!
//! The resharing protocol will be implemented as a Commonware actor that runs
//! within the main node process, using authenticated transport for all DKG
//! messages. This addresses audit issue #251 (DKG transport security) by
//! leveraging the existing P2P overlay rather than establishing standalone
//! network connections.
//!
//! The high-level flow for each epoch transition:
//!
//! 1. **Trigger**: The consensus engine reaches an epoch boundary (determined by
//!    [`ResharingConfig::epoch_length`](kora_config::ResharingConfig)).
//! 2. **Read validator registry**: An onchain `ValidatorManager` system contract
//!    provides the next epoch's validator set (additions, removals, key updates).
//! 3. **Reshare**: The current validator set runs a resharing ceremony to
//!    redistribute shares to the next validator set, preserving the group public key.
//! 4. **Transition**: The orchestrator actor stops the current consensus engine
//!    instance and starts a new one with the updated validator set and key shares.
//!
//! # Current status
//!
//! **This module contains only trait definitions and types.** The actual resharing
//! protocol, actor integration, and validator registry are not yet implemented.
//! See issue #103 for the full design and tracking.
//!
//! # References
//!
//! - Issue #103: DKG resharing tracking
//! - Audit #94: No key rotation
//! - Audit #251: DKG transport security (P0)
//! - Audit #283: Static validator set

use crate::{DkgError, DkgOutput};

/// Describes a planned change to the validator set for the next epoch.
///
/// This will be populated from the onchain `ValidatorManager` system contract
/// once it exists.
#[derive(Debug, Clone)]
pub struct ValidatorSetChange {
    /// The epoch number this change applies to.
    pub target_epoch: u64,

    /// Public keys of validators being added.
    pub additions: Vec<Vec<u8>>,

    /// Public keys of validators being removed.
    pub removals: Vec<Vec<u8>>,
}

/// Outcome of a successful resharing ceremony.
///
/// Contains the new key shares for the updated validator set. The group public
/// key remains unchanged across resharing ceremonies.
#[derive(Debug, Clone)]
pub struct ResharingOutput {
    /// The epoch this resharing was performed for.
    pub epoch: u64,

    /// The DKG output containing new key shares for this validator.
    ///
    /// The `group_public_key` field must match the previous epoch's group key.
    pub dkg_output: DkgOutput,

    /// The updated participant set for the new epoch.
    pub new_participants: Vec<Vec<u8>>,
}

/// Trait for DKG resharing coordination.
///
/// Implementors manage the lifecycle of resharing ceremonies, including
/// triggering at epoch boundaries, coordinating with the validator registry,
/// and producing new key shares.
///
/// # Planned implementations
///
/// - **`ResharingActor`**: A Commonware actor that runs the resharing protocol
///   using authenticated P2P transport within the node process.
/// - **`NoopResharing`**: A no-op implementation for networks with static
///   validator sets (the current default behavior).
pub trait Resharing: Send + Sync {
    /// Check whether a resharing ceremony should be initiated.
    ///
    /// Called by the consensus engine at each block to determine if the current
    /// block height corresponds to an epoch boundary that requires resharing.
    ///
    /// # Arguments
    ///
    /// * `current_height` - The current block height.
    /// * `epoch_length` - The configured epoch length in blocks.
    fn should_reshare(&self, current_height: u64, epoch_length: u64) -> bool;

    /// Initiate a resharing ceremony for the given validator set change.
    ///
    /// This is an async operation that coordinates with other validators to
    /// redistribute key shares. The group public key is preserved.
    ///
    /// # Errors
    ///
    /// Returns [`DkgError`] if the resharing ceremony fails (timeout, network
    /// issues, insufficient participation, etc.).
    fn initiate_resharing(
        &self,
        current_output: &DkgOutput,
        change: &ValidatorSetChange,
    ) -> Result<ResharingOutput, DkgError>;
}

/// No-op resharing implementation for static validator sets.
///
/// This is the default when `resharing.enabled = false`. It never triggers
/// resharing and always returns an error if resharing is attempted.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopResharing;

impl Resharing for NoopResharing {
    fn should_reshare(&self, _current_height: u64, _epoch_length: u64) -> bool {
        false
    }

    fn initiate_resharing(
        &self,
        _current_output: &DkgOutput,
        _change: &ValidatorSetChange,
    ) -> Result<ResharingOutput, DkgError> {
        Err(DkgError::CeremonyFailed(
            "resharing is not enabled; the validator set is static".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_resharing_never_triggers() {
        let noop = NoopResharing;
        assert!(!noop.should_reshare(0, 100));
        assert!(!noop.should_reshare(100, 100));
        assert!(!noop.should_reshare(1_000_000, 14_400));
    }

    #[test]
    fn noop_resharing_errors_on_initiate() {
        let noop = NoopResharing;
        let output = DkgOutput {
            group_public_key: vec![],
            public_polynomial: vec![],
            threshold: 2,
            participants: 3,
            share_index: 0,
            share_secret: vec![],
            participant_keys: vec![],
        };
        let change = ValidatorSetChange { target_epoch: 1, additions: vec![], removals: vec![] };
        let result = noop.initiate_resharing(&output, &change);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("resharing is not enabled"));
    }

    #[test]
    fn validator_set_change_debug() {
        let change = ValidatorSetChange {
            target_epoch: 5,
            additions: vec![vec![1, 2, 3]],
            removals: vec![vec![4, 5, 6]],
        };
        let debug = format!("{:?}", change);
        assert!(debug.contains("ValidatorSetChange"));
        assert!(debug.contains("target_epoch: 5"));
    }

    #[test]
    fn resharing_output_debug() {
        let output = ResharingOutput {
            epoch: 42,
            dkg_output: DkgOutput {
                group_public_key: vec![0xab],
                public_polynomial: vec![],
                threshold: 3,
                participants: 4,
                share_index: 1,
                share_secret: vec![],
                participant_keys: vec![],
            },
            new_participants: vec![vec![1], vec![2]],
        };
        let debug = format!("{:?}", output);
        assert!(debug.contains("ResharingOutput"));
        assert!(debug.contains("epoch: 42"));
    }
}
