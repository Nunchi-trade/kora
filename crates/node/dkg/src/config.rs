use std::{fmt, path::PathBuf, time::Duration};

use commonware_cryptography::ed25519;
use commonware_utils::{Faults, N3f1};

/// Configuration for a Distributed Key Generation (DKG) ceremony.
#[derive(Clone)]
pub struct DkgConfig {
    /// The validator's private identity key used for signing and authentication.
    pub identity_key: ed25519::PrivateKey,
    /// This validator's index in the participant set.
    pub validator_index: usize,
    /// Public keys of all validators participating in the DKG ceremony.
    pub participants: Vec<ed25519::PublicKey>,
    /// Chain identifier for domain separation.
    pub chain_id: u64,
    /// Directory for persisting DKG state and key shares.
    pub data_dir: PathBuf,
    /// Socket address to listen on for P2P communication.
    pub listen_addr: std::net::SocketAddr,
    /// Initial peers to connect to, as (public_key, address) pairs.
    pub bootstrap_peers: Vec<(ed25519::PublicKey, String)>,
    /// Timeout duration for DKG protocol rounds.
    pub timeout: Duration,
}

impl fmt::Debug for DkgConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DkgConfig")
            .field("identity_key", &"<redacted>")
            .field("validator_index", &self.validator_index)
            .field("participants", &self.participants)
            .field("chain_id", &self.chain_id)
            .field("data_dir", &self.data_dir)
            .field("listen_addr", &self.listen_addr)
            .field("bootstrap_peers", &self.bootstrap_peers)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl DkgConfig {
    /// Returns the total number of participants (n).
    pub const fn n(&self) -> usize {
        self.participants.len()
    }

    /// Returns the quorum / threshold value (t) as determined by N3f1.
    ///
    /// This is `n - f` where `f = (n-1)/3`. For example:
    /// - n=4: t=3 (tolerates 1 fault)
    /// - n=7: t=5 (tolerates 2 faults)
    /// - n=15: t=11 (tolerates 4 faults)
    pub fn t(&self) -> u32 {
        N3f1::quorum(self.participants.len())
    }

    /// Returns this validator's public key derived from the identity key.
    pub fn my_public_key(&self) -> ed25519::PublicKey {
        use commonware_cryptography::Signer;
        self.identity_key.public_key()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use commonware_cryptography::Signer;

    use super::*;

    /// Helper function to create a test DkgConfig with default values.
    fn test_config() -> DkgConfig {
        let identity_key = ed25519::PrivateKey::from_seed(42);
        let participants = vec![
            ed25519::PrivateKey::from_seed(42).public_key(),
            ed25519::PrivateKey::from_seed(43).public_key(),
            ed25519::PrivateKey::from_seed(44).public_key(),
            ed25519::PrivateKey::from_seed(45).public_key(),
        ];

        DkgConfig {
            identity_key,
            validator_index: 0,
            participants,
            chain_id: 1337,
            data_dir: PathBuf::from("/tmp/dkg-test"),
            listen_addr: "127.0.0.1:8000".parse::<SocketAddr>().unwrap(),
            bootstrap_peers: vec![],
            timeout: Duration::from_secs(60),
        }
    }

    #[test]
    fn test_n_returns_participant_count() {
        let config = test_config();
        assert_eq!(config.n(), 4);
    }

    #[test]
    fn test_t_returns_n3f1_quorum() {
        let config = test_config();
        // n=4: f=(4-1)/3=1, quorum=4-1=3
        assert_eq!(config.t(), 3);
    }

    #[test]
    fn test_t_with_fifteen_validators() {
        let identity_key = ed25519::PrivateKey::from_seed(42);
        let participants: Vec<_> =
            (0..15).map(|i| ed25519::PrivateKey::from_seed(i as u64).public_key()).collect();

        let config = DkgConfig {
            identity_key,
            validator_index: 0,
            participants,
            chain_id: 1337,
            data_dir: PathBuf::from("/tmp/dkg-test"),
            listen_addr: "127.0.0.1:8000".parse::<SocketAddr>().unwrap(),
            bootstrap_peers: vec![],
            timeout: Duration::from_secs(60),
        };
        // n=15: f=(15-1)/3=4, quorum=15-4=11 (NOT 10!)
        assert_eq!(config.t(), 11);
    }

    #[test]
    fn test_my_public_key_derived_from_identity() {
        let config = test_config();
        let expected_public_key = config.identity_key.public_key();
        let actual_public_key = config.my_public_key();
        assert_eq!(actual_public_key, expected_public_key);
    }

    #[test]
    fn test_dkg_config_clone() {
        let config = test_config();
        let cloned = config.clone();

        assert_eq!(config.n(), cloned.n());
        assert_eq!(config.t(), cloned.t());
        assert_eq!(config.my_public_key(), cloned.my_public_key());
        assert_eq!(config.chain_id, cloned.chain_id);
        assert_eq!(config.validator_index, cloned.validator_index);
    }

    #[test]
    fn test_debug_redacts_identity_key() {
        let config = test_config();
        let debug = format!("{config:?}");

        assert!(debug.contains("identity_key: \"<redacted>\""));
    }
}
