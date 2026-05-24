//! Consensus configuration.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use alloy_primitives::hex;
use commonware_codec::{FixedSize, ReadExt};
use commonware_cryptography::ed25519;
use serde::{Deserialize, Serialize};

use crate::ConfigError;

/// Default validator threshold.
pub const DEFAULT_THRESHOLD: u32 = 2;

/// Default maximum transactions decoded per block.
pub const DEFAULT_BLOCK_CODEC_MAX_TXS: usize = 10_000;

/// Default maximum bytes decoded per transaction in a block.
pub const DEFAULT_BLOCK_CODEC_MAX_TX_BYTES: usize = 8 * 1024 * 1024;

/// Default Simplex replay buffer size in bytes.
pub const DEFAULT_SIMPLEX_REPLAY_BUFFER_BYTES: usize = 16 * 1024 * 1024;

/// Default Simplex write buffer size in bytes.
pub const DEFAULT_SIMPLEX_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;

/// Default Simplex leader timeout in seconds.
///
/// Healthy views complete in ~7ms, so even 1 second provides ample margin.
/// A lower timeout limits the throughput penalty when a dead leader's turn
/// is reached in the round-robin schedule.
pub const DEFAULT_SIMPLEX_LEADER_TIMEOUT_SECS: u64 = 1;

/// Default Simplex certification timeout in seconds.
///
/// Healthy views complete in ~7ms, so 2 seconds provides a generous margin
/// for stragglers while avoiding 10-second stalls when certification fails.
/// This matches the underlying simplex crate default
/// ([`DEFAULT_NOTARIZATION_TIMEOUT`]).
pub const DEFAULT_SIMPLEX_CERTIFICATION_TIMEOUT_SECS: u64 = 2;

/// Default Simplex nullification retry timeout in seconds.
pub const DEFAULT_SIMPLEX_TIMEOUT_RETRY_SECS: u64 = 2;

/// Default Simplex fetch timeout in seconds.
pub const DEFAULT_SIMPLEX_FETCH_TIMEOUT_SECS: u64 = 5;

/// Default Simplex activity timeout in views.
pub const DEFAULT_SIMPLEX_ACTIVITY_TIMEOUT_VIEWS: u64 = 20;

/// Default Simplex skip timeout in views.
pub const DEFAULT_SIMPLEX_SKIP_TIMEOUT_VIEWS: u64 = 10;

/// Default number of concurrent Simplex fetch requests.
pub const DEFAULT_SIMPLEX_FETCH_CONCURRENT: usize = 8;

/// Block codec limits used by consensus networking and storage.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusBlockCodecConfig {
    /// Maximum number of transactions decoded per block.
    #[serde(default = "default_block_codec_max_txs")]
    pub max_txs: NonZeroUsize,

    /// Maximum bytes decoded per transaction in a block.
    #[serde(default = "default_block_codec_max_tx_bytes")]
    pub max_tx_bytes: NonZeroUsize,
}

impl Default for ConsensusBlockCodecConfig {
    fn default() -> Self {
        Self {
            max_txs: default_block_codec_max_txs(),
            max_tx_bytes: default_block_codec_max_tx_bytes(),
        }
    }
}

/// Simplex consensus tuning parameters.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusSimplexConfig {
    /// Replay buffer size in bytes.
    #[serde(default = "default_simplex_replay_buffer_bytes")]
    pub replay_buffer_bytes: NonZeroUsize,

    /// Write buffer size in bytes.
    #[serde(default = "default_simplex_write_buffer_bytes")]
    pub write_buffer_bytes: NonZeroUsize,

    /// Leader timeout in seconds.
    #[serde(default = "default_simplex_leader_timeout_secs")]
    pub leader_timeout_secs: NonZeroU64,

    /// Certification timeout in seconds.
    #[serde(default = "default_simplex_certification_timeout_secs")]
    pub certification_timeout_secs: NonZeroU64,

    /// Retry timeout after nullification in seconds.
    #[serde(default = "default_simplex_timeout_retry_secs")]
    pub timeout_retry_secs: NonZeroU64,

    /// Fetch timeout in seconds.
    #[serde(default = "default_simplex_fetch_timeout_secs")]
    pub fetch_timeout_secs: NonZeroU64,

    /// Activity timeout in views.
    #[serde(default = "default_simplex_activity_timeout_views")]
    pub activity_timeout_views: NonZeroU64,

    /// Skip timeout in views.
    #[serde(default = "default_simplex_skip_timeout_views")]
    pub skip_timeout_views: NonZeroU64,

    /// Maximum concurrent fetch requests.
    #[serde(default = "default_simplex_fetch_concurrent")]
    pub fetch_concurrent: NonZeroUsize,
}

impl Default for ConsensusSimplexConfig {
    fn default() -> Self {
        Self {
            replay_buffer_bytes: default_simplex_replay_buffer_bytes(),
            write_buffer_bytes: default_simplex_write_buffer_bytes(),
            leader_timeout_secs: default_simplex_leader_timeout_secs(),
            certification_timeout_secs: default_simplex_certification_timeout_secs(),
            timeout_retry_secs: default_simplex_timeout_retry_secs(),
            fetch_timeout_secs: default_simplex_fetch_timeout_secs(),
            activity_timeout_views: default_simplex_activity_timeout_views(),
            skip_timeout_views: default_simplex_skip_timeout_views(),
            fetch_concurrent: default_simplex_fetch_concurrent(),
        }
    }
}

/// Consensus layer configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusConfig {
    /// Path to the validator key file.
    #[serde(default)]
    pub validator_key: Option<PathBuf>,

    /// Threshold for consensus (e.g., 2f+1 of 3f+1).
    #[serde(default = "default_threshold")]
    pub threshold: u32,

    /// List of participant public keys (hex-encoded).
    #[serde(
        default,
        serialize_with = "serialize_participants",
        deserialize_with = "deserialize_participants"
    )]
    pub participants: Vec<Vec<u8>>,

    /// Block codec limits used by consensus.
    #[serde(default)]
    pub block_codec: ConsensusBlockCodecConfig,

    /// Simplex consensus tuning parameters.
    #[serde(default)]
    pub simplex: ConsensusSimplexConfig,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            validator_key: None,
            threshold: DEFAULT_THRESHOLD,
            participants: Vec::new(),
            block_codec: ConsensusBlockCodecConfig::default(),
            simplex: ConsensusSimplexConfig::default(),
        }
    }
}

impl ConsensusConfig {
    /// Build the validator set from configured participants.
    ///
    /// Parses the hex-encoded participant public keys into [`ed25519::PublicKey`] values.
    /// Returns an empty set if no participants are configured.
    pub fn build_validator_set(&self) -> Result<Vec<ed25519::PublicKey>, ConfigError> {
        self.participants
            .iter()
            .map(|bytes| {
                if bytes.len() != ed25519::PublicKey::SIZE {
                    return Err(ConfigError::InvalidParticipantKeyLength(bytes.len()));
                }
                let mut buf = bytes.as_slice();
                ed25519::PublicKey::read(&mut buf).map_err(|_| ConfigError::InvalidParticipantKey)
            })
            .collect()
    }
}

const fn default_threshold() -> u32 {
    DEFAULT_THRESHOLD
}

const fn default_block_codec_max_txs() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_BLOCK_CODEC_MAX_TXS).expect("default block codec max txs is non-zero")
}

const fn default_block_codec_max_tx_bytes() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_BLOCK_CODEC_MAX_TX_BYTES)
        .expect("default block codec max tx bytes is non-zero")
}

const fn default_simplex_replay_buffer_bytes() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_SIMPLEX_REPLAY_BUFFER_BYTES)
        .expect("default simplex replay buffer is non-zero")
}

const fn default_simplex_write_buffer_bytes() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_SIMPLEX_WRITE_BUFFER_BYTES)
        .expect("default simplex write buffer is non-zero")
}

const fn default_simplex_leader_timeout_secs() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SIMPLEX_LEADER_TIMEOUT_SECS)
        .expect("default simplex leader timeout is non-zero")
}

const fn default_simplex_certification_timeout_secs() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SIMPLEX_CERTIFICATION_TIMEOUT_SECS)
        .expect("default simplex certification timeout is non-zero")
}

const fn default_simplex_timeout_retry_secs() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SIMPLEX_TIMEOUT_RETRY_SECS)
        .expect("default simplex retry timeout is non-zero")
}

const fn default_simplex_fetch_timeout_secs() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SIMPLEX_FETCH_TIMEOUT_SECS)
        .expect("default simplex fetch timeout is non-zero")
}

const fn default_simplex_activity_timeout_views() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SIMPLEX_ACTIVITY_TIMEOUT_VIEWS)
        .expect("default simplex activity timeout is non-zero")
}

const fn default_simplex_skip_timeout_views() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SIMPLEX_SKIP_TIMEOUT_VIEWS)
        .expect("default simplex skip timeout is non-zero")
}

const fn default_simplex_fetch_concurrent() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_SIMPLEX_FETCH_CONCURRENT)
        .expect("default simplex fetch concurrency is non-zero")
}

fn serialize_participants<S>(participants: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(participants.len()))?;
    for p in participants {
        seq.serialize_element(&hex::encode(p))?;
    }
    seq.end()
}

fn deserialize_participants<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let strings: Vec<String> = Vec::deserialize(deserializer)?;
    strings
        .into_iter()
        .map(|s| hex::decode(s.strip_prefix("0x").unwrap_or(&s)).map_err(serde::de::Error::custom))
        .collect()
}

#[cfg(test)]
mod tests {
    use commonware_codec::Write as _;
    use commonware_cryptography::Signer as _;

    use super::*;

    fn create_valid_public_key_bytes() -> Vec<u8> {
        let private_key =
            ed25519::PrivateKey::from(ed25519_consensus::SigningKey::from([42u8; 32]));
        let public_key = private_key.public_key();
        let mut bytes = Vec::new();
        public_key.write(&mut bytes);
        bytes
    }

    #[test]
    fn default_consensus_config() {
        let config = ConsensusConfig::default();
        assert!(config.validator_key.is_none());
        assert_eq!(config.threshold, DEFAULT_THRESHOLD);
        assert!(config.participants.is_empty());
        assert_eq!(config.block_codec.max_txs.get(), DEFAULT_BLOCK_CODEC_MAX_TXS);
        assert_eq!(config.block_codec.max_tx_bytes.get(), DEFAULT_BLOCK_CODEC_MAX_TX_BYTES);
        assert_eq!(config.simplex.replay_buffer_bytes.get(), DEFAULT_SIMPLEX_REPLAY_BUFFER_BYTES);
        assert_eq!(config.simplex.write_buffer_bytes.get(), DEFAULT_SIMPLEX_WRITE_BUFFER_BYTES);
        assert_eq!(config.simplex.leader_timeout_secs.get(), DEFAULT_SIMPLEX_LEADER_TIMEOUT_SECS);
        assert_eq!(
            config.simplex.certification_timeout_secs.get(),
            DEFAULT_SIMPLEX_CERTIFICATION_TIMEOUT_SECS
        );
        assert_eq!(config.simplex.timeout_retry_secs.get(), DEFAULT_SIMPLEX_TIMEOUT_RETRY_SECS);
        assert_eq!(config.simplex.fetch_timeout_secs.get(), DEFAULT_SIMPLEX_FETCH_TIMEOUT_SECS);
        assert_eq!(
            config.simplex.activity_timeout_views.get(),
            DEFAULT_SIMPLEX_ACTIVITY_TIMEOUT_VIEWS
        );
        assert_eq!(config.simplex.skip_timeout_views.get(), DEFAULT_SIMPLEX_SKIP_TIMEOUT_VIEWS);
        assert_eq!(config.simplex.fetch_concurrent.get(), DEFAULT_SIMPLEX_FETCH_CONCURRENT);
    }

    #[test]
    fn default_threshold_constant() {
        assert_eq!(DEFAULT_THRESHOLD, 2);
        assert_eq!(default_threshold(), DEFAULT_THRESHOLD);
    }

    #[test]
    fn serde_json_roundtrip() {
        let pk_bytes = create_valid_public_key_bytes();
        let config = ConsensusConfig {
            validator_key: Some(PathBuf::from("/path/to/key")),
            threshold: 3,
            participants: vec![pk_bytes],
            ..Default::default()
        };
        let serialized = serde_json::to_string(&config).expect("serialize");
        let deserialized: ConsensusConfig = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(config, deserialized);
    }

    #[test]
    fn serde_toml_roundtrip() {
        let config =
            ConsensusConfig { validator_key: Some("/path/to/key".into()), ..Default::default() };
        let serialized = toml::to_string(&config).expect("serialize toml");
        let deserialized: ConsensusConfig = toml::from_str(&serialized).expect("deserialize toml");
        assert_eq!(config, deserialized);
    }

    #[test]
    fn serde_defaults_applied() {
        let config: ConsensusConfig = serde_json::from_str("{}").expect("deserialize");
        assert!(config.validator_key.is_none());
        assert_eq!(config.threshold, DEFAULT_THRESHOLD);
        assert!(config.participants.is_empty());
        assert_eq!(config.block_codec, ConsensusBlockCodecConfig::default());
        assert_eq!(config.simplex, ConsensusSimplexConfig::default());
    }

    #[test]
    fn serde_partial_threshold() {
        let config: ConsensusConfig =
            serde_json::from_str(r#"{"threshold": 7}"#).expect("deserialize");
        assert_eq!(config.threshold, 7);
        assert!(config.validator_key.is_none());
        assert!(config.participants.is_empty());
        assert_eq!(config.block_codec, ConsensusBlockCodecConfig::default());
        assert_eq!(config.simplex, ConsensusSimplexConfig::default());
    }

    #[test]
    fn serde_partial_validator_key() {
        let config: ConsensusConfig =
            serde_json::from_str(r#"{"validator_key": "/etc/key"}"#).expect("deserialize");
        assert_eq!(config.validator_key, Some(PathBuf::from("/etc/key")));
        assert_eq!(config.threshold, DEFAULT_THRESHOLD);
        assert_eq!(config.block_codec, ConsensusBlockCodecConfig::default());
        assert_eq!(config.simplex, ConsensusSimplexConfig::default());
    }

    #[test]
    fn serde_partial_block_codec_defaults() {
        let config: ConsensusConfig =
            serde_json::from_str(r#"{"block_codec": {"max_txs": 2048}}"#).expect("deserialize");

        assert_eq!(config.block_codec.max_txs.get(), 2048);
        assert_eq!(config.block_codec.max_tx_bytes.get(), DEFAULT_BLOCK_CODEC_MAX_TX_BYTES);
        assert_eq!(config.simplex, ConsensusSimplexConfig::default());
    }

    #[test]
    fn serde_partial_simplex_defaults() {
        let config: ConsensusConfig = serde_json::from_str(
            r#"{
                "simplex": {
                    "leader_timeout_secs": 7,
                    "fetch_concurrent": 3,
                    "activity_timeout_views": 30
                }
            }"#,
        )
        .expect("deserialize");

        assert_eq!(config.simplex.leader_timeout_secs.get(), 7);
        assert_eq!(config.simplex.fetch_concurrent.get(), 3);
        assert_eq!(config.simplex.activity_timeout_views.get(), 30);
        assert_eq!(
            config.simplex.certification_timeout_secs.get(),
            DEFAULT_SIMPLEX_CERTIFICATION_TIMEOUT_SECS
        );
        assert_eq!(config.block_codec, ConsensusBlockCodecConfig::default());
    }

    #[test]
    fn serde_rejects_zero_nonzero_fields() {
        let block_codec =
            serde_json::from_str::<ConsensusConfig>(r#"{"block_codec": {"max_tx_bytes": 0}}"#);
        assert!(block_codec.is_err());

        let simplex =
            serde_json::from_str::<ConsensusConfig>(r#"{"simplex": {"fetch_concurrent": 0}}"#);
        assert!(simplex.is_err());
    }

    #[test]
    fn deserialize_participants_with_0x_prefix() {
        let pk_bytes = create_valid_public_key_bytes();
        let hex_with_prefix = format!("0x{}", hex::encode(&pk_bytes));
        let json = format!(r#"{{"participants": ["{}"]}}"#, hex_with_prefix);

        let config: ConsensusConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config.participants.len(), 1);
        assert_eq!(config.participants[0], pk_bytes);
    }

    #[test]
    fn deserialize_participants_without_prefix() {
        let pk_bytes = create_valid_public_key_bytes();
        let hex_without_prefix = hex::encode(&pk_bytes);
        let json = format!(r#"{{"participants": ["{}"]}}"#, hex_without_prefix);

        let config: ConsensusConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config.participants.len(), 1);
        assert_eq!(config.participants[0], pk_bytes);
    }

    #[test]
    fn build_validator_set_empty() {
        let config = ConsensusConfig::default();
        let result = config.build_validator_set().expect("build empty set");
        assert!(result.is_empty());
    }

    #[test]
    fn build_validator_set_single_key() {
        let pk_bytes = create_valid_public_key_bytes();
        let config = ConsensusConfig { participants: vec![pk_bytes], ..Default::default() };
        let result = config.build_validator_set().expect("build validator set");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn build_validator_set_multiple_keys() {
        let keys: Vec<_> = (1..=3u8)
            .map(|i| {
                let pk = ed25519::PrivateKey::from(ed25519_consensus::SigningKey::from([i; 32]));
                let mut bytes = Vec::new();
                pk.public_key().write(&mut bytes);
                bytes
            })
            .collect();

        let config = ConsensusConfig { participants: keys, threshold: 2, ..Default::default() };

        let result = config.build_validator_set().expect("build validator set");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn build_validator_set_invalid_length() {
        let config = ConsensusConfig { participants: vec![vec![0u8; 16]], ..Default::default() };
        let result = config.build_validator_set();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::InvalidParticipantKeyLength(16)));
    }

    #[test]
    fn participants_hex_serialization() {
        let pk_bytes = create_valid_public_key_bytes();
        let expected_hex = hex::encode(&pk_bytes);
        let config = ConsensusConfig { participants: vec![pk_bytes], ..Default::default() };

        let serialized = serde_json::to_string(&config).expect("serialize");
        assert!(serialized.contains(&expected_hex));
    }

    #[test]
    fn consensus_config_clone_and_eq() {
        let pk_bytes = create_valid_public_key_bytes();
        let config = ConsensusConfig {
            validator_key: Some(PathBuf::from("/custom/path")),
            threshold: 10,
            participants: vec![pk_bytes],
            ..Default::default()
        };
        assert_eq!(config, config.clone());
        assert_ne!(config, ConsensusConfig::default());
    }

    #[test]
    fn consensus_config_debug() {
        let config = ConsensusConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("ConsensusConfig"));
        assert!(debug.contains("threshold"));
    }
}
