use std::{fmt, path::Path};

use commonware_utils::{Faults, N3f1};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::{DkgError, secret_file::write_secret_file};

/// Output of a successful DKG ceremony containing the group key, shares, and participant info.
pub struct DkgOutput {
    /// The aggregated group public key derived from all participants' contributions.
    pub group_public_key: Vec<u8>,
    /// Coefficients of the public polynomial used for share verification.
    pub public_polynomial: Vec<u8>,
    /// Quorum size (minimum active validators for consensus), computed from N3f1.
    ///
    /// This is always `n - (n-1)/3` where n is the participant count. The value
    /// stored in output.json may be stale if it was generated before this fix;
    /// on load, we recompute it from the participant count.
    pub threshold: u32,
    /// Total number of participants in the DKG ceremony.
    pub participants: usize,
    /// This participant's index in the DKG ceremony (0-indexed).
    pub share_index: u32,
    /// This participant's secret share of the distributed key.
    pub share_secret: Vec<u8>,
    /// Public keys of all participants in the DKG ceremony.
    pub participant_keys: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct OutputJson {
    group_public_key: String,
    public_polynomial: String,
    /// Persisted as "threshold" in JSON for backward compatibility, but the
    /// authoritative value is always recomputed from `participants` via N3f1.
    threshold: u32,
    participants: usize,
    #[serde(default)]
    participant_keys: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct ShareJson {
    index: u32,
    secret: String,
}

impl fmt::Debug for DkgOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DkgOutput")
            .field("group_public_key", &self.group_public_key)
            .field("public_polynomial", &self.public_polynomial)
            .field("threshold", &self.threshold)
            .field("participants", &self.participants)
            .field("share_index", &self.share_index)
            .field("share_secret", &"<redacted>")
            .field("participant_keys", &self.participant_keys)
            .finish()
    }
}

impl Drop for DkgOutput {
    fn drop(&mut self) {
        self.share_secret.zeroize();
    }
}

impl Drop for ShareJson {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

impl DkgOutput {
    /// Persists the DKG output to `output.json` and the secret share to `share.key` in `data_dir`.
    pub fn save(&self, data_dir: &Path) -> Result<(), DkgError> {
        let output_json = OutputJson {
            group_public_key: hex::encode(&self.group_public_key),
            public_polynomial: hex::encode(&self.public_polynomial),
            threshold: self.threshold,
            participants: self.participants,
            participant_keys: self.participant_keys.iter().map(hex::encode).collect(),
        };

        let output_path = data_dir.join("output.json");
        write_secret_file(&output_path, serde_json::to_string_pretty(&output_json)?.as_bytes())?;

        let share_json =
            ShareJson { index: self.share_index, secret: hex::encode(&self.share_secret) };

        let share_path = data_dir.join("share.key");
        let mut share_content = serde_json::to_string_pretty(&share_json)?;
        write_secret_file(&share_path, share_content.as_bytes())?;
        share_content.zeroize();

        Ok(())
    }

    /// Loads a DKG output from `output.json` and `share.key` in `data_dir`.
    ///
    /// The `threshold` field is always recomputed from `participants` using N3f1
    /// to ensure correctness regardless of what value was persisted in the JSON.
    pub fn load(data_dir: &Path) -> Result<Self, DkgError> {
        let output_path = data_dir.join("output.json");
        let output_str = std::fs::read_to_string(&output_path)?;
        let output: OutputJson = serde_json::from_str(&output_str)
            .map_err(|e| DkgError::Serialization(e.to_string()))?;

        let share_path = data_dir.join("share.key");
        let mut share_str = std::fs::read_to_string(&share_path)?;
        let share: ShareJson =
            serde_json::from_str(&share_str).map_err(|e| DkgError::Serialization(e.to_string()))?;
        share_str.zeroize();

        let participant_keys = output
            .participant_keys
            .iter()
            .map(|k| hex::decode(k).map_err(|e| DkgError::Serialization(e.to_string())))
            .collect::<Result<Vec<_>, _>>()?;
        let share_index = share.index;
        let share_secret =
            hex::decode(&share.secret).map_err(|e| DkgError::Serialization(e.to_string()))?;
        drop(share);

        // Always compute the correct quorum from N3f1 rather than trusting
        // the persisted threshold value, which may be wrong in old output files.
        let correct_threshold = N3f1::quorum(output.participants);

        Ok(Self {
            group_public_key: hex::decode(&output.group_public_key)
                .map_err(|e| DkgError::Serialization(e.to_string()))?,
            public_polynomial: hex::decode(&output.public_polynomial)
                .map_err(|e| DkgError::Serialization(e.to_string()))?,
            threshold: correct_threshold,
            participants: output.participants,
            share_index,
            share_secret,
            participant_keys,
        })
    }

    /// Returns `true` if both `output.json` and `share.key` exist in `data_dir`.
    pub fn exists(data_dir: &Path) -> bool {
        data_dir.join("output.json").exists() && data_dir.join("share.key").exists()
    }
}

impl From<serde_json::Error> for DkgError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_share_secret() {
        let output = DkgOutput {
            group_public_key: vec![1, 2, 3],
            public_polynomial: vec![4, 5, 6],
            threshold: 2,
            participants: 3,
            share_index: 1,
            share_secret: vec![222, 173, 190, 239],
            participant_keys: vec![vec![7, 8, 9]],
        };

        let debug = format!("{output:?}");

        assert!(debug.contains("share_secret: \"<redacted>\""));
        assert!(!debug.contains("222"));
        assert!(!debug.contains("173"));
        assert!(!debug.contains("190"));
        assert!(!debug.contains("239"));
    }

    #[cfg(unix)]
    #[test]
    fn save_restricts_existing_secret_file_permissions() {
        use std::{
            fs,
            os::unix::fs::PermissionsExt as _,
            time::{SystemTime, UNIX_EPOCH},
        };

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("kora-dkg-output-{}-{nonce}", std::process::id()));
        fs::create_dir(&dir).expect("create temp dir");

        for file in ["output.json", "share.key"] {
            let path = dir.join(file);
            fs::write(&path, b"old").expect("write permissive file");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
                .expect("set permissive mode");
        }

        let output = DkgOutput {
            group_public_key: vec![1, 2, 3],
            public_polynomial: vec![4, 5, 6],
            threshold: 2,
            participants: 3,
            share_index: 1,
            share_secret: vec![10, 11, 12],
            participant_keys: vec![vec![7, 8, 9]],
        };

        output.save(&dir).expect("save dkg output");

        for file in ["output.json", "share.key"] {
            let mode = fs::metadata(dir.join(file)).expect("stat secret file").permissions().mode()
                & 0o777;
            assert_eq!(mode, 0o600, "{file} should be mode 0600");
        }

        fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
