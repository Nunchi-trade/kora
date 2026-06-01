use std::{io::Write as _, path::Path};

use commonware_utils::{Faults, N3f1};
use serde::{Deserialize, Serialize};

use crate::DkgError;

/// Output of a successful DKG ceremony containing the group key, shares, and participant info.
#[derive(Debug, Clone)]
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

        // Write output.json with explicit 0640 permissions for defense-in-depth.
        // The file contains only public data (group key, polynomial, participant keys),
        // but we restrict access to be consistent with the DKG data directory.
        let output_path = data_dir.join("output.json");
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o640)
                .open(&output_path)?;
            f.write_all(serde_json::to_string_pretty(&output_json)?.as_bytes())?;
        }

        let share_json =
            ShareJson { index: self.share_index, secret: hex::encode(&self.share_secret) };

        let share_path = data_dir.join("share.key");
        write_secret_file(&share_path, serde_json::to_string_pretty(&share_json)?.as_bytes())?;

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
        let share_str = std::fs::read_to_string(&share_path)?;
        let share: ShareJson =
            serde_json::from_str(&share_str).map_err(|e| DkgError::Serialization(e.to_string()))?;

        let participant_keys = output
            .participant_keys
            .iter()
            .map(|k| hex::decode(k).map_err(|e| DkgError::Serialization(e.to_string())))
            .collect::<Result<Vec<_>, _>>()?;

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
            share_index: share.index,
            share_secret: hex::decode(&share.secret)
                .map_err(|e| DkgError::Serialization(e.to_string()))?,
            participant_keys,
        })
    }

    /// Returns `true` if both `output.json` and `share.key` exist in `data_dir`.
    pub fn exists(data_dir: &Path) -> bool {
        data_dir.join("output.json").exists() && data_dir.join("share.key").exists()
    }
}

/// Write `data` to `path` with mode `0600` so key material is never world-readable.
fn write_secret_file(path: &Path, data: &[u8]) -> Result<(), DkgError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(data)?;
    Ok(())
}

impl From<serde_json::Error> for DkgError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}
