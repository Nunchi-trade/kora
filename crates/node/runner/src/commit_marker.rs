//! Commit digest marker file for crash-recovery validation.
//!
//! After each successful QMDB persist, the digest of the committed block is
//! written to a small marker file (`last_committed_digest`). On startup the
//! recovery procedure reads this marker and compares it against the archive
//! head to detect whether QMDB may be behind or inconsistent.
//!
//! The write uses an atomic rename pattern (write to a temporary file, then
//! rename) so a crash mid-write never produces a corrupt marker.

use std::{
    io::Write as _,
    path::{Path, PathBuf},
};

use commonware_cryptography::sha256;
use kora_domain::ConsensusDigest;
use tracing::{debug, warn};

/// Name of the marker file within the data directory.
const MARKER_FILENAME: &str = "last_committed_digest";

/// Name of the temporary file used during atomic writes.
const MARKER_TMP_FILENAME: &str = "last_committed_digest.tmp";

/// Resolve the marker file path for a given data directory.
pub fn marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(MARKER_FILENAME)
}

/// Write the committed block's digest to the marker file atomically.
///
/// The digest is written as 64 lowercase hex characters followed by a newline.
/// The write goes to a temporary file first, which is then renamed into place
/// so that a crash mid-write never leaves a corrupt marker.
pub fn write_commit_marker(data_dir: &Path, digest: &ConsensusDigest) -> std::io::Result<()> {
    let tmp_path = data_dir.join(MARKER_TMP_FILENAME);
    let final_path = marker_path(data_dir);

    // Ensure the directory exists.
    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write to temp file.
    let hex = hex::encode(digest.as_ref());
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(hex.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }

    // Atomic rename.
    std::fs::rename(&tmp_path, &final_path)?;

    debug!(digest = %hex, path = %final_path.display(), "wrote commit marker");
    Ok(())
}

/// Read the last committed digest from the marker file.
///
/// Returns `None` if the marker file does not exist (fresh node or pre-fix
/// node). Returns `Some(digest)` if the file exists and contains a valid
/// 64-character hex string. Logs a warning and returns `None` if the file
/// exists but is malformed.
pub fn read_commit_marker(data_dir: &Path) -> Option<ConsensusDigest> {
    let path = marker_path(data_dir);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(
                error = %e,
                path = %path.display(),
                "failed to read commit marker file"
            );
            return None;
        }
    };

    let hex_str = content.trim();
    if hex_str.len() != 64 {
        warn!(
            len = hex_str.len(),
            path = %path.display(),
            "commit marker file has unexpected length (expected 64 hex chars)"
        );
        return None;
    }

    let mut bytes = [0u8; 32];
    match hex::decode_to_slice(hex_str, &mut bytes) {
        Ok(()) => Some(sha256::Digest(bytes)),
        Err(e) => {
            warn!(
                error = %e,
                path = %path.display(),
                "commit marker file contains invalid hex"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_write_read() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let digest = sha256::Digest([0xab; 32]);

        write_commit_marker(dir.path(), &digest).expect("write");
        let read_back = read_commit_marker(dir.path());

        assert_eq!(read_back, Some(digest));
    }

    #[test]
    fn missing_marker_returns_none() {
        let dir = tempfile::tempdir().expect("create temp dir");
        assert_eq!(read_commit_marker(dir.path()), None);
    }

    #[test]
    fn corrupt_marker_returns_none() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = marker_path(dir.path());
        std::fs::write(&path, "not-valid-hex\n").expect("write corrupt");

        assert_eq!(read_commit_marker(dir.path()), None);
    }

    #[test]
    fn overwrite_marker() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let digest_a = sha256::Digest([0x11; 32]);
        let digest_b = sha256::Digest([0x22; 32]);

        write_commit_marker(dir.path(), &digest_a).expect("write a");
        assert_eq!(read_commit_marker(dir.path()), Some(digest_a));

        write_commit_marker(dir.path(), &digest_b).expect("write b");
        assert_eq!(read_commit_marker(dir.path()), Some(digest_b));
    }
}
