//! Append-only GC log for selfdestructed contract addresses.
//!
//! When a contract selfdestructs, its account entry in QMDB is deleted and the
//! generation counter is incremented so new storage writes use a fresh
//! namespace. However, the old storage entries (keyed by the previous
//! generation) remain on disk indefinitely because Commonware does not yet
//! support prefix-based key scanning or bulk deletion.
//!
//! This module records every selfdestructed address together with the block
//! height at which it was finalized. A future garbage collector can read this
//! log and reclaim the orphaned storage entries once the upstream storage layer
//! adds the necessary primitives.
//!
//! The log format is newline-delimited text:
//!
//! ```text
//! <block_height>,<hex_address>
//! ```
//!
//! This format is intentionally simple and human-readable to aid debugging and
//! operational tooling. Each line is flushed immediately so the log survives
//! crashes.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
    sync::Mutex,
};

use alloy_primitives::Address;
use tracing::{info, warn};

/// Default filename for the GC log within the data directory.
const GC_LOG_FILENAME: &str = "selfdestruct-gc.log";

/// Append-only log tracking selfdestructed addresses for future garbage
/// collection.
///
/// Each entry records the finalized block height and the selfdestructed
/// contract address. The log is safe to truncate or delete -- the worst case
/// is that some orphaned storage is never reclaimed.
pub struct SelfdestructGcLog {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl std::fmt::Debug for SelfdestructGcLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelfdestructGcLog").field("path", &self.path).finish()
    }
}

impl SelfdestructGcLog {
    /// Open or create the GC log at `dir/selfdestruct-gc.log`.
    ///
    /// The file is opened in append mode. If the directory does not exist it
    /// is created.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or the directory
    /// cannot be created.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(GC_LOG_FILENAME);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { writer: Mutex::new(BufWriter::new(file)), path })
    }

    /// Record one or more selfdestructed addresses from a finalized block.
    ///
    /// Each address is written as a separate line. The buffer is flushed after
    /// all addresses in the batch are written so that the log is durable even
    /// if the process crashes shortly after.
    pub fn record(&self, block_height: u64, addresses: &[Address]) {
        if addresses.is_empty() {
            return;
        }

        let mut writer = match self.writer.lock() {
            Ok(w) => w,
            Err(e) => {
                warn!(error = %e, "GC log mutex poisoned; skipping write");
                return;
            }
        };

        for address in addresses {
            if let Err(e) = writeln!(writer, "{},{}", block_height, address) {
                warn!(
                    block_height,
                    address = ?address,
                    error = %e,
                    "failed to write selfdestruct GC entry"
                );
                return;
            }
        }

        if let Err(e) = writer.flush() {
            warn!(block_height, error = %e, "failed to flush selfdestruct GC log");
        } else {
            info!(
                block_height,
                count = addresses.len(),
                path = %self.path.display(),
                "recorded selfdestructed addresses for GC"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;

    #[test]
    fn record_writes_entries_and_flushes() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let gc_log = SelfdestructGcLog::open(dir.path()).expect("open gc log");

        let addr1 = Address::repeat_byte(0x11);
        let addr2 = Address::repeat_byte(0x22);

        gc_log.record(42, &[addr1, addr2]);
        gc_log.record(43, &[addr1]);

        let mut contents = String::new();
        File::open(dir.path().join(GC_LOG_FILENAME))
            .expect("open log file")
            .read_to_string(&mut contents)
            .expect("read log file");

        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("42,0x"), "expected 0x prefix: {}", lines[0]);
        assert!(lines[0].to_lowercase().contains("1111111111111111111111111111111111111111"));
        assert!(lines[1].starts_with("42,0x"), "expected 0x prefix: {}", lines[1]);
        assert!(lines[1].to_lowercase().contains("2222222222222222222222222222222222222222"));
        assert!(lines[2].starts_with("43,0x"), "expected 0x prefix: {}", lines[2]);
    }

    #[test]
    fn record_empty_is_noop() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let gc_log = SelfdestructGcLog::open(dir.path()).expect("open gc log");

        gc_log.record(1, &[]);

        let metadata = std::fs::metadata(dir.path().join(GC_LOG_FILENAME)).expect("metadata");
        assert_eq!(metadata.len(), 0);
    }

    #[test]
    fn open_creates_directory() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let nested = dir.path().join("deeply").join("nested");
        let gc_log = SelfdestructGcLog::open(&nested).expect("open gc log");

        gc_log.record(1, &[Address::ZERO]);

        assert!(nested.join(GC_LOG_FILENAME).exists());
    }
}
