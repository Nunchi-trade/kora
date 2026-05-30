//! Append-only log for equivocation proofs.
//!
//! When a validator is caught equivocating (signing conflicting messages in the
//! same consensus round), the proof is persisted to an append-only file so that
//! it survives node restarts and can be used for future slashing or forensic
//! analysis.
//!
//! The log format is newline-delimited text:
//!
//! ```text
//! <timestamp_secs>,<type>,<signer_hex>,<view>
//! ```

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::warn;

/// Default filename for the equivocation log within the data directory.
const EQUIVOCATION_LOG_FILENAME: &str = "equivocation-proofs.log";

/// Append-only log tracking detected equivocation events.
///
/// Each entry records the wall-clock timestamp, equivocation type, signer
/// identity, and consensus view.  The log is safe to truncate or delete --
/// the worst case is that historical evidence is lost.
pub struct EquivocationLog {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl std::fmt::Debug for EquivocationLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EquivocationLog").field("path", &self.path).finish()
    }
}

impl EquivocationLog {
    /// Open or create the equivocation log at `dir/equivocation-proofs.log`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or the directory
    /// cannot be created.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(EQUIVOCATION_LOG_FILENAME);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { writer: Mutex::new(BufWriter::new(file)), path })
    }

    /// Record an equivocation event.
    ///
    /// `kind` is a short string like `"conflicting_notarize"`,
    /// `"conflicting_finalize"`, or `"nullify_finalize"`.
    pub fn record(&self, kind: &str, signer: &str, view: u64) {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);

        let mut writer = match self.writer.lock() {
            Ok(w) => w,
            Err(e) => {
                warn!(error = %e, "equivocation log mutex poisoned; skipping write");
                return;
            }
        };

        if let Err(e) = writeln!(writer, "{timestamp},{kind},{signer},{view}") {
            warn!(
                kind,
                signer,
                view,
                error = %e,
                "failed to write equivocation log entry"
            );
            return;
        }

        if let Err(e) = writer.flush() {
            warn!(error = %e, "failed to flush equivocation log");
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
        let log = EquivocationLog::open(dir.path()).expect("open log");

        log.record("conflicting_notarize", "0xabc123", 42);
        log.record("nullify_finalize", "0xdef456", 99);

        let mut contents = String::new();
        File::open(dir.path().join(EQUIVOCATION_LOG_FILENAME))
            .expect("open log file")
            .read_to_string(&mut contents)
            .expect("read log file");

        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("conflicting_notarize"));
        assert!(lines[0].contains("0xabc123"));
        assert!(lines[0].contains(",42"));
        assert!(lines[1].contains("nullify_finalize"));
    }
}
