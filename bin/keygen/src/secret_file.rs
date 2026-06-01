use std::{fs, io::Write as _, path::Path};

use eyre::{Result, WrapErr as _};

/// Write secret-bearing data with restrictive permissions.
pub(crate) fn write_secret_file(path: &Path, data: &[u8]) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }

    let mut file = options
        .open(path)
        .wrap_err_with(|| format!("Failed to create secret file {}", path.display()))?;
    restrict_file_permissions(&file)
        .wrap_err_with(|| format!("Failed to restrict secret file {}", path.display()))?;
    file.write_all(data)
        .wrap_err_with(|| format!("Failed to write secret file {}", path.display()))?;
    restrict_file_permissions(&file)
        .wrap_err_with(|| format!("Failed to restrict secret file {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn restrict_file_permissions(file: &fs::File) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;
    file.set_permissions(fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_file_permissions(_file: &fs::File) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        os::unix::fs::PermissionsExt as _,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("keygen-secret-file-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create temp dir");
        path
    }

    #[test]
    fn write_secret_file_restricts_existing_file_permissions() {
        let dir = temp_dir();
        let path = dir.join("validator.key");
        fs::write(&path, b"old secret").expect("write permissive file");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("set permissive mode");

        write_secret_file(&path, b"new secret").expect("write secret file");

        let mode = fs::metadata(&path).expect("stat secret file").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(fs::read(&path).expect("read secret file"), b"new secret");

        fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
