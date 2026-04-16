//! Native certificate loading from well-known OS paths.
//!
//! Replaces the `rustls-native-certs` + `openssl-probe` crate chain by directly
//! probing environment variables and well-known Linux certificate paths.

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::CertificateDer;
use std::path::{Path, PathBuf};

/// Well-known certificate bundle file paths (checked in order, first existing wins).
const CERT_FILE_PATHS: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt", // Debian/Ubuntu
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // RHEL/CentOS 7+
    "/etc/pki/tls/certs/ca-bundle.crt",   // RHEL/CentOS 6
    "/etc/ssl/ca-bundle.pem",             // openSUSE
    "/etc/pki/tls/cacert.pem",
    "/etc/ssl/cert.pem",                      // Alpine, common fallback
    "/opt/etc/ssl/certs/ca-certificates.crt", // Entware/OpenWrt
];

/// Well-known certificate directory paths (all existing directories are scanned).
const CERT_DIR_PATHS: &[&str] = &["/etc/ssl/certs", "/etc/pki/tls/certs"];

/// Result of loading native certificates.
pub(crate) struct CertificateResult {
    pub certs: Vec<CertificateDer<'static>>,
    pub errors: Vec<String>,
}

/// Load system CA certificates.
///
/// Priority:
/// 1. `SSL_CERT_FILE` / `SSL_CERT_DIR` environment variables (exclusive if set).
/// 2. Well-known Linux certificate file and directory paths.
pub(crate) fn load_native_certs() -> CertificateResult {
    let env_file = std::env::var("SSL_CERT_FILE").ok().map(PathBuf::from);
    let env_dir = std::env::var("SSL_CERT_DIR").ok().map(PathBuf::from);

    // If environment variables are set, use only those
    if env_file.is_some() || env_dir.is_some() {
        return load_from_paths(env_file.as_deref(), env_dir.as_deref());
    }

    // Probe well-known paths
    let cert_file = CERT_FILE_PATHS.iter().map(Path::new).find(|p| p.exists());

    let cert_dirs: Vec<&Path> = CERT_DIR_PATHS
        .iter()
        .map(Path::new)
        .filter(|p| p.is_dir())
        .collect();

    let mut result = CertificateResult {
        certs: Vec::new(),
        errors: Vec::new(),
    };

    if let Some(file) = cert_file {
        load_pem_file(file, &mut result);
    }

    for dir in cert_dirs {
        load_pem_dir(dir, &mut result);
    }

    // Deduplicate
    result.certs.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    result.certs.dedup();

    result
}

/// Load from explicit file and/or directory paths (used for env var overrides).
fn load_from_paths(file: Option<&Path>, dir: Option<&Path>) -> CertificateResult {
    let mut result = CertificateResult {
        certs: Vec::new(),
        errors: Vec::new(),
    };

    if let Some(file) = file {
        load_pem_file(file, &mut result);
    }
    if let Some(dir) = dir {
        load_pem_dir(dir, &mut result);
    }

    result.certs.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    result.certs.dedup();

    result
}

/// Load PEM certificates from a single file.
fn load_pem_file(path: &Path, result: &mut CertificateResult) {
    match CertificateDer::pem_file_iter(path) {
        Ok(iter) => {
            for cert in iter {
                match cert {
                    Ok(c) => result.certs.push(c),
                    Err(e) => result.errors.push(format!("{}: {e}", path.display())),
                }
            }
        }
        Err(e) => result.errors.push(format!("{}: {e}", path.display())),
    }
}

/// Load PEM certificates from all regular files in a directory.
fn load_pem_dir(dir: &Path, result: &mut CertificateResult) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            result.errors.push(format!("{}: {e}", dir.display()));
            return;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                result.errors.push(format!("{}: {e}", dir.display()));
                continue;
            }
        };
        let path = entry.path();
        // Follow symlinks but skip entries that don't resolve to regular files
        match std::fs::metadata(&path) {
            Ok(meta) if meta.is_file() => load_pem_file(&path, result),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A self-signed test certificate in PEM format.
    const TEST_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBdjCCAR2gAwIBAgIUY5f0JXKWFNRm8MHjRLnYGe/I0jYwCgYIKoZIzj0EAwIw
EjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNDAxMDEwMDAwMDBaFw0zNDAxMDEwMDAw
MDBaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNC
AARGJzMiJLNr3s8QxZ0vdH6IH/mmGHJDdKjYXSqq1WhVDwfXYXRGH/4TjRqAzq9z
Sx0WBQgRx+4m9E2JXkS2mPAjo1MwUTAdBgNVHQ4EFgQUgpV4xMOm6YFqGTCgcXs9
wQ5eaHIwHwYDVR0jBBgwFoAUgpV4xMOm6YFqGTCgcXs9wQ5eaHIwDwYDVR0TAQH/
BAUwAwEB/zAKBggqhkjOPQQDAgNHADBEAiAm4G38MRa/dn7vpFwR+gZTb5GXPsC5
BQmVjIJ+bw1bPAIgM3uBzsr+1TgfXTa7M7dDJWsJb5cTm3w+07iAcWNoNg=
-----END CERTIFICATE-----
";

    /// Create a unique temp directory and return its path.
    /// Caller is responsible for cleanup via `std::fs::remove_dir_all`.
    fn make_temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pg_walstream_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_load_pem_file_valid() {
        let dir = make_temp_dir("pem_file_valid");
        let file_path = dir.join("test.pem");
        let mut f = std::fs::File::create(&file_path).unwrap();
        f.write_all(TEST_PEM.as_bytes()).unwrap();

        let mut result = CertificateResult {
            certs: Vec::new(),
            errors: Vec::new(),
        };
        load_pem_file(&file_path, &mut result);

        assert_eq!(result.certs.len(), 1, "Should load 1 certificate");
        assert!(result.errors.is_empty(), "Should have no errors");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_pem_file_nonexistent() {
        let mut result = CertificateResult {
            certs: Vec::new(),
            errors: Vec::new(),
        };
        load_pem_file(
            Path::new("/tmp/nonexistent-cert-file-12345.pem"),
            &mut result,
        );

        assert!(result.certs.is_empty());
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_load_pem_dir_valid() {
        let dir = make_temp_dir("pem_dir_valid");

        // Write two cert files
        for name in &["a.pem", "b.pem"] {
            let path = dir.join(name);
            std::fs::write(&path, TEST_PEM).unwrap();
        }

        let mut result = CertificateResult {
            certs: Vec::new(),
            errors: Vec::new(),
        };
        load_pem_dir(&dir, &mut result);

        // Both files have the same cert, so we get 2 before dedup
        assert_eq!(result.certs.len(), 2);
        assert!(result.errors.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_pem_dir_nonexistent() {
        let mut result = CertificateResult {
            certs: Vec::new(),
            errors: Vec::new(),
        };
        load_pem_dir(Path::new("/tmp/nonexistent-cert-dir-12345"), &mut result);

        assert!(result.certs.is_empty());
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_dedup_in_load_from_paths() {
        let dir = make_temp_dir("dedup");
        let file_path = dir.join("ca.pem");
        std::fs::write(&file_path, TEST_PEM).unwrap();

        // Load from both file and directory — same cert should be deduped
        let result = load_from_paths(Some(&file_path), Some(&dir));

        assert_eq!(
            result.certs.len(),
            1,
            "Duplicate certs should be deduplicated"
        );
        assert!(result.errors.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cert_file_paths_are_absolute() {
        for path in CERT_FILE_PATHS {
            assert!(
                path.starts_with('/'),
                "Cert file path should be absolute: {path}"
            );
        }
    }

    #[test]
    fn test_cert_dir_paths_are_absolute() {
        for path in CERT_DIR_PATHS {
            assert!(
                path.starts_with('/'),
                "Cert dir path should be absolute: {path}"
            );
        }
    }
}
