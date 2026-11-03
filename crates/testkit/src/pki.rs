//! Development PKI for TLS test clusters (ADR-0017): one CA, a SPIFFE
//! certificate per process, files under the cluster's temporary directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arca_lake_protocol::pki::Ca;
use arca_lake_protocol::tls::{Role, TlsContext, TlsFiles};

/// Trust domain of test clusters.
pub const TRUST_DOMAIN: &str = "test.arca-lake.invalid";

const VALID: Duration = Duration::from_hours(24);

/// A CA and the directory its certificates are written to.
pub struct TestPki {
    /// The CA.
    pub ca: Ca,
    dir: PathBuf,
    serial: AtomicU64,
}

impl std::fmt::Debug for TestPki {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestPki")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl TestPki {
    /// A new CA writing under `dir`.
    ///
    /// # Panics
    ///
    /// On key generation or I/O failures.
    #[must_use]
    pub fn new(dir: &Path) -> Arc<Self> {
        std::fs::create_dir_all(dir).unwrap();
        let ca = Ca::new("arca-lake test CA", VALID).unwrap();
        std::fs::write(dir.join("ca.pem"), &ca.cert_pem).unwrap();
        Arc::new(Self {
            ca,
            dir: dir.to_path_buf(),
            serial: AtomicU64::new(1),
        })
    }

    /// Write a certificate for `role`/`name` into `sub` and return the files.
    ///
    /// # Panics
    ///
    /// On signing or I/O failures.
    pub fn write(&self, sub: &Path, role: Role, name: &str) -> TlsFiles {
        std::fs::create_dir_all(sub).unwrap();
        let issued = self
            .ca
            .issue(
                TRUST_DOMAIN,
                role,
                name,
                self.serial.fetch_add(1, Ordering::Relaxed),
                VALID,
            )
            .unwrap();
        let put = |name: &str, text: &str| {
            let p = sub.join(name);
            let tmp = p.with_extension("tmp");
            std::fs::write(&tmp, text).unwrap();
            std::fs::rename(&tmp, &p).unwrap();
        };
        put("cert.pem", &issued.cert_pem);
        put("key.pem", &issued.key_pem);
        put("ca.pem", &self.ca.cert_pem);
        TlsFiles {
            ca: sub.join("ca.pem"),
            cert: sub.join("cert.pem"),
            key: sub.join("key.pem"),
            crl: None,
            trust_domain: TRUST_DOMAIN.into(),
        }
    }

    /// Directory of the files [`Self::ctx`] writes for `role`/`name`.
    #[must_use]
    pub fn dir_of(&self, role: Role, name: &str) -> PathBuf {
        self.dir.join(format!("{}-{name}", role.as_str()))
    }

    /// A context for a process of `role` named `name`, files under the
    /// PKI directory.
    ///
    /// # Panics
    ///
    /// On failures.
    #[must_use]
    pub fn ctx(&self, role: Role, name: &str) -> Arc<TlsContext> {
        TlsContext::load(self.write(&self.dir_of(role, name), role, name), role).unwrap()
    }

    /// A node context before the node has an id (`pending`), stored under
    /// the node's directory; [`Self::name_node`] re-issues it.
    ///
    /// # Panics
    ///
    /// On failures.
    #[must_use]
    pub fn pending_node(&self, node_dir: &Path) -> Arc<TlsContext> {
        TlsContext::load(
            self.write(&node_dir.join("tls"), Role::Node, "pending"),
            Role::Node,
        )
        .unwrap()
    }

    /// Re-issue a node's certificate for its id and reload the running
    /// node (a certificate rotation under the same CA).
    ///
    /// # Panics
    ///
    /// On failures.
    pub fn name_node(&self, node_dir: &Path, id: u64, ctx: &TlsContext) {
        self.write(&node_dir.join("tls"), Role::Node, &id.to_string());
        assert!(ctx.reload().unwrap(), "node certificate reloaded");
    }
}
