//! Mutual TLS between arca-lake processes (RFC-012 §5.5, ADR-0017).
//!
//! Every process holds a [`TlsContext`]: its certificate chain and key, the
//! trust bundle (one or more roots), optional CRLs and the trust domain.
//! Identities are SPIFFE URI SANs `spiffe://<trust-domain>/<role>/<name>`.
//! The context is reloaded when its files change; a reload that fails
//! validation keeps the previous material. When the trust bundle or the
//! CRLs change, every connection authenticated under the previous trust is
//! closed ([`TlsStream`] checks the trust generation on every read and
//! write), so a withdrawn identity cannot keep a connection open.
//!
//! Key material is never logged; errors name files, not contents.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{
    CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName, UnixTime,
};
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Role of a process (RFC-012 §5.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Storage node.
    Node,
    /// S3 gateway.
    Gateway,
    /// Controller.
    Controller,
    /// Operator tools.
    Admin,
}

impl Role {
    /// Path segment in the SPIFFE id.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Gateway => "gateway",
            Self::Controller => "controller",
            Self::Admin => "admin",
        }
    }

    /// Parse a path segment.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "node" => Self::Node,
            "gateway" => Self::Gateway,
            "controller" => Self::Controller,
            "admin" => Self::Admin,
            _ => return None,
        })
    }
}

/// An authenticated peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerIdentity {
    /// Trust domain.
    pub trust_domain: String,
    /// Role.
    pub role: Role,
    /// Name (node id for nodes, process name otherwise).
    pub name: String,
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "spiffe://{}/{}/{}",
            self.trust_domain,
            self.role.as_str(),
            self.name
        )
    }
}

/// Parse `spiffe://<trust-domain>/<role>/<name>`.
#[must_use]
pub fn parse_spiffe(uri: &str) -> Option<PeerIdentity> {
    let rest = uri.strip_prefix("spiffe://")?;
    let mut it = rest.split('/');
    let td = it.next()?;
    let role = Role::parse(it.next()?)?;
    let name = it.next()?;
    if td.is_empty() || name.is_empty() || it.next().is_some() {
        return None;
    }
    let ok = |s: &str| {
        s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_'))
    };
    if !ok(td) || !ok(name) {
        return None;
    }
    Some(PeerIdentity {
        trust_domain: td.to_string(),
        role,
        name: name.to_string(),
    })
}

/// SPIFFE identities and the expiry of a DER certificate.
///
/// # Errors
///
/// A certificate that does not parse.
pub fn certificate_identities(der: &[u8]) -> Result<(Vec<PeerIdentity>, i64), TlsError> {
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|e| TlsError::Invalid(format!("certificate does not parse: {e}")))?;
    let mut ids = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for n in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::URI(u) = n
                && let Some(id) = parse_spiffe(u)
            {
                ids.push(id);
            }
        }
    }
    Ok((ids, cert.validity().not_after.timestamp()))
}

/// [`certificate_identities`] of the first certificate in a PEM file.
///
/// # Errors
///
/// PEM or certificate that does not parse.
pub fn pem_certificate_identities(pem: &[u8]) -> Result<(Vec<PeerIdentity>, i64), TlsError> {
    let der =
        CertificateDer::from_pem_slice(pem).map_err(|e| TlsError::Invalid(format!("PEM: {e}")))?;
    certificate_identities(&der)
}

/// TLS configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A file could not be read.
    #[error("{path}: {source}")]
    Io {
        /// File.
        path: PathBuf,
        /// Error.
        source: io::Error,
    },
    /// Material that does not validate.
    #[error("invalid TLS material: {0}")]
    Invalid(String),
}

/// Files and trust domain of a process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// Trust bundle (PEM, one or more CA certificates).
    pub ca: PathBuf,
    /// Own certificate chain (PEM, leaf first).
    pub cert: PathBuf,
    /// Own private key (PEM, PKCS#8, PKCS#1 or SEC1).
    pub key: PathBuf,
    /// Certificate revocation lists (PEM), if any.
    pub crl: Option<PathBuf>,
    /// Trust domain every peer must belong to.
    pub trust_domain: String,
}

/// Counters of a context (exported by each process).
#[derive(Debug, Default)]
pub struct TlsStats {
    /// Handshakes that failed (both directions).
    pub handshake_failures: AtomicU64,
    /// Successful reloads that changed the material.
    pub reloads: AtomicU64,
    /// Reloads refused because the new material did not validate.
    pub reload_failures: AtomicU64,
    /// RPCs refused by the authorization table.
    pub rpcs_denied: AtomicU64,
    /// Connections closed because the trust they were authenticated under
    /// was withdrawn.
    pub connections_revoked: AtomicU64,
    /// `not_after` of the own certificate, Unix seconds.
    pub cert_not_after: AtomicI64,
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })
}

struct Material {
    raw: (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>),
    trust_raw: (Vec<u8>, Vec<u8>),
    server: Arc<rustls::ServerConfig>,
    roots: Arc<RootCertStore>,
    crls: Vec<CertificateRevocationListDer<'static>>,
    chain: Vec<CertificateDer<'static>>,
    key: Arc<PrivateKeyDer<'static>>,
    own: PeerIdentity,
    not_after: i64,
}

/// The outcome of a WebPKI server verification with the host-name check
/// ignored: identities are SPIFFE URIs, checked separately (ADR-0017).
fn chain_only(r: Result<ServerCertVerified, rustls::Error>) -> Result<(), rustls::Error> {
    match r {
        Ok(_)
        | Err(rustls::Error::InvalidCertificate(
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
        )) => Ok(()),
        Err(e) => Err(e),
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

#[allow(clippy::too_many_lines)]
fn load(files: &TlsFiles) -> Result<Material, TlsError> {
    let ca = read(&files.ca)?;
    let cert = read(&files.cert)?;
    let key = read(&files.key)?;
    let crl = match &files.crl {
        Some(p) => read(p)?,
        None => Vec::new(),
    };
    let mut roots = RootCertStore::empty();
    for c in CertificateDer::pem_slice_iter(&ca) {
        let c = c.map_err(|e| TlsError::Invalid(format!("{}: {e}", files.ca.display())))?;
        roots
            .add(c)
            .map_err(|e| TlsError::Invalid(format!("{}: {e}", files.ca.display())))?;
    }
    if roots.is_empty() {
        return Err(TlsError::Invalid(format!(
            "{}: no CA certificate",
            files.ca.display()
        )));
    }
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::Invalid(format!("{}: {e}", files.cert.display())))?;
    let Some(leaf) = chain.first() else {
        return Err(TlsError::Invalid(format!(
            "{}: no certificate",
            files.cert.display()
        )));
    };
    let pk = PrivateKeyDer::from_pem_slice(&key).map_err(|_| {
        TlsError::Invalid(format!("{}: no usable private key", files.key.display()))
    })?;
    let crls: Vec<CertificateRevocationListDer<'static>> = if crl.is_empty() {
        Vec::new()
    } else {
        CertificateRevocationListDer::pem_slice_iter(&crl)
            .collect::<Result<_, _>>()
            .map_err(|e| TlsError::Invalid(format!("CRL: {e}")))?
    };
    let (ids, not_after) = certificate_identities(leaf)?;
    let own = ids
        .into_iter()
        .find(|i| i.trust_domain == files.trust_domain)
        .ok_or_else(|| {
            TlsError::Invalid(format!(
                "{}: no spiffe://{}/<role>/<name> URI SAN",
                files.cert.display(),
                files.trust_domain
            ))
        })?;
    let roots = Arc::new(roots);
    let p = provider();
    let client_verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::clone(&roots),
        Arc::clone(&p),
    )
    .with_crls(crls.clone())
    // A CRL file covers the issuers it lists; certificates of other
    // roots in the bundle (a CA rotation) have no CRL to consult.
    .allow_unknown_revocation_status()
    .only_check_end_entity_revocation()
    .build()
    .map_err(|e| TlsError::Invalid(format!("client verifier: {e}")))?;
    let mut server = rustls::ServerConfig::builder_with_provider(Arc::clone(&p))
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Invalid(e.to_string()))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(chain.clone(), pk.clone_key())
        .map_err(|e| TlsError::Invalid(format!("{}: {e}", files.key.display())))?;
    server.alpn_protocols = vec![b"h2".to_vec()];
    // Validate the own chain against the bundle, as a peer would.
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::clone(&roots),
        Arc::clone(&p),
    )
    .with_crls(crls.clone())
    // A CRL file covers the issuers it lists; certificates of other
    // roots in the bundle (a CA rotation) have no CRL to consult.
    .allow_unknown_revocation_status()
    .only_check_end_entity_revocation()
    .build()
    .map_err(|e| TlsError::Invalid(format!("server verifier: {e}")))?;
    let name = ServerName::try_from("arca.invalid").expect("literal");
    if let Err(e) =
        chain_only(verifier.verify_server_cert(leaf, &chain[1..], &name, &[], UnixTime::now()))
    {
        return Err(TlsError::Invalid(format!(
            "{}: own certificate does not validate against {}: {e}",
            files.cert.display(),
            files.ca.display()
        )));
    }
    Ok(Material {
        raw: (ca.clone(), cert, key, crl.clone()),
        trust_raw: (ca, crl),
        server: Arc::new(server),
        roots,
        crls,
        chain,
        key: Arc::new(pk),
        own,
        not_after,
    })
}

/// A process's TLS state.
pub struct TlsContext {
    files: TlsFiles,
    current: RwLock<Arc<Material>>,
    trust_generation: Arc<AtomicU64>,
    /// Counters.
    pub stats: Arc<TlsStats>,
}

impl fmt::Debug for TlsContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsContext")
            .field("identity", &self.identity().to_string())
            .field("ca", &self.files.ca)
            .field("cert", &self.files.cert)
            .finish_non_exhaustive()
    }
}

impl TlsContext {
    /// Load and validate; the own certificate must carry a SPIFFE id in
    /// the trust domain with `role`.
    ///
    /// # Errors
    ///
    /// Unreadable files, material that does not validate, a wrong role.
    pub fn load(files: TlsFiles, role: Role) -> Result<Arc<Self>, TlsError> {
        let m = load(&files)?;
        if m.own.role != role {
            return Err(TlsError::Invalid(format!(
                "{}: certificate identity {} has role {}, this process is a {}",
                files.cert.display(),
                m.own,
                m.own.role.as_str(),
                role.as_str()
            )));
        }
        let stats = Arc::new(TlsStats::default());
        stats.cert_not_after.store(m.not_after, Ordering::Relaxed);
        Ok(Arc::new(Self {
            files,
            current: RwLock::new(Arc::new(m)),
            trust_generation: Arc::new(AtomicU64::new(0)),
            stats,
        }))
    }

    fn material(&self) -> Arc<Material> {
        Arc::clone(&self.current.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Own identity.
    #[must_use]
    pub fn identity(&self) -> PeerIdentity {
        self.material().own.clone()
    }

    /// Trust domain.
    #[must_use]
    pub fn trust_domain(&self) -> &str {
        &self.files.trust_domain
    }

    /// Own certificate expiry, Unix seconds.
    #[must_use]
    pub fn not_after(&self) -> i64 {
        self.material().not_after
    }

    /// Re-read the files; returns whether anything changed. Invalid new
    /// material is refused and the previous kept.
    ///
    /// # Errors
    ///
    /// The validation failure (the previous material stays in force).
    pub fn reload(&self) -> Result<bool, TlsError> {
        let cur = self.material();
        let new = match load(&self.files) {
            Ok(m) => m,
            Err(e) => {
                // Unchanged files that fail now (e.g. a CRL past its next
                // update) are still a failure worth counting.
                self.stats.reload_failures.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        };
        if new.raw == cur.raw {
            return Ok(false);
        }
        if new.own.role != cur.own.role {
            self.stats.reload_failures.fetch_add(1, Ordering::Relaxed);
            return Err(TlsError::Invalid(format!(
                "new certificate has role {}, this process is a {}",
                new.own.role.as_str(),
                cur.own.role.as_str()
            )));
        }
        let trust_changed = new.trust_raw != cur.trust_raw;
        self.stats
            .cert_not_after
            .store(new.not_after, Ordering::Relaxed);
        tracing::info!(
            identity = %new.own,
            not_after = new.not_after,
            trust_changed,
            "TLS material reloaded"
        );
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(new);
        if trust_changed {
            self.trust_generation.fetch_add(1, Ordering::SeqCst);
        }
        self.stats.reloads.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Reload every `every` while the context lives.
    pub fn spawn_reloader(self: &Arc<Self>, every: Duration) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(ctx) = weak.upgrade() else { return };
                if let Err(e) = ctx.reload() {
                    tracing::warn!(error = %e, "TLS reload refused; keeping the previous material");
                }
            }
        });
    }

    /// Server configuration for new connections.
    #[must_use]
    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.material().server)
    }

    /// Client configuration expecting a node peer (with `node_id` when the
    /// caller knows which node it is calling).
    ///
    /// # Errors
    ///
    /// Material that no longer builds (should not happen after `load`).
    pub fn client_config(
        &self,
        node_id: Option<u64>,
    ) -> Result<Arc<rustls::ClientConfig>, TlsError> {
        let m = self.material();
        let p = provider();
        let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::clone(&m.roots),
            Arc::clone(&p),
        )
        .with_crls(m.crls.clone())
        .allow_unknown_revocation_status()
        .only_check_end_entity_revocation()
        .build()
        .map_err(|e| TlsError::Invalid(e.to_string()))?;
        let verifier = Arc::new(NodeVerifier {
            inner,
            trust_domain: self.files.trust_domain.clone(),
            node_id,
            provider: Arc::clone(&p),
        });
        let mut cfg = rustls::ClientConfig::builder_with_provider(p)
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::Invalid(e.to_string()))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(m.chain.clone(), m.key.clone_key())
            .map_err(|e| TlsError::Invalid(e.to_string()))?;
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Arc::new(cfg))
    }

    /// The identity a server presented or a client certificate carried,
    /// in this context's trust domain.
    #[must_use]
    pub fn peer_identity(&self, certs: Option<&[CertificateDer<'_>]>) -> Option<PeerIdentity> {
        let leaf = certs?.first()?;
        certificate_identities(leaf)
            .ok()?
            .0
            .into_iter()
            .find(|i| i.trust_domain == self.files.trust_domain)
    }

    /// Wrap an authenticated stream so that it closes when the trust it
    /// was authenticated under is withdrawn.
    pub fn track<S>(&self, inner: S) -> TlsStream<S> {
        TlsStream {
            inner,
            generation: self.trust_generation.load(Ordering::SeqCst),
            current: Arc::clone(&self.trust_generation),
            stats: Arc::clone(&self.stats),
            revoked: false,
        }
    }
}

/// Verifies a node's chain against the bundle (and CRLs), ignores host
/// names, and requires `spiffe://<td>/node/<id>`.
#[derive(Debug)]
struct NodeVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    trust_domain: String,
    node_id: Option<u64>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for NodeVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        chain_only(self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ))?;
        let (ids, _) = certificate_identities(end_entity)
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        let ok = ids.iter().any(|i| {
            i.trust_domain == self.trust_domain
                && i.role == Role::Node
                && self.node_id.is_none_or(|n| i.name == n.to_string())
        });
        if ok {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Prometheus text for a process's TLS state (`prefix` is `arca_node`,
/// `arca_gateway` or `arca_controller`); `stats` is `None` on an explicitly
/// plaintext process, which reports `_tls_enabled 0`.
#[must_use]
pub fn render_metrics(prefix: &str, stats: Option<&TlsStats>) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HELP {prefix}_tls_enabled 1 when node RPCs use mutual TLS, 0 when explicitly plaintext"
    );
    let _ = writeln!(out, "# TYPE {prefix}_tls_enabled gauge");
    let _ = writeln!(out, "{prefix}_tls_enabled {}", u8::from(stats.is_some()));
    let Some(s) = stats else { return out };
    for (name, help, v) in [
        (
            "handshake_failures_total",
            "TLS handshakes refused or failed",
            &s.handshake_failures,
        ),
        (
            "reloads_total",
            "TLS material reloads that changed it",
            &s.reloads,
        ),
        (
            "reload_failures_total",
            "TLS reloads refused because the new material did not validate",
            &s.reload_failures,
        ),
        (
            "rpcs_denied_total",
            "RPCs refused by the role authorization table",
            &s.rpcs_denied,
        ),
        (
            "connections_revoked_total",
            "connections closed because their trust was withdrawn",
            &s.connections_revoked,
        ),
    ] {
        let _ = writeln!(out, "# HELP {prefix}_tls_{name} {help}");
        let _ = writeln!(out, "# TYPE {prefix}_tls_{name} counter");
        let _ = writeln!(out, "{prefix}_tls_{name} {}", v.load(Ordering::Relaxed));
    }
    let _ = writeln!(
        out,
        "# HELP {prefix}_tls_cert_not_after_seconds expiry of the own certificate, Unix seconds"
    );
    let _ = writeln!(out, "# TYPE {prefix}_tls_cert_not_after_seconds gauge");
    let _ = writeln!(
        out,
        "{prefix}_tls_cert_not_after_seconds {}",
        s.cert_not_after.load(Ordering::Relaxed)
    );
    out
}

/// A stream that fails once the trust generation it was opened under is
/// superseded (trust bundle or CRL changed).
pub struct TlsStream<S> {
    inner: S,
    generation: u64,
    current: Arc<AtomicU64>,
    stats: Arc<TlsStats>,
    revoked: bool,
}

impl<S> TlsStream<S> {
    /// The wrapped stream.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    fn check(&mut self) -> io::Result<()> {
        if self.revoked || self.current.load(Ordering::SeqCst) != self.generation {
            if !self.revoked {
                self.revoked = true;
                self.stats
                    .connections_revoked
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "trust bundle changed; connection closed",
            ));
        }
        Ok(())
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TlsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(e) = self.check() {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TlsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(e) = self.check() {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A server-side connection: TLS (authenticated) or explicitly plaintext.
pub enum ServerIo {
    /// Plaintext (`--insecure-plaintext`).
    Plain(tokio::net::TcpStream, PeerInfo),
    /// Mutually authenticated.
    Tls(
        Box<TlsStream<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>>,
        PeerInfo,
    ),
}

impl tonic::transport::server::Connected for ServerIo {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            Self::Plain(_, p) | Self::Tls(_, p) => p.clone(),
        }
    }
}

impl AsyncRead for ServerIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s, _) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s, _) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ServerIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s, _) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s, _) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s, _) => Pin::new(s).poll_flush(cx),
            Self::Tls(s, _) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s, _) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s, _) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// TLS handshake deadline (RFC-012 §5.9).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Accept connections on `listener`: with `tls`, complete a mutual TLS
/// handshake (concurrently, bounded by [`HANDSHAKE_TIMEOUT`]) and attach
/// the peer identity; without, pass plaintext connections through. The
/// stream ends when the listener fails.
pub fn incoming(
    listener: tokio::net::TcpListener,
    tls: Option<Arc<TlsContext>>,
) -> tokio_stream::wrappers::ReceiverStream<io::Result<ServerIo>> {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        loop {
            // Stop (and release the listener) once the server dropped the
            // stream: a restarted node must be able to bind the address.
            let accepted = tokio::select! {
                r = listener.accept() => r,
                () = tx.closed() => return,
            };
            let (tcp, remote) = match accepted {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let _ = tcp.set_nodelay(true);
            let Some(ctx) = tls.clone() else {
                let info = PeerInfo {
                    identity: None,
                    remote: Some(remote),
                };
                if tx.send(Ok(ServerIo::Plain(tcp, info))).await.is_err() {
                    return;
                }
                continue;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let acceptor = tokio_rustls::TlsAcceptor::from(ctx.server_config());
                let hs = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await;
                let stream = match hs {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        ctx.stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(%remote, error = %e, "TLS handshake refused");
                        return;
                    }
                    Err(_) => {
                        ctx.stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(%remote, "TLS handshake timed out");
                        return;
                    }
                };
                let identity = ctx.peer_identity(stream.get_ref().1.peer_certificates());
                if identity.is_none() {
                    // The chain verified but carries no identity in our
                    // trust domain: authenticated, yet nothing is allowed.
                    ctx.stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%remote, "peer certificate has no identity in this trust domain");
                    return;
                }
                let info = PeerInfo {
                    identity,
                    remote: Some(remote),
                };
                let _ = tx
                    .send(Ok(ServerIo::Tls(Box::new(ctx.track(stream)), info)))
                    .await;
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Connect to a node over mutual TLS, expecting `node_id` when known.
///
/// # Errors
///
/// Connection, handshake (counted) or identity failures.
///
/// # Panics
///
/// Never: the placeholder server name is a valid literal.
pub async fn connect(
    ctx: &Arc<TlsContext>,
    addr: &str,
    node_id: Option<u64>,
) -> io::Result<TlsStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>> {
    let tcp = tokio::net::TcpStream::connect(addr).await?;
    let _ = tcp.set_nodelay(true);
    let cfg = ctx.client_config(node_id).map_err(io::Error::other)?;
    let name = ServerName::try_from("arca.invalid").expect("literal");
    match tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_rustls::TlsConnector::from(cfg).connect(name, tcp),
    )
    .await
    {
        Ok(Ok(s)) => Ok(ctx.track(s)),
        Ok(Err(e)) => {
            ctx.stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
            Err(e)
        }
        Err(_) => {
            ctx.stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TLS handshake timed out",
            ))
        }
    }
}

/// What a storage node knows about the caller of an RPC.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Authenticated identity; `None` on an explicitly plaintext listener.
    pub identity: Option<PeerIdentity>,
    /// Remote address.
    pub remote: Option<std::net::SocketAddr>,
}

/// Storage node RPCs for authorization (ADR-0017 table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRpc {
    /// `PutShard`.
    PutShard,
    /// `GetShard`.
    GetShard,
    /// `ListShards`.
    ListShards,
    /// `DeleteShard`.
    DeleteShard,
    /// `ReclaimShard` (Phase 3.12).
    ReclaimShard,
    /// `ListPending` (Phase 3.12).
    ListPending,
    /// `VerifyShards`.
    VerifyShards,
    /// `ScrubShards`.
    ScrubShards,
    /// `UpdateGroupEpoch`.
    UpdateGroupEpoch,
    /// `Health` without cluster facts.
    Health,
    /// `Health` carrying `format_write` or an incarnation.
    HealthWithFacts,
}

impl NodeRpc {
    /// Every RPC (the table test walks it).
    pub const ALL: [Self; 11] = [
        Self::PutShard,
        Self::GetShard,
        Self::ListShards,
        Self::DeleteShard,
        Self::ReclaimShard,
        Self::ListPending,
        Self::VerifyShards,
        Self::ScrubShards,
        Self::UpdateGroupEpoch,
        Self::Health,
        Self::HealthWithFacts,
    ];

    /// Whether `role` may call this RPC (ADR-0017).
    #[must_use]
    pub const fn allows(self, role: Role) -> bool {
        match self {
            Self::PutShard | Self::GetShard | Self::ListShards | Self::DeleteShard => {
                matches!(role, Role::Gateway | Role::Controller | Role::Admin)
            }
            Self::ReclaimShard
            | Self::ListPending
            | Self::VerifyShards
            | Self::ScrubShards
            | Self::UpdateGroupEpoch
            | Self::HealthWithFacts => matches!(role, Role::Controller | Role::Admin),
            Self::Health => true,
        }
    }
}

/// Decide an RPC for a peer: `Ok` when authorised; `require_tls` is the
/// listener's mode (a plaintext listener accepts everything, by explicit
/// operator choice).
///
/// # Errors
///
/// `PermissionDenied` naming the identity and the RPC (never key material).
pub fn authorize(
    peer: Option<&PeerInfo>,
    rpc: NodeRpc,
    require_tls: bool,
) -> Result<(), tonic::Status> {
    if !require_tls {
        return Ok(());
    }
    let Some(id) = peer.and_then(|p| p.identity.as_ref()) else {
        return Err(tonic::Status::permission_denied(format!(
            "{rpc:?}: caller not authenticated"
        )));
    };
    if rpc.allows(id.role) {
        Ok(())
    } else {
        Err(tonic::Status::permission_denied(format!(
            "{rpc:?} not permitted for {id}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spiffe_ids_parse_strictly() {
        let id = parse_spiffe("spiffe://lab.example/node/7").unwrap();
        assert_eq!((id.role, id.name.as_str()), (Role::Node, "7"));
        assert_eq!(id.to_string(), "spiffe://lab.example/node/7");
        for bad in [
            "spiffe://lab.example/node",
            "spiffe://lab.example/root/7",
            "spiffe:///node/7",
            "spiffe://lab.example/node/7/x",
            "spiffe://lab example/node/7",
            "https://lab.example/node/7",
            "spiffe://lab.example/node/",
        ] {
            assert!(parse_spiffe(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn authorization_table_matches_adr_0017() {
        use NodeRpc as R;
        let roles = [Role::Gateway, Role::Controller, Role::Admin, Role::Node];
        let expect = |rpc: R| -> [bool; 4] {
            match rpc {
                R::PutShard | R::GetShard | R::ListShards | R::DeleteShard => {
                    [true, true, true, false]
                }
                R::ReclaimShard
                | R::ListPending
                | R::VerifyShards
                | R::ScrubShards
                | R::UpdateGroupEpoch
                | R::HealthWithFacts => [false, true, true, false],
                R::Health => [true, true, true, true],
            }
        };
        for rpc in R::ALL {
            let got: Vec<bool> = roles.iter().map(|r| rpc.allows(*r)).collect();
            assert_eq!(got, expect(rpc).to_vec(), "{rpc:?}");
        }
        // No identity on a TLS listener: refused; plaintext listener: open.
        assert!(authorize(None, R::Health, true).is_err());
        assert!(authorize(None, R::UpdateGroupEpoch, false).is_ok());
    }
}
