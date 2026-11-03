//! Development PKI (RFC-012 §10, ADR-0017): a CA, SPIFFE leaf certificates
//! and CRLs for test and lab clusters. Production clusters use their own
//! CA (or SPIRE); nothing here is needed at runtime.

use std::time::{Duration, SystemTime};

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose,
    RevokedCertParams, SanType, SerialNumber,
};

use crate::tls::Role;

/// PEM text of a certificate and its key.
#[derive(Debug, Clone)]
pub struct Issued {
    /// Certificate (PEM).
    pub cert_pem: String,
    /// Private key (PEM, PKCS#8). Secret: write with mode 0600.
    pub key_pem: String,
    /// Serial number.
    pub serial: u64,
}

/// A certificate authority.
pub struct Ca {
    issuer: Issuer<'static, KeyPair>,
    /// CA certificate (PEM).
    pub cert_pem: String,
}

impl std::fmt::Debug for Ca {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ca").finish_non_exhaustive()
    }
}

fn ts(t: SystemTime) -> time::OffsetDateTime {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    time::OffsetDateTime::from_unix_timestamp(i64::try_from(secs).unwrap_or(0))
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
}

#[allow(clippy::needless_pass_by_value)]
fn err(e: rcgen::Error) -> String {
    e.to_string()
}

impl Ca {
    /// A new self-signed CA valid for `valid`.
    ///
    /// # Errors
    ///
    /// Key generation or signing errors.
    pub fn new(name: &str, valid: Duration) -> Result<Self, String> {
        let key = KeyPair::generate().map_err(err)?;
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let now = SystemTime::now();
        params.not_before = ts(now - Duration::from_secs(300));
        params.not_after = ts(now + valid);
        let cert = params.self_signed(&key).map_err(err)?;
        Ok(Self {
            cert_pem: cert.pem(),
            issuer: Issuer::new(params, key),
        })
    }

    /// Restore a CA from its PEM certificate and key (the operator tool
    /// keeps both in a directory).
    ///
    /// # Errors
    ///
    /// PEM that does not parse.
    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<Self, String> {
        let key = KeyPair::from_pem(key_pem).map_err(err)?;
        let issuer = Issuer::from_ca_cert_pem(cert_pem, key).map_err(err)?;
        Ok(Self {
            issuer,
            cert_pem: cert_pem.to_string(),
        })
    }

    /// Private key of the CA (PEM). Secret.
    #[must_use]
    pub fn key_pem(&self) -> String {
        self.issuer.key().serialize_pem()
    }

    /// Issue `spiffe://<trust_domain>/<role>/<name>` valid from
    /// `not_before` for `valid`, with both server and client usages.
    ///
    /// # Errors
    ///
    /// Key generation or signing errors.
    pub fn issue_at(
        &self,
        trust_domain: &str,
        role: Role,
        name: &str,
        serial: u64,
        not_before: SystemTime,
        valid: Duration,
    ) -> Result<Issued, String> {
        let key = KeyPair::generate().map_err(err)?;
        let mut p = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
        let uri = format!("spiffe://{trust_domain}/{}/{name}", role.as_str());
        p.subject_alt_names = vec![SanType::URI(uri.as_str().try_into().map_err(err)?)];
        p.distinguished_name
            .push(DnType::CommonName, format!("{}/{name}", role.as_str()));
        p.is_ca = IsCa::NoCa;
        p.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        p.serial_number = Some(SerialNumber::from(serial));
        p.not_before = ts(not_before);
        p.not_after = ts(not_before + valid);
        let cert = p.signed_by(&key, &self.issuer).map_err(err)?;
        Ok(Issued {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            serial,
        })
    }

    /// Issue a certificate valid from five minutes ago for `valid`.
    ///
    /// # Errors
    ///
    /// As [`Self::issue_at`].
    pub fn issue(
        &self,
        trust_domain: &str,
        role: Role,
        name: &str,
        serial: u64,
        valid: Duration,
    ) -> Result<Issued, String> {
        self.issue_at(
            trust_domain,
            role,
            name,
            serial,
            SystemTime::now() - Duration::from_secs(300),
            valid,
        )
    }

    /// A CRL revoking `serials`, valid for a day.
    ///
    /// # Errors
    ///
    /// Signing errors.
    pub fn crl(&self, serials: &[u64], number: u64) -> Result<String, String> {
        let now = SystemTime::now();
        let params = CertificateRevocationListParams {
            this_update: ts(now - Duration::from_secs(60)),
            next_update: ts(now + Duration::from_hours(24)),
            crl_number: SerialNumber::from(number),
            issuing_distribution_point: None,
            revoked_certs: serials
                .iter()
                .map(|s| RevokedCertParams {
                    serial_number: SerialNumber::from(*s),
                    revocation_time: ts(now - Duration::from_secs(60)),
                    reason_code: None,
                    invalidity_date: None,
                })
                .collect(),
            key_identifier_method: KeyIdMethod::Sha256,
        };
        params
            .signed_by(&self.issuer)
            .map_err(err)?
            .pem()
            .map_err(err)
    }
}
