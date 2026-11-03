//! HTTP server wiring around an [`S3`] implementation: SigV4 (via `s3s`),
//! the authorizer (RFC-012 §5.3) and the audit record per request
//! (RFC-012 §5.7).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::future::BoxFuture;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use s3s::S3;
use s3s::access::S3Access;
use s3s::service::{S3Service, S3ServiceBuilder};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::auth::{AuditRecord, CredentialStore, StoreAuth};

/// Gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Address to listen on. Port 0 picks a free port.
    pub listen: SocketAddr,
    /// Static credentials `(access key, secret key)` for a single principal
    /// named after the access key. `None` and no `store` disables
    /// authentication and accepts anonymous requests; only for tests and
    /// experiments.
    pub credentials: Option<(String, String)>,
    /// Multi-key credential store (takes precedence over `credentials`).
    pub store: Option<Arc<CredentialStore>>,
    /// Emit an audit record per request on target `arca.audit`.
    pub audit: bool,
    /// Request accounting for `/metrics` (RFC-013 §7), if wanted.
    pub stats: Option<Arc<crate::requests::S3Stats>>,
}

impl GatewayConfig {
    /// A configuration with static credentials (or anonymous when `None`).
    #[must_use]
    pub fn new(listen: SocketAddr, credentials: Option<(String, String)>) -> Self {
        Self {
            listen,
            credentials,
            store: None,
            audit: true,
            stats: None,
        }
    }
}

/// A running gateway.
#[derive(Debug)]
pub struct ServerHandle {
    /// The address actually bound.
    pub local_addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl ServerHandle {
    /// Stop accepting connections and wait for in-flight ones to finish.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.task.await;
    }
}

/// Bind and start serving `backend` without an authorizer (every
/// authenticated request is allowed; the memory backend and most tests).
///
/// # Errors
///
/// Returns the bind error.
pub async fn serve<B>(cfg: GatewayConfig, backend: B) -> std::io::Result<ServerHandle>
where
    B: S3 + Send + Sync + 'static,
{
    serve_with_access(cfg, backend, None::<AllowAll>).await
}

/// An `S3Access` that allows everything (no authorizer configured).
#[derive(Debug, Clone, Copy)]
pub struct AllowAll;

#[async_trait::async_trait]
impl S3Access for AllowAll {}

/// Bind and start serving `backend` with an authorizer.
///
/// # Errors
///
/// Returns the bind error.
pub async fn serve_with_access<B, A>(
    cfg: GatewayConfig,
    backend: B,
    access: Option<A>,
) -> std::io::Result<ServerHandle>
where
    B: S3 + Send + Sync + 'static,
    A: S3Access,
{
    let service = {
        // Client-visible error messages never carry internal addresses.
        let mut b = S3ServiceBuilder::new(crate::redact::Redacted(backend));
        let store = cfg.store.clone().or_else(|| {
            cfg.credentials
                .as_ref()
                .map(|(ak, sk)| Arc::new(CredentialStore::from_keys(&[(ak, sk, ak)])))
        });
        if let Some(store) = store {
            if let Some(s) = &cfg.stats {
                s.set_credentials(Arc::clone(&store));
            }
            b.set_auth(StoreAuth(store));
        }
        if let Some(a) = access {
            b.set_access(a);
        }
        b.build()
    };
    let listener = TcpListener::bind(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let (tx, mut rx) = oneshot::channel::<()>();
    let audit = cfg.audit;
    let stats = cfg.stats.clone();

    let task = tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        let graceful = GracefulShutdown::new();
        loop {
            let (socket, peer) = tokio::select! {
                res = listener.accept() => match res {
                    Ok(conn) => conn,
                    Err(_) => continue,
                },
                _ = &mut rx => break,
            };
            let svc = Audited {
                inner: service.clone(),
                peer: Some(peer),
                audit,
                stats: stats.clone(),
            };
            let conn = http.serve_connection(TokioIo::new(socket), svc);
            let conn = graceful.watch(conn.into_owned());
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
        tokio::select! {
            () = graceful.shutdown() => {}
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}
        }
    });

    Ok(ServerHandle {
        local_addr,
        shutdown: tx,
        task,
    })
}

/// The access key id named by a SigV4 `Authorization` header or a presigned
/// URL's `X-Amz-Credential`, if any. Only the id is taken: the credential
/// scope, signed headers and signature are left behind.
fn claimed_key_id(headers: &http::HeaderMap, uri: &http::Uri) -> Option<String> {
    let from_header = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split("Credential=").nth(1))
        .and_then(|v| v.split('/').next());
    let from_query = uri.query().and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == "X-Amz-Credential").then(|| {
                v.split("%2F")
                    .next()
                    .unwrap_or(v)
                    .split('/')
                    .next()
                    .unwrap_or(v)
            })
        })
    });
    from_header
        .or(from_query)
        .filter(|id| !id.is_empty() && id.len() <= 128)
        .map(str::to_string)
}

/// Wraps the `s3s` service: creates the audit record, hands it to the
/// authorizer and the backend through the request extensions, stamps
/// `x-amz-request-id` on the response, and emits the record once the
/// response is ready.
#[derive(Clone)]
struct Audited {
    inner: S3Service,
    peer: Option<SocketAddr>,
    audit: bool,
    stats: Option<Arc<crate::requests::S3Stats>>,
}

impl hyper::service::Service<http::Request<hyper::body::Incoming>> for Audited {
    type Response = s3s::HttpResponse;
    type Error = s3s::HttpError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, mut req: http::Request<hyper::body::Incoming>) -> Self::Future {
        let record = AuditRecord::new(self.peer);
        // The access key id the request claims (never the signature), so
        // that a refused key is still attributable (RFC-012 S-A9); the
        // authorizer overwrites the principal once it resolves the key.
        let claimed = claimed_key_id(req.headers(), req.uri());
        record.with(|f| {
            f.key_id.clone_from(&claimed);
            f.principal = if claimed.is_some() {
                String::new()
            } else {
                "anonymous".to_string()
            };
        });
        let request_id = record.fields().request_id;
        let bytes_in = req
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        req.extensions_mut().insert(record.clone());
        let started = Instant::now();
        let fut = hyper::service::Service::call(&self.inner, req);
        let audit = self.audit;
        let s3_stats = self.stats.clone();
        Box::pin(async move {
            let mut res = fut.await;
            let status = match &res {
                Ok(r) => r.status().as_u16(),
                Err(_) => 500,
            };
            if let Ok(r) = &mut res
                && let Ok(v) = http::HeaderValue::from_str(&request_id)
            {
                r.headers_mut().insert("x-amz-request-id", v);
            }
            let bytes_out = res
                .as_ref()
                .ok()
                .and_then(|r| r.headers().get(http::header::CONTENT_LENGTH))
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            if let Some(s) = &s3_stats {
                s.record(
                    &record.fields().operation,
                    status,
                    bytes_in,
                    bytes_out,
                    started.elapsed(),
                );
            }
            if audit {
                record.emit(status, bytes_in, bytes_out, started.elapsed().as_millis());
            }
            res
        })
    }
}
