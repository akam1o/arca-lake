//! HTTP server wiring around an [`S3`] implementation.

use std::net::SocketAddr;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use s3s::S3;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Address to listen on. Port 0 picks a free port.
    pub listen: SocketAddr,
    /// Static credentials `(access key, secret key)`. `None` disables
    /// authentication and accepts anonymous requests; only for tests.
    pub credentials: Option<(String, String)>,
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

/// Bind and start serving `backend`. Returns once the listener is bound.
///
/// # Errors
///
/// Returns the bind error.
pub async fn serve<B>(cfg: GatewayConfig, backend: B) -> std::io::Result<ServerHandle>
where
    B: S3 + Send + Sync + 'static,
{
    let service = {
        let mut b = S3ServiceBuilder::new(backend);
        if let Some((ak, sk)) = cfg.credentials {
            b.set_auth(SimpleAuth::from_single(ak, sk));
        }
        b.build()
    };
    let listener = TcpListener::bind(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let (tx, mut rx) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        let graceful = GracefulShutdown::new();
        loop {
            let (socket, _) = tokio::select! {
                res = listener.accept() => match res {
                    Ok(conn) => conn,
                    Err(_) => continue,
                },
                _ = &mut rx => break,
            };
            let conn = http.serve_connection(TokioIo::new(socket), service.clone());
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
