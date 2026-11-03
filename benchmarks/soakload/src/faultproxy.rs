//! A userspace TCP proxy that injects network faults between arca-lake
//! processes without touching the host's network configuration (Phase
//! 3.4 §7.2): a node is registered under the proxy's address and every
//! client reaches it through the proxy.
//!
//! The mode is read from a control file every 200 ms:
//!
//! - `pass`: forward;
//! - `delay <ms>`: hold every chunk for `<ms>` before forwarding;
//! - `blackhole`: accept and keep connections but forward nothing (a
//!   partition that does not reset sockets);
//! - `reset`: close every connection and refuse new ones.
//!
//! Bytes are forwarded unchanged (TLS passes through end to end).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Parser)]
struct Args {
    /// Listen address.
    #[arg(long)]
    listen: String,
    /// Target address.
    #[arg(long)]
    target: String,
    /// Control file (absent = pass).
    #[arg(long)]
    control: std::path::PathBuf,
}

/// 0 pass, 1 delay, 2 blackhole, 3 reset; the delay in ms; a generation
/// bumped when entering reset.
struct Mode {
    kind: AtomicU64,
    delay_ms: AtomicU64,
    generation: AtomicU64,
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mode: Arc<Mode>,
    generation: u64,
) {
    let mut buf = vec![0u8; 64 << 10];
    loop {
        if mode.generation.load(Ordering::Relaxed) != generation {
            return;
        }
        let n = match tokio::time::timeout(Duration::from_millis(200), from.read(&mut buf)).await {
            Err(_) => continue,
            Ok(Ok(0) | Err(_)) => return,
            Ok(Ok(n)) => n,
        };
        loop {
            match mode.kind.load(Ordering::Relaxed) {
                1 => {
                    tokio::time::sleep(Duration::from_millis(
                        mode.delay_ms.load(Ordering::Relaxed),
                    ))
                    .await;
                    break;
                }
                2 => {
                    if mode.generation.load(Ordering::Relaxed) != generation {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                3 => return,
                _ => break,
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mode = Arc::new(Mode {
        kind: AtomicU64::new(0),
        delay_ms: AtomicU64::new(0),
        generation: AtomicU64::new(0),
    });
    {
        let mode = Arc::clone(&mode);
        let control = args.control.clone();
        tokio::spawn(async move {
            let mut last = String::new();
            loop {
                let text = tokio::fs::read_to_string(&control)
                    .await
                    .unwrap_or_default();
                let text = text.trim().to_string();
                if text != last {
                    let mut it = text.split_whitespace();
                    let (k, d) = match (it.next(), it.next()) {
                        (Some("delay"), Some(ms)) => (1, ms.parse().unwrap_or(0)),
                        (Some("blackhole"), _) => (2, 0),
                        (Some("reset"), _) => (3, 0),
                        _ => (0, 0),
                    };
                    mode.delay_ms.store(d, Ordering::Relaxed);
                    if k == 3 || mode.kind.load(Ordering::Relaxed) == 2 && k != 2 {
                        // Reset closes everything; leaving a blackhole drops
                        // the stalled connections too (clients reconnect).
                        mode.generation.fetch_add(1, Ordering::Relaxed);
                    }
                    mode.kind.store(k, Ordering::Relaxed);
                    eprintln!("faultproxy: mode {text:?}");
                    last = text;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }
    let listener = TcpListener::bind(&args.listen).await.expect("bind");
    loop {
        let Ok((inbound, _)) = listener.accept().await else {
            continue;
        };
        if mode.kind.load(Ordering::Relaxed) == 3 {
            drop(inbound);
            continue;
        }
        let target = args.target.clone();
        let mode = Arc::clone(&mode);
        tokio::spawn(async move {
            let Ok(outbound) = TcpStream::connect(&target).await else {
                return;
            };
            let _ = inbound.set_nodelay(true);
            let _ = outbound.set_nodelay(true);
            let generation = mode.generation.load(Ordering::Relaxed);
            let (ri, wi) = inbound.into_split();
            let (ro, wo) = outbound.into_split();
            let a = tokio::spawn(pump(ri, wo, Arc::clone(&mode), generation));
            let b = tokio::spawn(pump(ro, wi, mode, generation));
            let _ = tokio::join!(a, b);
        });
    }
}
