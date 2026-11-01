//! arca-lake command-line entry point.

use std::net::SocketAddr;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "arca-lake",
    version,
    about = "arca-lake distributed object storage (design phase)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run an S3 gateway.
    Gateway(GatewayArgs),
    /// Run a storage node (not implemented yet).
    StorageNode,
    /// Run the control plane (not implemented yet).
    Controller,
}

#[derive(Debug, clap::Args)]
struct GatewayArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,
    /// Access key for static authentication. Requires `--secret-key`.
    #[arg(long, env = "ARCA_LAKE_ACCESS_KEY", requires = "secret_key")]
    access_key: Option<String>,
    /// Secret key for static authentication. Requires `--access-key`.
    #[arg(
        long,
        env = "ARCA_LAKE_SECRET_KEY",
        requires = "access_key",
        hide_env_values = true
    )]
    secret_key: Option<String>,
    /// Allow anonymous access (no authentication). Phase 0 experiments only.
    #[arg(long, conflicts_with = "access_key")]
    allow_anonymous: bool,
    /// Backend. Only `memory` exists in Phase 0.
    #[arg(long, default_value = "memory", value_parser = ["memory"])]
    backend: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Gateway(args) => run_gateway(args).await,
        Command::StorageNode | Command::Controller => {
            eprintln!("not implemented yet; see docs/roadmap/phase-0.md");
            ExitCode::from(2)
        }
    }
}

async fn run_gateway(args: GatewayArgs) -> ExitCode {
    let credentials = match (args.access_key, args.secret_key) {
        (Some(ak), Some(sk)) => Some((ak, sk)),
        _ if args.allow_anonymous => {
            eprintln!("WARNING: authentication disabled; every request is accepted");
            None
        }
        _ => {
            eprintln!("error: provide --access-key/--secret-key or --allow-anonymous");
            return ExitCode::from(2);
        }
    };
    let cfg = arca_lake_gateway::GatewayConfig {
        listen: args.listen,
        credentials,
    };
    let server = match arca_lake_gateway::serve(
        cfg,
        arca_lake_gateway::memory::MemoryBackend::new(),
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot bind {}: {e}", args.listen);
            return ExitCode::from(1);
        }
    };
    eprintln!(
        "gateway listening on http://{} (backend: {})",
        server.local_addr, args.backend
    );
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("shutting down");
    server.shutdown().await;
    ExitCode::SUCCESS
}
