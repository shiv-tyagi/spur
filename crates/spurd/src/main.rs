mod executor;
mod gpu;
mod reporter;

use std::sync::Arc;

use clap::Parser;
use tracing::info;

use reporter::NodeReporter;

#[derive(Parser)]
#[command(name = "spurd", about = "Spur node agent daemon")]
struct Args {
    /// Configuration file path
    #[arg(short = 'f', long, default_value = "/etc/spur/spur.conf")]
    config: std::path::PathBuf,

    /// Controller address
    #[arg(long, env = "SPUR_CONTROLLER_ADDR", default_value = "http://localhost:6817")]
    controller: String,

    /// Node name (defaults to hostname)
    #[arg(short = 'N', long)]
    hostname: Option<String>,

    /// Foreground mode
    #[arg(short = 'D', long)]
    foreground: bool,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.parse().unwrap()),
        )
        .init();

    let hostname = args.hostname.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into())
    });

    info!(
        version = env!("CARGO_PKG_VERSION"),
        hostname = %hostname,
        controller = %args.controller,
        "spurd starting"
    );

    // Discover local resources
    let resources = reporter::discover_resources();
    info!(
        cpus = resources.cpus,
        memory_mb = resources.memory_mb,
        gpus = resources.gpus.len(),
        "resources discovered"
    );

    // Create the node reporter (handles registration + heartbeat)
    let reporter = Arc::new(NodeReporter::new(
        hostname.clone(),
        args.controller.clone(),
        resources,
    ));

    // Register with controller
    reporter.register().await?;

    // Start heartbeat loop
    let hb_reporter = reporter.clone();
    let hb_handle = tokio::spawn(async move {
        hb_reporter.heartbeat_loop().await;
    });

    // Start job execution listener (polls controller for assigned jobs)
    let exec_reporter = reporter.clone();
    let exec_handle = tokio::spawn(async move {
        executor::job_execution_loop(exec_reporter).await;
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("spurd shutting down");

    hb_handle.abort();
    exec_handle.abort();

    Ok(())
}
