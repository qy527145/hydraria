use clap::Parser;
use hydraria::cache::CacheStore;
use hydraria::models::AppState;
use hydraria::routes::build_router;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "hydraria", about = "Multi-threaded HTTP streaming proxy")]
struct Cli {
    /// Bind address (e.g. 127.0.0.1:9527)
    #[arg(short, long, default_value = "127.0.0.1:9527")]
    bind: String,

    /// Cache directory. Defaults to ~/.hydraria/cache.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

fn default_cache_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".hydraria")
        .join("cache")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,hyper=warn")),
        )
        .init();

    let cli = Cli::parse();

    let cache_dir = cli.cache_dir.clone().unwrap_or_else(default_cache_dir);
    let cache = Arc::new(CacheStore::new(cache_dir.clone())?);

    let addr: SocketAddr = cli.bind.parse()?;
    let state = AppState::new(cli.bind.clone(), cache);
    let app = build_router(state).layer(tower_http::cors::CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Hydraria listening on http://{}", addr);
    tracing::info!("Dashboard:   http://{}/", addr);
    tracing::info!("Cache dir:   {}", cache_dir.display());
    tracing::info!("Create task: POST http://{}/api/tasks", addr);
    tracing::info!("Stream URL:  http://{}/stream/<task_id>", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
