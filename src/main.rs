use clap::Parser;
use hydraria::cache::CacheStore;
use hydraria::models::{AppState, GlobalSettings};
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

    /// Persistence file for tasks + settings. Defaults to ~/.hydraria/tasks.json.
    #[arg(long)]
    state_file: Option<PathBuf>,
}

fn home_subdir(sub: &str) -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".hydraria")
        .join(sub)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,reqwest=warn")),
        )
        .init();

    let cli = Cli::parse();

    let cache_dir = cli.cache_dir.clone().unwrap_or_else(|| home_subdir("cache"));
    let cache = Arc::new(CacheStore::new(cache_dir.clone())?);

    let state_file = cli
        .state_file
        .clone()
        .unwrap_or_else(|| home_subdir("tasks.json"));

    let addr: SocketAddr = cli.bind.parse()?;
    let state = AppState::new(
        cli.bind.clone(),
        cache,
        state_file.clone(),
        GlobalSettings::default(),
    );
    let state = Arc::new(state);

    let restored = state.restore().unwrap_or_else(|e| {
        tracing::warn!("could not restore persisted state ({e}); starting fresh");
        0
    });

    Arc::clone(&state).spawn_background();

    let app = build_router((*state).clone()).layer(tower_http::cors::CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Bind-aware dashboard URL: 0.0.0.0 isn't clickable, so substitute the
    // loopback. The URL is printed on its own line with no leading prefix so
    // terminals (VSCode, Windows Terminal, iTerm, kitty, ...) recognize it
    // as a hyperlink — ctrl+click jumps to the dashboard.
    let dashboard_host = if addr.ip().is_unspecified() {
        format!("127.0.0.1:{}", addr.port())
    } else {
        addr.to_string()
    };
    let dashboard_url = format!("http://{}/", dashboard_host);

    println!();
    println!("  ╭─────────────────────────────────────────────────────────╮");
    println!("  │  Hydraria — multi-threaded HTTP streaming proxy         │");
    println!("  ╰─────────────────────────────────────────────────────────╯");
    println!();
    println!("    Dashboard:");
    println!("    {}", dashboard_url);
    println!();
    println!("    Bind:        {}", addr);
    println!("    Cache dir:   {}", cache_dir.display());
    println!("    State file:  {}", state_file.display());
    if restored > 0 {
        println!("    Restored:    {} task(s) from disk", restored);
    }
    println!();
    println!("    Logs: set RUST_LOG=hydraria=debug (or =trace) for more detail");
    println!();

    axum::serve(listener, app).await?;
    Ok(())
}
