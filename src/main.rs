use clap::{Parser, Subcommand};
use hydraria::cache::CacheStore;
use hydraria::engine::Engine;
use hydraria::models::{AppState, GlobalSettings, TaskConfig};
use hydraria::routes::build_router;
use std::collections::HashMap;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "hydraria", about = "Multi-threaded HTTP streaming proxy", version)]
struct Cli {
    /// Bind address (e.g. 127.0.0.1:9527). Used when no subcommand is given
    /// or with the explicit `server` subcommand.
    #[arg(short, long, default_value = "127.0.0.1:9527", global = true)]
    bind: String,

    /// Cache directory. Defaults to ~/.hydraria/cache.
    #[arg(long, global = true)]
    cache_dir: Option<PathBuf>,

    /// Persistence file for tasks + settings. Defaults to ~/.hydraria/tasks.json.
    #[arg(long, global = true)]
    state_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the proxy server (default action when no subcommand is given).
    Server,

    /// Download URL(s) directly to a file using the engine, without starting
    /// the server. URLs are treated as mirrors by default; pass --volumes
    /// to treat them as ordered parts to concatenate.
    Download {
        /// One or more origin URLs.
        urls: Vec<String>,

        /// Output file path. `-` writes to stdout.
        #[arg(short = 'o', long = "output")]
        output: PathBuf,

        /// Extra request header (`-H "Cookie: …"`), repeatable.
        #[arg(short = 'H', long = "header")]
        headers: Vec<String>,

        /// Max concurrent chunk fetchers.
        #[arg(long, default_value_t = 16)]
        threads: usize,

        /// Chunk size, accepts `1M`/`5M`/`512K`/raw bytes.
        #[arg(long, default_value = "5M")]
        split: String,

        /// Treat URLs as ordered volumes (one part each) instead of mirrors.
        #[arg(long)]
        volumes: bool,

        /// Use the shared on-disk cache (resumable across runs).
        #[arg(long)]
        cache: bool,
    },
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

    match cli.command {
        Some(Command::Download {
            urls,
            output,
            headers,
            threads,
            split,
            volumes,
            cache,
        }) => {
            let cache_dir = cli.cache_dir.clone().unwrap_or_else(|| home_subdir("cache"));
            run_download(urls, output, headers, threads, split, volumes, cache, cache_dir).await
        }
        Some(Command::Server) | None => run_server(cli).await,
    }
}

async fn run_server(cli: Cli) -> anyhow::Result<()> {
    let cache_dir = cli.cache_dir.clone().unwrap_or_else(|| home_subdir("cache"));
    let cache = Arc::new(CacheStore::new(cache_dir.clone())?);

    let state_file = cli
        .state_file
        .clone()
        .unwrap_or_else(|| home_subdir("tasks.json"));

    let addr: SocketAddr = cli.bind.parse()?;
    let plugins = hydraria::plugins::default_registry();
    let state = AppState::new(
        cli.bind.clone(),
        cache,
        state_file.clone(),
        GlobalSettings::default(),
        plugins,
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

#[allow(clippy::too_many_arguments)]
async fn run_download(
    urls: Vec<String>,
    output: PathBuf,
    raw_headers: Vec<String>,
    threads: usize,
    split: String,
    volumes_mode: bool,
    use_cache: bool,
    cache_dir: PathBuf,
) -> anyhow::Result<()> {
    if urls.is_empty() {
        anyhow::bail!("at least one URL is required (pass URLs as positional arguments)");
    }

    let headers = parse_header_args(&raw_headers)?;

    // Build a one-off TaskConfig and run it through the same engine the
    // server uses. We construct `volumes` directly from the CLI URL list:
    //   * `--volumes` true  → each URL is its own volume (one mirror each)
    //   * otherwise         → all URLs are mirrors of one volume
    // Then serde validates the rest of the config (notably parsing the
    // `--split` size string into a u64).
    let volumes: Vec<Vec<String>> = if volumes_mode {
        urls.iter().map(|u| vec![u.clone()]).collect()
    } else {
        vec![urls.clone()]
    };
    let cfg_json = serde_json::json!({
        "volumes": volumes,
        "max_threads": threads,
        "max_split": split,
        "cache": use_cache,
        "headers": headers,
        "persist": false,
        "auto_filename": true,
    });
    let mut cfg: TaskConfig = serde_json::from_value(cfg_json)?;
    cfg.normalize();

    let engine = Engine::new(Arc::new(cfg.clone()))?;
    eprintln!("probing {} URL(s)...", cfg.urls().len());
    let probe = engine.probe().await?;

    let total = probe.total_size;
    let etag = probe.etag.clone();
    let filename = probe.filename.clone();
    if let Some(t) = total {
        eprintln!("total size: {} ({})", t, fmt_size(t));
    } else {
        eprintln!("total size: unknown (passthrough mode)");
    }
    if let Some(n) = &filename {
        eprintln!("upstream filename: {}", n);
    }

    // Wire up the engine with cache (if enabled) just like the server does.
    let cache_entry = if use_cache && probe.accepts_ranges {
        if let Some(total) = total {
            let store = CacheStore::new(cache_dir)?;
            let key = hydraria::cache::CacheStore::key_for_task(&cfg);
            let mut url_list = cfg.urls();
            url_list.sort();
            let meta = hydraria::cache::CacheMeta {
                etag: etag.clone(),
                last_modified: probe.last_modified.clone(),
                total_size: total,
                content_type: probe.content_type.clone(),
                block_size: hydraria::cache::BLOCK_SIZE,
                urls: url_list,
            };
            Some(store.open(&key, meta)?)
        } else {
            None
        }
    } else {
        None
    };

    let engine = Arc::new(
        engine
            .with_cache(cache_entry)
            .with_volumes(probe.volumes.clone()),
    );

    let total = match total {
        Some(t) if t > 0 => t,
        _ => anyhow::bail!("cannot download: upstream did not report a total size and passthrough downloads aren't supported via the CLI yet"),
    };

    // Open output destination.
    let stdout_mode = output.as_os_str() == "-";
    let mut sink: Sink = if stdout_mode {
        Sink::Stdout(tokio::io::stdout())
    } else {
        let f = tokio::fs::File::create(&output).await?;
        Sink::File(tokio::io::BufWriter::with_capacity(8 * 1024 * 1024, f))
    };

    let to_tty = !stdout_mode;
    // CLI download mode = "give me the whole file"; treat as bounded — no
    // need for head-zone shrinking (we want max throughput from byte 0).
    let mut rx = engine.stream_range(0, total - 1, false);
    let mut written: u64 = 0;
    let start = std::time::Instant::now();
    let mut last_print = std::time::Instant::now();

    while let Some(item) = rx.recv().await {
        let bytes = item.map_err(|e| anyhow::anyhow!("fetch error: {e}"))?;
        sink.write_all(&bytes).await?;
        written += bytes.len() as u64;
        if to_tty && last_print.elapsed().as_millis() >= 200 {
            print_progress(written, total, start);
            last_print = std::time::Instant::now();
        }
    }
    sink.flush().await?;
    if to_tty {
        print_progress(written, total, start);
        eprintln!();
    }
    eprintln!(
        "done: {} bytes in {:.1}s ({}/s avg)",
        written,
        start.elapsed().as_secs_f64(),
        fmt_size((written as f64 / start.elapsed().as_secs_f64().max(0.001)) as u64),
    );
    Ok(())
}

fn parse_header_args(raw: &[String]) -> anyhow::Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for h in raw {
        let (k, v) = h
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("header must be `Name: value`, got: {h}"))?;
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

fn fmt_size(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.1} {}", v, UNITS[i])
    }
}

fn print_progress(written: u64, total: u64, start: std::time::Instant) {
    let pct = if total > 0 { (written * 100) / total } else { 0 };
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let rate = (written as f64 / elapsed) as u64;
    let bar_width = 30usize;
    let filled = ((pct as usize) * bar_width / 100).min(bar_width);
    let bar: String = std::iter::repeat('=')
        .take(filled)
        .chain(std::iter::repeat(' ').take(bar_width - filled))
        .collect();
    let mut stderr = std::io::stderr().lock();
    let _ = write!(
        stderr,
        "\r[{}] {:>3}% {:>10} / {:<10} {:>10}/s",
        bar,
        pct,
        fmt_size(written),
        fmt_size(total),
        fmt_size(rate),
    );
    let _ = stderr.flush();
}

#[cfg(unix)]
#[allow(dead_code)]
fn atty_stderr() -> bool {
    // Stub kept for parity with Windows; the CLI only checks `to_tty` which
    // is currently always `!stdout_mode`. We could wire this up properly
    // with a crate later if needed.
    true
}
#[cfg(not(unix))]
#[allow(dead_code)]
fn atty_stderr() -> bool {
    true
}

// Tiny enum-based sink so we can write to either a file or stdout without
// pulling in async-trait. Both arms expose the same shape `write_all` +
// `flush`; the `Sink` enum dispatches.
enum Sink {
    File(tokio::io::BufWriter<tokio::fs::File>),
    Stdout(tokio::io::Stdout),
}

impl Sink {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Sink::File(w) => w.write_all(buf).await,
            Sink::Stdout(w) => w.write_all(buf).await,
        }
    }
    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Sink::File(w) => w.flush().await,
            Sink::Stdout(w) => w.flush().await,
        }
    }
}
