# Hydraria

> A high-performance, low-latency, multi-threaded HTTP streaming proxy written in Rust — with a built-in web dashboard.

Hydraria turns a slow, single-source HTTP download into a parallelized, multi-source pull, then streams the assembled bytes back to any standard HTTP client (browser, VLC, IINA, Aria2, `wget`, …) over a stable short link. The whole thing is one statically linked Rust binary; the dashboard is embedded inside it via `rust-embed`, so there is nothing to deploy on the frontend.

```
[ user / Web UI ] --(1. configure task)--> [ Task Manager ] --(2. mint short link)--> /stream/a1b2c3
                                                                    |
[ player / downloader ] <--(4. single-threaded stream)-- [ Proxy Engine ] <--(3. GET short link)
       |                                                         |
       v                                            (internally: multi-threaded multi-source pull)
 [ receives one stream ]                                  [ origin 1, origin 2, origin 3 ... ]
```

## Highlights

- **Multi-threaded chunked fetching** — each request is split into `max_split`-sized byte ranges and pulled concurrently up to `max_threads`.
- **Multi-source failover** — list multiple origin URLs; chunks are round-robined across them and a failed chunk transparently retries on a different origin.
- **Range / Seek support** — when a player issues `Range: bytes=…` (e.g. seeking forward in a video), Hydraria re-plans the chunks from that offset. No re-download of earlier bytes.
- **Passthrough fallback** — if the origin doesn't advertise `Accept-Ranges: bytes` or doesn't return a `Content-Length`, Hydraria automatically falls back to a single-stream passthrough so unrangeable sources still work.
- **Backpressure-aware streaming** — the chunk planner uses a bounded-channel pipeline (`tokio::sync::mpsc`), so a slow client throttles upstream fetches instead of blowing memory.
- **Custom headers per task** — set `Cookie`, `User-Agent`, `Referer`, etc. once at task creation; every upstream chunk request carries them.
- **Embedded dashboard** — the web UI is compiled into the binary (`rust-embed`); no external static-file directory needed.

## Architecture

| Layer | Module | Responsibility |
| --- | --- | --- |
| Core engine (data plane) | [src/engine.rs](src/engine.rs) | Probe upstream, plan chunks, run the parallel fetcher, serialize chunks back into a single ordered byte stream. |
| Control plane | [src/routes.rs](src/routes.rs), [src/models.rs](src/models.rs) | Task manager, REST API, short-link generation, in-memory task store (`Arc<RwLock<HashMap>>`). |
| Application layer | [src/main.rs](src/main.rs), [src/assets.rs](src/assets.rs) | CLI, axum server, embedded dashboard at `/`. |
| Web UI | [web/index.html](web/index.html) | Single-file dashboard (vanilla JS, no build step). |

## Tech stack

| Concern | Crate |
| --- | --- |
| Async runtime | `tokio` |
| HTTP server / routing | `axum` 0.8 |
| HTTP client | `reqwest` (with streaming + rustls) |
| Embedded static assets | `rust-embed` |
| Concurrency primitives | `tokio::sync::mpsc`, `tokio::sync::Semaphore`, `parking_lot::RwLock` |
| Logging | `tracing` + `tracing-subscriber` |
| CLI | `clap` |

## Build & Run

Requires Rust 1.85+ (edition 2024).

```bash
cargo build --release
./target/release/hydraria --bind 127.0.0.1:9527
```

Then open the dashboard:

```
http://127.0.0.1:9527/
```

Logs go to stdout; control verbosity with `RUST_LOG`, e.g. `RUST_LOG=hydraria=debug,info`.

## API

### Control plane

#### `POST /api/tasks`

Create a new proxy task. Returns the short link.

```bash
curl -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "urls": ["https://server1.com/file.mp4", "https://server2.com/file.mp4"],
    "max_threads": 16,
    "max_split": "5M",
    "cache": false,
    "headers": {
      "User-Agent": "Mozilla/5.0",
      "Cookie": "session=xxxx"
    }
  }'
```

Response:

```json
{ "task_id": "a1b2c3", "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3" }
```

`max_split` accepts either a number of bytes or a human-readable string: `"5M"`, `"512K"`, `"1G"`, etc.

#### `GET /api/tasks`

List all active tasks with stats (bytes served, active connections, config).

#### `GET /api/tasks/:task_id`

Fetch a single task's status.

#### `DELETE /api/tasks/:task_id`

Stop & remove a task. Returns `204`.

### Data plane

#### `GET /stream/:task_id`

The endpoint clients consume. Behaves like a regular HTTP file server:

- Honors `Range: bytes=start-end` (and suffix ranges `bytes=-N`).
- Returns `206 Partial Content` for partial reads, `200 OK` for full reads.
- Forwards `Content-Type`, `ETag`, `Last-Modified`, `Accept-Ranges` from the origin probe.
- Adds `X-Hydraria-Task: <task_id>` for traceability.

`HEAD` is also supported for clients that probe before downloading.

## Example: drop into VLC / IINA / Aria2

```bash
# 1. Create task
TASK=$(curl -s -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{"urls":["https://your-source/file.mp4"],"max_threads":16,"max_split":"5M"}' \
  | sed 's/.*"task_id":"\([^"]*\)".*/\1/')

# 2. Open the short link in any client
vlc        "http://127.0.0.1:9527/stream/$TASK"
aria2c -x16 "http://127.0.0.1:9527/stream/$TASK"
wget        "http://127.0.0.1:9527/stream/$TASK"
```

The client sees a single, plain HTTP/1.1 stream. Hydraria fans the actual fetching out to many parallel range requests across all configured origins.

## How chunked streaming works

1. **Probe** — when a client connects, Hydraria issues a `HEAD` (falling back to a tiny `Range: bytes=0-0` GET) to learn `Content-Length`, `Accept-Ranges`, MIME type, and ETag.
2. **Plan** — given the client's effective range `[start, end]`, the engine slices it into `max_split`-sized sub-ranges.
3. **Pull** — each sub-range is fetched in parallel under a `Semaphore(max_threads)`. Each sub-range gets its own bounded mpsc channel so its bytes can stream as soon as they arrive — no buffering whole chunks in memory.
4. **Stitch** — a serializer task drains the per-chunk channels in plan order and forwards bytes to the client. Because the channels are bounded, a slow client naturally backpressures the fetchers.
5. **Retry** — if a chunk's origin fails mid-stream, the engine retries that chunk on the next URL in the round-robin list.

## Configuration reference

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `urls` | `string[]` | required | Origin URLs. The same content must be available on each. |
| `max_threads` | `int` | `8` | Maximum concurrent chunk fetchers per client connection. |
| `max_split` | `int` or human string | `5M` | Size of each chunk. Smaller = faster failover & seek, more overhead. |
| `cache` | `bool` | `false` | Reserved for future on-disk caching. |
| `headers` | `object<string,string>` | `{}` | Headers to attach to every upstream request. |
| `name` | `string?` | `null` | Optional friendly name shown in the dashboard. |

## Project layout

```
.
├── Cargo.toml
├── src
│   ├── main.rs        # CLI + axum server bootstrap
│   ├── lib.rs         # module roots
│   ├── models.rs      # TaskConfig, TaskStore, AppState, short_id()
│   ├── engine.rs      # multi-threaded chunked fetcher + range parser
│   ├── routes.rs      # axum router for control + data plane
│   ├── assets.rs      # rust-embed-backed static asset handler
│   └── error.rs       # ProxyError + IntoResponse
└── web
    └── index.html     # dashboard (embedded into the binary at compile time)
```

## Roadmap

- On-disk LRU cache for hot byte ranges (the `cache` flag is wired but currently a no-op).
- Persistent task store (SQLite) so tasks survive a restart.
- Per-task bandwidth limits.
- A `download` CLI subcommand that drives the engine directly to a local file.
- Auth / token gating on the control-plane API for non-localhost binds.

## License

MIT (or whatever you choose — adjust before publishing).
