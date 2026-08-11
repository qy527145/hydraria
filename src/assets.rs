use axum::body::Body;
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

/// The built dashboard (`web/dist`), embedded into the binary at compile time.
/// `build.rs` creates the directory when the frontend hasn't been built yet so
/// a fresh clone still compiles.
#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;

/// Serve the embedded single-page dashboard.
///
/// Unmatched `/api` and `/stream` paths get a JSON 404 instead of the HTML
/// shell — handing back a document there would surface as a confusing JSON
/// parse error in the client. Everything else falls back to `index.html` so a
/// reload on a client-side route still boots the app.
pub async fn static_handler(method: Method, uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.starts_with("api/") || path.starts_with("stream/") {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({ "error": format!("no such endpoint: /{path}") })),
        )
            .into_response();
    }
    if !matches!(method, Method::GET | Method::HEAD) {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let candidate = if path.is_empty() { "index.html" } else { path };
    if let Some(file) = Assets::get(candidate) {
        return serve(candidate, file);
    }
    match Assets::get("index.html") {
        Some(index) => serve("index.html", index),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard not built: run `npm install && npm run build` in web/",
        )
            .into_response(),
    }
}

fn serve(path: &str, file: rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    // Vite fingerprints every asset filename, so those are safe to cache
    // forever; index.html must be revalidated or an upgrade would never be
    // picked up by a browser that already holds the old shell.
    let cache = if path == "index.html" {
        "no-cache"
    } else {
        "public, max-age=31536000, immutable"
    };
    (
        [
            (header::CONTENT_TYPE, mime.as_ref().to_string()),
            (header::CACHE_CONTROL, cache.to_string()),
        ],
        Body::from(file.data.into_owned()),
    )
        .into_response()
}
