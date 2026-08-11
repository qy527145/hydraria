fn main() {
    // rust-embed needs web/dist to exist at compile time. Creating it keeps
    // `cargo build` working in a fresh clone where the frontend hasn't been
    // built yet; a real build overwrites it.
    let _ = std::fs::create_dir_all("web/dist");
    println!("cargo:rerun-if-changed=web/dist");
}
