//! Plugin system for post-processing the bytes that flow from upstream to the
//! proxy client.
//!
//! A plugin contributes two things:
//!
//! 1. A **transform** applied to bytes streamed from upstream → client. The
//!    transform sees the **merged offset** of each byte being emitted, so it
//!    works uniformly across single-volume and multi-volume tasks. (The user-
//!    facing contract: encryption happens on the unsplit file's coordinate
//!    space, so volumes can be split / merged anywhere as long as the order
//!    is preserved.)
//!
//! 2. A **forward** operation — the inverse transform applied as a one-shot
//!    tool. For the canonical "ChaCha20 encrypt → distribute → decrypt on
//!    proxy" flow this is the encryption step the sender runs locally; the
//!    proxy direction is the matching decryption.
//!
//! ## Composition
//!
//! A task can apply multiple plugins. The plugin list is stored in **forward
//! order** (the order the sender applied them when building the distributed
//! file). On the proxy direction the pipeline applies them in **reverse**
//! order — symmetric ciphers (ChaCha20) are insensitive to direction but a
//! hypothetical compress-then-encrypt chain needs decrypt-then-decompress.
//!
//! ## Cache interaction
//!
//! The on-disk cache stores **raw upstream bytes** (i.e., encrypted bytes for
//! the ChaCha20 case). Transforms are applied on the read-out path, after
//! the cache has been satisfied. This keeps cache keys URL-only and means
//! changing a decryption key never wastes already-fetched data.

pub mod chacha20;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata describing a registered plugin. Sent to the dashboard so the UI
/// can render the right configuration controls without knowing the plugin's
/// implementation.
#[derive(Debug, Clone, Serialize)]
pub struct PluginInfo {
    pub id: String,
    pub name: String,
    pub description: String,
    /// JSON schema-ish field descriptors for the global-config form. The web
    /// UI renders inputs from this list rather than hard-coding the form per
    /// plugin.
    pub global_fields: Vec<ConfigField>,
    /// Same shape, for the per-task config block.
    pub task_fields: Vec<ConfigField>,
    /// Field descriptors for the **forward tool**'s parameter form (only
    /// shown in the plugin's tool card). Plugins without a forward op can
    /// leave this empty and set `has_forward = false`.
    pub forward_fields: Vec<ConfigField>,
    pub has_forward: bool,
    /// Default global config (used to seed the form before the user has
    /// saved anything). Plugins that need no global config return `null`.
    pub default_global: serde_json::Value,
    /// Default per-task config (used when adding the plugin to a task).
    pub default_task: serde_json::Value,
}

/// One form field. `kind` controls how the UI renders it; `key` is the JSON
/// object key under which the value is stored.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigField {
    pub key: String,
    pub label: String,
    pub kind: FieldKind,
    /// Helper text shown beneath the input (e.g., "32 字节 hex, 留空自动生成").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Marked required: the UI's validate step warns on empty.
    #[serde(default)]
    pub required: bool,
    /// When set on a `Hex` field, the UI shows a "随机生成" button next to
    /// it. Click → `crypto.getRandomValues` produces this many random bytes,
    /// hex-encoded, and writes them into the input. Useful for keys/nonces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_random_bytes: Option<u32>,
    /// When set on a `Path` / `DirPath` field with mode `save_file`, the
    /// "选择…" button uses the SaveFile dialog flavor and pre-fills this
    /// name. Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_filename: Option<String>,
    /// `Path` field flavor: `open` (default) or `save`. Controls whether the
    /// native picker is OpenFileDialog or SaveFileDialog. Ignored for
    /// non-Path kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_mode: Option<&'static str>,
    /// Dropdown choices when `kind == Select`. The UI renders one
    /// `<option>` per entry; the value of the selected option goes to the
    /// server as a plain string. Ignored for non-Select kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<SelectOption>>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    /// Plain single-line string.
    Text,
    /// Multi-line string.
    TextArea,
    /// Hex-encoded byte string. UI renders as monospace input.
    Hex,
    /// Local filesystem path to a file. UI renders a "选择文件…" button that
    /// invokes the native open-file dialog (`POST /api/fs/pick`).
    Path,
    /// Local filesystem path to a directory. UI renders a "选择目录…" button
    /// that invokes the native open-folder dialog. Auto-created on use.
    DirPath,
    /// Numeric (u64). UI uses `<input type="number">`.
    Number,
    /// Byte-size string like `5M` / `512K` / `1G`. UI renders a plain text
    /// input but validates against the size grammar before submitting; the
    /// server parses with `models::parse_size`. Stored on the wire as the
    /// original string (so round-tripping preserves the user's preferred
    /// unit); plugins call `parse_size` themselves to get a `u64`.
    Size,
    /// Boolean checkbox.
    Boolean,
    /// Dropdown / `<select>`. The choices live in the field's separate
    /// `options` array (kept off the FieldKind enum so the latter stays
    /// `Copy`). The selected value is sent to the server as a plain string.
    Select,
}

/// One choice in a `Select` field. `value` is what the server receives;
/// `label` is what the user sees in the dropdown.
#[derive(Debug, Clone, Serialize)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
}

/// Result of a forward (sender-side) operation. The plugin reads from
/// `input_path`, writes to `output_path`, and returns this payload describing
/// what it did — including any **freshly-generated secrets** (key, nonce)
/// the user must copy into the receiver-side task config.
#[derive(Debug, Clone, Serialize)]
pub struct ForwardResult {
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// Free-form JSON: e.g. `{ "key": "<hex>", "nonce": "<hex>" }`. The web
    /// UI just dumps this into a copy-friendly preformatted block.
    pub info: serde_json::Value,
    /// Optional human-readable message rendered above the info block (e.g.
    /// "已加密并写入 /tmp/out.enc — 请妥善保存以下密钥").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Byte-level transform applied to outgoing data. Implementations must be
/// random-access: invoking `transform(off, &mut buf)` twice with the same
/// `off` and `buf` must always produce the same result, because the engine
/// may slice the same byte region differently across requests (cache HIT vs
/// cache MISS resumption).
pub trait ByteTransform: Send + Sync {
    /// Mutate `data` in place. `merged_offset` is the byte position of
    /// `data[0]` in the **merged** file (i.e., counting from byte 0 of the
    /// stitched stream, ignoring volume boundaries).
    fn transform(&self, merged_offset: u64, data: &mut [u8]);
}

/// Plugin trait: lifecycle (validate / build transform) + the forward tool.
///
/// `validate_*` returns user-facing error strings — they're surfaced verbatim
/// in the UI, so prefer concise Chinese messages where idiomatic.
pub trait ProxyPlugin: Send + Sync {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    fn description(&self) -> &str;

    /// UI form descriptor for the global config. Keys returned here must
    /// match the JSON keys the plugin reads in `validate_global_config` /
    /// `build_transform` / `forward`.
    fn global_fields(&self) -> Vec<ConfigField> {
        Vec::new()
    }
    fn task_fields(&self) -> Vec<ConfigField> {
        Vec::new()
    }
    fn forward_fields(&self) -> Vec<ConfigField> {
        Vec::new()
    }
    fn has_forward(&self) -> bool {
        false
    }

    fn default_global_config(&self) -> serde_json::Value {
        serde_json::Value::Object(Default::default())
    }
    fn default_task_config(&self) -> serde_json::Value {
        serde_json::Value::Object(Default::default())
    }

    fn validate_global_config(&self, _config: &serde_json::Value) -> Result<(), String> {
        Ok(())
    }
    fn validate_task_config(
        &self,
        _global: &serde_json::Value,
        _task: &serde_json::Value,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Build the per-task byte transform. `global` is whatever the operator
    /// set in the global plugin config; `task` is the task-author's per-task
    /// config (e.g., the decryption key for this file). Implementations are
    /// expected to call `validate_task_config` internally if they need it —
    /// the route handler also pre-validates, so duplicated checks are fine
    /// but not required.
    fn build_transform(
        &self,
        global: &serde_json::Value,
        task: &serde_json::Value,
    ) -> Result<Arc<dyn ByteTransform>, String>;

    /// Run the forward (sender-side) operation. Reads from / writes to local
    /// filesystem paths supplied by `params` — no network upload/download.
    /// Implementations should be blocking-friendly; the route handler wraps
    /// the call in `spawn_blocking`.
    fn forward(
        &self,
        _global: &serde_json::Value,
        _task: &serde_json::Value,
        _params: &serde_json::Value,
    ) -> Result<ForwardResult, String> {
        Err("plugin does not support forward operation".into())
    }
}

/// Serializable view of one plugin slot inside a task config. `enabled =
/// false` keeps the config around for one-click toggle without losing the
/// key/nonce.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskPluginConfig {
    pub id: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Ordered list of transforms applied to outgoing bytes. The pipeline owns
/// nothing but `Arc`s, so it's cheap to clone between the engine and request
/// handlers.
#[derive(Clone, Default)]
pub struct TransformPipeline {
    /// Stored in **forward** order (sender's application order). Applied
    /// in reverse on the proxy direction (cf. `apply_reverse`).
    transforms: Vec<Arc<dyn ByteTransform>>,
}

impl TransformPipeline {
    pub fn new() -> Self {
        Self { transforms: Vec::new() }
    }

    pub fn push(&mut self, t: Arc<dyn ByteTransform>) {
        self.transforms.push(t);
    }

    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }

    pub fn len(&self) -> usize {
        self.transforms.len()
    }

    /// Apply every transform to `data` in reverse-of-stored order — i.e., the
    /// last-applied forward transform is undone first. For a single
    /// symmetric transform (the ChaCha20 case) this is the same as forward
    /// order, so the test surface stays small.
    pub fn apply_reverse(&self, merged_offset: u64, data: &mut [u8]) {
        for t in self.transforms.iter().rev() {
            t.transform(merged_offset, data);
        }
    }
}

/// Process-wide registry of available plugins. Built once at startup and
/// shared via `AppState`. Global plugin configs (separate from the registry)
/// live on `GlobalSettings::plugin_globals` so they persist to disk.
pub struct PluginRegistry {
    by_id: HashMap<String, Arc<dyn ProxyPlugin>>,
    /// Stable iteration order — same order plugins were registered, so the
    /// UI's tool cards render predictably.
    order: RwLock<Vec<String>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
            order: RwLock::new(Vec::new()),
        }
    }

    pub fn register(&mut self, plugin: Arc<dyn ProxyPlugin>) {
        let id = plugin.id().to_string();
        if self.by_id.insert(id.clone(), plugin).is_none() {
            self.order.write().push(id);
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn ProxyPlugin>> {
        self.by_id.get(id).cloned()
    }

    pub fn ids(&self) -> Vec<String> {
        self.order.read().clone()
    }

    /// Build a metadata vec for `/api/plugins`. Includes each plugin's
    /// current global config (from `GlobalSettings`) merged with the plugin's
    /// defaults — so the UI never sees a missing key.
    pub fn info_list(&self, globals: &HashMap<String, serde_json::Value>) -> Vec<PluginInfo> {
        self.order
            .read()
            .iter()
            .filter_map(|id| {
                let p = self.by_id.get(id)?;
                let default_global = p.default_global_config();
                let _ = globals.get(id).cloned().unwrap_or_else(|| default_global.clone());
                Some(PluginInfo {
                    id: p.id().to_string(),
                    name: p.name().to_string(),
                    description: p.description().to_string(),
                    global_fields: p.global_fields(),
                    task_fields: p.task_fields(),
                    forward_fields: p.forward_fields(),
                    has_forward: p.has_forward(),
                    default_global: p.default_global_config(),
                    default_task: p.default_task_config(),
                })
            })
            .collect()
    }

    /// Build the active transform pipeline for a task. Plugins listed on the
    /// task but unknown to the registry are skipped (with a tracing warning),
    /// not errored — so removing a plugin from a future build doesn't break
    /// every persisted task using it.
    pub fn build_pipeline(
        &self,
        plugin_configs: &[TaskPluginConfig],
        globals: &HashMap<String, serde_json::Value>,
    ) -> Result<TransformPipeline, String> {
        let mut pipeline = TransformPipeline::new();
        for pc in plugin_configs {
            if !pc.enabled {
                continue;
            }
            let plugin = match self.by_id.get(&pc.id) {
                Some(p) => p,
                None => {
                    tracing::warn!(
                        "task references unknown plugin id '{}'; skipping",
                        pc.id
                    );
                    continue;
                }
            };
            let global = globals
                .get(&pc.id)
                .cloned()
                .unwrap_or_else(|| plugin.default_global_config());
            let transform = plugin
                .build_transform(&global, &pc.config)
                .map_err(|e| format!("plugin '{}' build failed: {}", pc.id, e))?;
            pipeline.push(transform);
        }
        Ok(pipeline)
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Bundle of built-in plugins. Currently only ChaCha20 — extend by adding
/// new `register(...)` calls here.
pub fn default_registry() -> Arc<PluginRegistry> {
    let mut r = PluginRegistry::new();
    r.register(Arc::new(chacha20::ChaCha20Plugin::default()));
    Arc::new(r)
}
