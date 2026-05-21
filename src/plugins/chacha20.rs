//! Built-in ChaCha20 plugin.
//!
//! The cipher is RFC 8439 ChaCha20 with a 32-byte key, a 12-byte nonce, and a
//! 32-bit block counter starting at zero. This pair (key, nonce) is the
//! **task-level secret** the receiver pastes in to decrypt; the global
//! config carries only knobs that affect the I/O buffer size for the
//! forward tool.
//!
//! The on-wire / on-disk representation is **raw ciphertext** with no header
//! — i.e., encrypted bytes are byte-for-byte the same size as the plaintext.
//! Splitting an encrypted file into volumes is therefore equivalent to
//! splitting the plaintext: the receiver just configures the volumes in
//! order and the engine's merged-offset tracking does the rest.
//!
//! ## Why merged offset?
//!
//! ChaCha20's keystream is byte-addressable: byte N of the keystream is the
//! same no matter how the ciphertext is sliced or sourced. The user wanted
//! "encrypt-then-split": apply ChaCha20 to the whole logical file, then chop
//! it anywhere. The corollary is that decryption needs to know each
//! ciphertext byte's **position in the un-split logical file** — that's
//! exactly what `merged_offset` tracks in `engine.rs::fetch_*`.

use crate::models::parse_size;
use crate::plugins::{
    ByteTransform, ConfigField, FieldKind, ForwardResult, ProxyPlugin, SelectOption,
};
use chacha20::ChaCha20;
use chacha20::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use rand::{Rng, RngCore};
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const DEFAULT_BUFFER: usize = 64 * 1024;
/// Minimum target size for any non-final volume in split mode. Smaller and
/// you end up with absurd amounts of tiny files; the user's "不要过小" hint.
const MIN_VOLUME_BYTES: u64 = 1024 * 1024;
/// Default suffix template when the user leaves the field blank. `{N}` is
/// replaced with the zero-padded 1-based volume index (width = digits of
/// total volume count).
const DEFAULT_SUFFIX_TEMPLATE: &str = ".part{N}.enc";

#[derive(Default)]
pub struct ChaCha20Plugin;

/// Holds the materialized 32-byte key and 12-byte nonce. Stateless beyond
/// that — `transform` rebuilds the cipher on each call so it's safe to share
/// across threads (no internal counter to race on).
pub struct ChaCha20Transform {
    key: [u8; KEY_LEN],
    nonce: [u8; NONCE_LEN],
}

#[derive(Deserialize, Default)]
struct GlobalCfg {
    /// Buffer size for the streaming forward tool. Accepts either a raw byte
    /// count or a size string like `64K` / `1M`. Larger = fewer read/write
    /// syscalls at the cost of memory. Defaults to 64 KiB; clamped to
    /// [4 KiB, 64 MiB].
    #[serde(default)]
    buffer_size: Option<serde_json::Value>,
}

#[derive(Deserialize, Default)]
struct TaskCfg {
    /// 32-byte hex-encoded encryption key.
    #[serde(default)]
    key: Option<String>,
    /// 12-byte hex-encoded nonce.
    #[serde(default)]
    nonce: Option<String>,
}

/// `random` — historical default: volume sizes uniformly random in
/// `[max/2, max]`. `fixed` — every volume is **exactly** `max_volume_size`
/// except the last one (which gets the remainder, possibly smaller).
#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SplitMode {
    #[default]
    Random,
    Fixed,
}

#[derive(Deserialize, Default)]
struct ForwardParams {
    /// Absolute path to the input plaintext file.
    input_path: String,
    /// Absolute path to the output directory. Created (recursively) if it
    /// doesn't exist.
    output_dir: String,
    /// Filename prefix used as the stem of every output file. Defaults to
    /// the input file's basename (without extension).
    #[serde(default)]
    filename_prefix: Option<String>,
    /// Per-volume suffix template. `{N}` is replaced with the zero-padded
    /// 1-based volume index (pad width = digits of the total volume count).
    /// Defaults to `.part{N}.enc` when encrypting, `.part{N}` when only
    /// splitting (see `effective_suffix_template`).
    #[serde(default)]
    volume_suffix: Option<String>,
    /// `0` / empty / absent → write a single output file. Positive → split
    /// into multiple volumes. Accepts a number or a size string (`5M`,
    /// `512K`, `1G`).
    #[serde(default)]
    max_volume_size: Option<serde_json::Value>,
    /// How to size each volume when `max_volume_size > 0`. See `SplitMode`.
    #[serde(default)]
    split_mode: SplitMode,
    /// When false, the tool degenerates into a plain file splitter — bytes
    /// are written through verbatim (no ChaCha20). The key/nonce fields are
    /// then ignored entirely. Defaults to true: existing callers that send
    /// only `input_path` / `output_dir` keep the encryption behavior they
    /// had before this flag was added.
    #[serde(default = "default_true")]
    encrypt: bool,
    /// When true AND `encrypt` is true, generate a fresh random key if
    /// `task.key` is empty; same for `task.nonce`. Ignored when
    /// `encrypt = false`. Defaults to true — the typical sender workflow is
    /// "give me a fresh secret to share with the receiver".
    #[serde(default = "default_true")]
    generate_missing: bool,
}

fn default_true() -> bool {
    true
}

impl ProxyPlugin for ChaCha20Plugin {
    fn id(&self) -> &str {
        "chacha20"
    }
    fn name(&self) -> &str {
        "ChaCha20 加解密"
    }
    fn description(&self) -> &str {
        "用 ChaCha20 流密码加密源文件后分发,代理时按密钥+nonce 实时解密。\
         分卷顺序保持即可任意分合 (按合并后字节偏移寻址 keystream)。"
    }

    fn global_fields(&self) -> Vec<ConfigField> {
        vec![ConfigField {
            key: "buffer_size".into(),
            label: "正向工具 I/O 缓冲".into(),
            kind: FieldKind::Size,
            hint: Some(
                "加密时一次读写多少字节,影响磁盘吞吐(类似 SSD 块大小);\
                 默认 64K,可用 4K..64M,单位可写 K/M/G"
                    .into(),
            ),
            required: false,
            generate_random_bytes: None,
            default_filename: None,
            path_mode: None,
            options: None,
        }]
    }

    fn task_fields(&self) -> Vec<ConfigField> {
        vec![
            ConfigField {
                key: "key".into(),
                label: "密钥 (32 字节 hex)".into(),
                kind: FieldKind::Hex,
                hint: Some("64 个十六进制字符;可点击右侧🎲随机生成".into()),
                required: true,
                generate_random_bytes: Some(KEY_LEN as u32),
                default_filename: None,
                path_mode: None,
                options: None,
            },
            ConfigField {
                key: "nonce".into(),
                label: "Nonce (12 字节 hex)".into(),
                kind: FieldKind::Hex,
                hint: Some("24 个十六进制字符;与上面的密钥配套,可🎲随机生成".into()),
                required: true,
                generate_random_bytes: Some(NONCE_LEN as u32),
                default_filename: None,
                path_mode: None,
                options: None,
            },
        ]
    }

    fn forward_fields(&self) -> Vec<ConfigField> {
        vec![
            ConfigField {
                key: "input_path".into(),
                label: "输入文件 (明文) 绝对路径".into(),
                kind: FieldKind::Path,
                hint: Some("点击右侧📁选择;或手动粘贴绝对路径".into()),
                required: true,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: Some("open"),
                options: None,
            },
            ConfigField {
                key: "output_dir".into(),
                label: "输出目录".into(),
                kind: FieldKind::DirPath,
                hint: Some("不存在会自动创建".into()),
                required: true,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: None,
            },
            ConfigField {
                key: "filename_prefix".into(),
                label: "文件名前缀".into(),
                kind: FieldKind::Text,
                hint: Some("留空则使用输入文件的基名（无扩展名）".into()),
                required: false,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: None,
            },
            ConfigField {
                key: "volume_suffix".into(),
                label: "分卷后缀模板".into(),
                kind: FieldKind::Text,
                hint: Some(
                    "{N} 会被替换为零填充的卷号;留空时加密模式用 \".part{N}.enc\",\
                     纯分卷用 \".part{N}\""
                        .into(),
                ),
                required: false,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: None,
            },
            ConfigField {
                key: "max_volume_size".into(),
                label: "分卷最大大小".into(),
                kind: FieldKind::Size,
                hint: Some(
                    "留空或 0 = 单文件;支持 5M / 512K / 1G 这种写法"
                        .into(),
                ),
                required: false,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: None,
            },
            ConfigField {
                key: "split_mode".into(),
                label: "分卷策略".into(),
                kind: FieldKind::Select,
                hint: Some(
                    "随机大小适合伪装分发;固定大小最后一卷为余数"
                        .into(),
                ),
                required: false,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: Some(vec![
                    SelectOption {
                        value: "random".into(),
                        label: "随机大小 (默认,最大/2 ~ 最大区间)".into(),
                    },
                    SelectOption {
                        value: "fixed".into(),
                        label: "固定大小 (严格按最大,最后一卷为余数)".into(),
                    },
                ]),
            },
            ConfigField {
                key: "encrypt".into(),
                label: "启用 ChaCha20 加密".into(),
                kind: FieldKind::Boolean,
                hint: Some(
                    "取消勾选 = 仅按规则切分输入文件,不加密 (此时密钥/Nonce 字段被忽略)"
                        .into(),
                ),
                required: false,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: None,
            },
            ConfigField {
                key: "generate_missing".into(),
                label: "自动生成缺失的密钥/Nonce".into(),
                kind: FieldKind::Boolean,
                hint: Some(
                    "勾选后,点击执行前 UI 会客户端随机生成并回填到上方两个字段,\
                     便于复制保存"
                        .into(),
                ),
                required: false,
                generate_random_bytes: None,
                default_filename: None,
                path_mode: None,
                options: None,
            },
        ]
    }

    fn has_forward(&self) -> bool {
        true
    }

    fn default_global_config(&self) -> serde_json::Value {
        serde_json::json!({ "buffer_size": "64K" })
    }

    fn default_task_config(&self) -> serde_json::Value {
        serde_json::json!({ "key": "", "nonce": "" })
    }

    fn validate_global_config(&self, config: &serde_json::Value) -> Result<(), String> {
        let g: GlobalCfg = serde_json::from_value(config.clone())
            .map_err(|e| format!("global config: {e}"))?;
        if let Some(raw) = g.buffer_size {
            let b = coerce_size(&raw, "buffer_size")?;
            if b < 4 * 1024 || b > 64 * 1024 * 1024 {
                return Err("buffer_size must be between 4 KiB and 64 MiB".into());
            }
        }
        Ok(())
    }

    fn validate_task_config(
        &self,
        _global: &serde_json::Value,
        task: &serde_json::Value,
    ) -> Result<(), String> {
        let t: TaskCfg = serde_json::from_value(task.clone())
            .map_err(|e| format!("task config: {e}"))?;
        let _key = parse_key(t.key.as_deref())?;
        let _nonce = parse_nonce(t.nonce.as_deref())?;
        Ok(())
    }

    fn build_transform(
        &self,
        _global: &serde_json::Value,
        task: &serde_json::Value,
    ) -> Result<Arc<dyn ByteTransform>, String> {
        let t: TaskCfg = serde_json::from_value(task.clone())
            .map_err(|e| format!("task config: {e}"))?;
        let key = parse_key(t.key.as_deref())?;
        let nonce = parse_nonce(t.nonce.as_deref())?;
        Ok(Arc::new(ChaCha20Transform { key, nonce }))
    }

    fn forward(
        &self,
        global: &serde_json::Value,
        task: &serde_json::Value,
        params: &serde_json::Value,
    ) -> Result<ForwardResult, String> {
        let g: GlobalCfg = serde_json::from_value(global.clone())
            .map_err(|e| format!("global config: {e}"))?;
        let mut t: TaskCfg = serde_json::from_value(task.clone())
            .map_err(|e| format!("task config: {e}"))?;
        let p: ForwardParams = serde_json::from_value(params.clone())
            .map_err(|e| format!("forward params: {e}"))?;

        // Resolve / generate secrets first — fail fast before any disk IO.
        // Only relevant when actually encrypting; pure-split mode skips this
        // block entirely so the user can run the tool without ever touching
        // a key/nonce field.
        let mut generated_key = false;
        let mut generated_nonce = false;
        let (key_bytes, nonce_bytes) = if p.encrypt {
            let key = match parse_key_opt(t.key.as_deref())? {
                Some(k) => k,
                None => {
                    if !p.generate_missing {
                        return Err(
                            "missing task.key (encryption enabled and generate_missing is false)"
                                .into(),
                        );
                    }
                    let mut k = [0u8; KEY_LEN];
                    rand::thread_rng().fill_bytes(&mut k);
                    generated_key = true;
                    k
                }
            };
            let nonce = match parse_nonce_opt(t.nonce.as_deref())? {
                Some(n) => n,
                None => {
                    if !p.generate_missing {
                        return Err(
                            "missing task.nonce (encryption enabled and generate_missing is false)"
                                .into(),
                        );
                    }
                    let mut n = [0u8; NONCE_LEN];
                    rand::thread_rng().fill_bytes(&mut n);
                    generated_nonce = true;
                    n
                }
            };
            t.key = Some(hex::encode(key));
            t.nonce = Some(hex::encode(nonce));
            (key, nonce)
        } else {
            // Plain split mode — bytes pass through verbatim. The cipher
            // we'd build below is unused, but we still need *something* in
            // these slots to keep the loop uniform; zeroed buffers are
            // never touched because we won't call `apply_keystream`.
            ([0u8; KEY_LEN], [0u8; NONCE_LEN])
        };

        // Buffer size: accepts a number or a size string ("64K"); fall back
        // to DEFAULT_BUFFER on absent/zero/parse-failure (parse_size already
        // errored out via validate_global_config for malformed strings).
        let buffer_size = g
            .buffer_size
            .as_ref()
            .and_then(|v| coerce_size(v, "buffer_size").ok())
            .map(|b| b.clamp(4 * 1024, 64 * 1024 * 1024) as usize)
            .unwrap_or(DEFAULT_BUFFER);

        // max_volume_size: 0 / absent / empty string ⇒ single output. Parse
        // any non-empty value through the same size grammar the rate-limit
        // / split-size fields use elsewhere in the app.
        let max_volume_size = match p.max_volume_size.as_ref() {
            None => 0u64,
            Some(v) => coerce_size(v, "max_volume_size")?,
        };

        // Resolve filenames and output directory.
        let input_path = PathBuf::from(&p.input_path);
        if !input_path.is_file() {
            return Err(format!("input '{}' is not a file", p.input_path));
        }
        let total_size = std::fs::metadata(&input_path)
            .map_err(|e| format!("stat input '{}': {}", p.input_path, e))?
            .len();
        let output_dir = PathBuf::from(p.output_dir.trim());
        if output_dir.as_os_str().is_empty() {
            return Err("output_dir is required".into());
        }
        std::fs::create_dir_all(&output_dir)
            .map_err(|e| format!("mkdir output_dir '{}': {}", output_dir.display(), e))?;

        let prefix = p
            .filename_prefix
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                // Default prefix = input file basename without extension.
                input_path
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "output".to_string())
            });
        // Default suffix differs by mode: encrypted output keeps the `.enc`
        // marker so it's obviously not playable as-is; pure-split keeps the
        // original-looking extension by dropping `.enc`.
        let default_suffix = if p.encrypt {
            DEFAULT_SUFFIX_TEMPLATE
        } else {
            ".part{N}"
        };
        let suffix_template = p
            .volume_suffix
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(default_suffix)
            .to_string();

        // Plan volume sizes. `max_volume_size = 0` → single output.
        // Otherwise the planner is parameterized by `split_mode`:
        //   * Random — sizes uniformly random in [max/2, max], last vol
        //     gets the leftover.
        //   * Fixed  — every non-final vol is exactly `max_volume_size`;
        //     the last vol gets whatever's left (1..=max).
        let plan = plan_volume_sizes(total_size, max_volume_size, p.split_mode);

        // Stream across all volumes. When encrypting we keep one ChaCha20
        // instance and let it advance naturally — the keystream byte at
        // offset N is the same regardless of which output file gets that
        // ciphertext byte, which is the property we need for "split
        // anywhere" decryption. When `encrypt = false`, we skip the cipher
        // entirely and just copy bytes.
        let input_file = File::open(&input_path)
            .map_err(|e| format!("open input '{}': {}", input_path.display(), e))?;
        let mut reader = BufReader::with_capacity(buffer_size, input_file);
        let mut cipher = if p.encrypt {
            Some(ChaCha20::new(&key_bytes.into(), &nonce_bytes.into()))
        } else {
            None
        };
        let pad_width = digit_count(plan.len());
        let mut written: Vec<VolumeOutput> = Vec::with_capacity(plan.len());
        let mut buf = vec![0u8; buffer_size];

        for (idx, vol_size) in plan.iter().enumerate() {
            let suffix = if plan.len() == 1 {
                // Single-output mode: drop `{N}` placeholder when present so
                // the user gets a clean filename like `movie.enc` instead of
                // `movie.part1.enc`.
                strip_volume_marker(&suffix_template)
            } else {
                substitute_volume_marker(&suffix_template, idx + 1, pad_width)
            };
            let path = output_dir.join(format!("{}{}", prefix, suffix));
            let out_file = File::create(&path)
                .map_err(|e| format!("create '{}': {}", path.display(), e))?;
            let mut writer = BufWriter::with_capacity(buffer_size, out_file);

            let mut remaining = *vol_size;
            while remaining > 0 {
                let want = remaining.min(buf.len() as u64) as usize;
                let n = read_exact_or_eof(&mut reader, &mut buf[..want])
                    .map_err(|e| format!("read input: {}", e))?;
                if n == 0 {
                    break;
                }
                if let Some(c) = cipher.as_mut() {
                    c.apply_keystream(&mut buf[..n]);
                }
                writer
                    .write_all(&buf[..n])
                    .map_err(|e| format!("write '{}': {}", path.display(), e))?;
                remaining -= n as u64;
            }
            writer
                .flush()
                .map_err(|e| format!("flush '{}': {}", path.display(), e))?;

            written.push(VolumeOutput {
                path: path.to_string_lossy().into_owned(),
                size: vol_size - remaining,
            });
        }

        // The info payload is intentionally different per mode so the UI's
        // result panel doesn't surface meaningless zeros.
        let info = if p.encrypt {
            serde_json::json!({
                "encrypted": true,
                "key": hex::encode(key_bytes),
                "nonce": hex::encode(nonce_bytes),
                "total_size": total_size,
                "volume_count": written.len(),
                "volumes": written,
                "generated_key": generated_key,
                "generated_nonce": generated_nonce,
            })
        } else {
            serde_json::json!({
                "encrypted": false,
                "total_size": total_size,
                "volume_count": written.len(),
                "volumes": written,
            })
        };
        let bytes_in = total_size;
        let bytes_out = written.iter().map(|v| v.size).sum();
        let message = if !p.encrypt {
            Some(format!(
                "已切分为 {} 个分卷 (未加密)。",
                written.len()
            ))
        } else if generated_key || generated_nonce {
            Some(
                "已加密。新生成的密钥/Nonce 见下方,请立即复制保存,关闭后无法找回。"
                    .to_string(),
            )
        } else {
            Some("已加密。".to_string())
        };
        Ok(ForwardResult {
            bytes_in,
            bytes_out,
            info,
            message,
        })
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct VolumeOutput {
    path: String,
    size: u64,
}

/// Decide volume sizes from the input length. `max == 0` collapses to a
/// single volume covering everything. Otherwise the strategy depends on
/// `mode`:
///   * `Random` — sizes uniformly random in `[max/2, max]` (clamped to ≥
///     MIN_VOLUME_BYTES), last volume gets the leftover.
///   * `Fixed`  — every non-final volume is exactly `max`; the last volume
///     is the remainder (1..=max bytes). The user explicitly opted into
///     "tail can be small" by choosing this mode.
fn plan_volume_sizes(total: u64, max: u64, mode: SplitMode) -> Vec<u64> {
    if max == 0 || total <= max {
        return vec![total];
    }
    match mode {
        SplitMode::Fixed => {
            let mut out = Vec::new();
            let mut remaining = total;
            while remaining > max {
                out.push(max);
                remaining -= max;
            }
            if remaining > 0 {
                out.push(remaining);
            }
            out
        }
        SplitMode::Random => {
            let lower = (max / 2).max(MIN_VOLUME_BYTES.min(max));
            let mut rng = rand::thread_rng();
            let mut out = Vec::new();
            let mut remaining = total;
            while remaining > 0 {
                if remaining <= max {
                    out.push(remaining);
                    break;
                }
                let high = max.min(remaining - 1);
                let low = lower.min(high);
                let pick = rng.gen_range(low..=high);
                out.push(pick);
                remaining -= pick;
            }
            out
        }
    }
}

/// Coerce a JSON value (string with units like "5M", or a raw number) into
/// a byte count. Routes string inputs through `models::parse_size` so the
/// grammar matches `max_split` / rate-limit fields throughout the app.
fn coerce_size(v: &serde_json::Value, field: &str) -> Result<u64, String> {
    match v {
        serde_json::Value::Null => Ok(0),
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| format!("{field}: number must be a non-negative integer")),
        serde_json::Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return Ok(0);
            }
            parse_size(s).map_err(|e| format!("{field}: {e}"))
        }
        other => Err(format!("{field}: unexpected type {:?}", other)),
    }
}

/// Width (in characters) needed to print every 1-based index in [1, n] with
/// equal padding. For n=1 → 1, n=10 → 2, n=100 → 3, etc. Used to build
/// zero-padded `{N}` substitutions.
fn digit_count(n: usize) -> usize {
    if n <= 1 {
        return 1;
    }
    let mut k = n;
    let mut d = 0;
    while k > 0 {
        d += 1;
        k /= 10;
    }
    d
}

/// Replace `{N}` in `tmpl` with a zero-padded `idx`. Other `{...}` runs are
/// left untouched.
fn substitute_volume_marker(tmpl: &str, idx: usize, width: usize) -> String {
    tmpl.replace("{N}", &format!("{:0width$}", idx, width = width))
}

/// Drop the `{N}` placeholder entirely (single-volume mode). Removes
/// trailing dots/separators that the placeholder might have introduced.
fn strip_volume_marker(tmpl: &str) -> String {
    let s = tmpl.replace("{N}", "");
    // Collapse runs of `.` produced by removing the marker (e.g.
    // ".part{N}.enc" → ".part.enc"). Keep at most one dot between non-dot
    // segments so the result looks like a regular extension.
    let mut out = String::with_capacity(s.len());
    let mut last_dot = false;
    for c in s.chars() {
        if c == '.' {
            if last_dot {
                continue;
            }
            last_dot = true;
        } else {
            last_dot = false;
        }
        out.push(c);
    }
    out
}

/// Read up to `buf.len()` bytes, returning the actual count. Treats short
/// reads as success (we keep going until we've filled the buffer or hit
/// EOF) — `Read::read` is allowed to return less than requested.
fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let n = reader.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

impl ByteTransform for ChaCha20Transform {
    fn transform(&self, merged_offset: u64, data: &mut [u8]) {
        if data.is_empty() {
            return;
        }
        // Re-create the cipher per call — keying is microsecond-cheap and
        // the alternative (one stateful cipher per stream) would force us to
        // serialize chunks back into stream order, defeating the engine's
        // parallel chunk fetch. Stateless + seek is the right shape here.
        let mut cipher = ChaCha20::new(&self.key.into(), &self.nonce.into());
        // `try_seek` returns an error if the chained 32-bit block counter
        // would overflow. ChaCha20 supports up to 256 GiB per (key, nonce);
        // we surface this as a silent no-op + tracing warning rather than
        // failing the stream — the encrypted bytes will look like garbage
        // to the client, which is what they'd see anyway with a wrong key.
        if let Err(e) = cipher.try_seek(merged_offset) {
            tracing::warn!(
                "chacha20 seek to offset {} failed: {}; bytes will not be decrypted",
                merged_offset, e
            );
            return;
        }
        cipher.apply_keystream(data);
    }
}

fn parse_key(s: Option<&str>) -> Result<[u8; KEY_LEN], String> {
    parse_key_opt(s)?.ok_or_else(|| "task.key is required (32 bytes hex)".into())
}

fn parse_nonce(s: Option<&str>) -> Result<[u8; NONCE_LEN], String> {
    parse_nonce_opt(s)?.ok_or_else(|| "task.nonce is required (12 bytes hex)".into())
}

fn parse_key_opt(s: Option<&str>) -> Result<Option<[u8; KEY_LEN]>, String> {
    let trimmed = s.map(|s| s.trim()).filter(|s| !s.is_empty());
    let s = match trimmed {
        Some(v) => v,
        None => return Ok(None),
    };
    let bytes = hex::decode(s).map_err(|e| format!("key hex decode: {e}"))?;
    if bytes.len() != KEY_LEN {
        return Err(format!(
            "key must be exactly {} bytes (got {}); expected {} hex chars",
            KEY_LEN,
            bytes.len(),
            KEY_LEN * 2
        ));
    }
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(&bytes);
    Ok(Some(out))
}

fn parse_nonce_opt(s: Option<&str>) -> Result<Option<[u8; NONCE_LEN]>, String> {
    let trimmed = s.map(|s| s.trim()).filter(|s| !s.is_empty());
    let s = match trimmed {
        Some(v) => v,
        None => return Ok(None),
    };
    let bytes = hex::decode(s).map_err(|e| format!("nonce hex decode: {e}"))?;
    if bytes.len() != NONCE_LEN {
        return Err(format!(
            "nonce must be exactly {} bytes (got {}); expected {} hex chars",
            NONCE_LEN,
            bytes.len(),
            NONCE_LEN * 2
        ));
    }
    let mut out = [0u8; NONCE_LEN];
    out.copy_from_slice(&bytes);
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_key() -> String {
        // 32 zero bytes
        "0".repeat(64)
    }
    fn fixed_nonce() -> String {
        "0".repeat(24)
    }

    #[test]
    fn transform_roundtrip_at_offset_zero() {
        let plugin = ChaCha20Plugin;
        let task = serde_json::json!({ "key": fixed_key(), "nonce": fixed_nonce() });
        let t = plugin
            .build_transform(&serde_json::Value::Null, &task)
            .unwrap();
        let mut buf = b"hello world".to_vec();
        let plain = buf.clone();
        t.transform(0, &mut buf);
        assert_ne!(buf, plain, "transform should mutate bytes");
        t.transform(0, &mut buf);
        assert_eq!(buf, plain, "two applications of stream cipher = identity");
    }

    #[test]
    fn transform_seek_offset_is_independent() {
        // Encrypting [a, b, c, d] at offset 0 then decrypting bytes [c, d]
        // by passing offset 2 should recover the original [c, d] — i.e.,
        // every byte's keystream depends only on its merged offset, not on
        // how the data was sliced.
        let plugin = ChaCha20Plugin;
        let task = serde_json::json!({ "key": fixed_key(), "nonce": fixed_nonce() });
        let t = plugin
            .build_transform(&serde_json::Value::Null, &task)
            .unwrap();

        let mut whole = vec![10u8, 20, 30, 40];
        let plain = whole.clone();
        t.transform(0, &mut whole);
        // Slice off the tail (mimicking the "client asked for a sub-range" path).
        let mut tail = whole[2..].to_vec();
        t.transform(2, &mut tail);
        assert_eq!(tail, &plain[2..]);
    }

    #[test]
    fn transform_arbitrary_byte_offset_works() {
        // ChaCha20 seek must be byte-granular even though the underlying
        // block is 64 bytes. Spot-check several mid-block offsets.
        let plugin = ChaCha20Plugin;
        let task = serde_json::json!({ "key": fixed_key(), "nonce": fixed_nonce() });
        let t = plugin
            .build_transform(&serde_json::Value::Null, &task)
            .unwrap();
        // Build a 200-byte plaintext.
        let mut all: Vec<u8> = (0..200u16).map(|i| (i & 0xff) as u8).collect();
        let plain = all.clone();
        t.transform(0, &mut all);
        // Pick three windows that straddle ChaCha20's 64-byte block boundary.
        for (start, end) in [(0usize, 70usize), (60, 130), (130, 200)] {
            let mut slice = all[start..end].to_vec();
            t.transform(start as u64, &mut slice);
            assert_eq!(slice, &plain[start..end], "offset {} window broken", start);
        }
    }

    #[test]
    fn missing_key_errors() {
        let plugin = ChaCha20Plugin;
        let task = serde_json::json!({ "nonce": fixed_nonce() });
        let err = plugin
            .build_transform(&serde_json::Value::Null, &task)
            .err()
            .unwrap();
        assert!(err.contains("key"), "got: {}", err);
    }

    #[test]
    fn wrong_key_length_errors() {
        let plugin = ChaCha20Plugin;
        let task = serde_json::json!({ "key": "abcd", "nonce": fixed_nonce() });
        let err = plugin
            .build_transform(&serde_json::Value::Null, &task)
            .err()
            .unwrap();
        assert!(err.contains("32 bytes"), "got: {}", err);
    }

    #[test]
    fn plan_volume_sizes_single_when_max_zero() {
        assert_eq!(plan_volume_sizes(1024, 0, SplitMode::Random), vec![1024]);
    }

    #[test]
    fn plan_volume_sizes_single_when_total_le_max() {
        assert_eq!(plan_volume_sizes(1024, 4096, SplitMode::Random), vec![1024]);
    }

    #[test]
    fn plan_volume_sizes_random_respects_bounds() {
        // 100 MiB total, 10 MiB max → at least 10 volumes; each non-final
        // volume must be in [5 MiB, 10 MiB]; sum equals total exactly.
        let total = 100 * 1024 * 1024u64;
        let max = 10 * 1024 * 1024u64;
        for _ in 0..50 {
            let plan = plan_volume_sizes(total, max, SplitMode::Random);
            let sum: u64 = plan.iter().sum();
            assert_eq!(sum, total, "plan must cover total exactly: {:?}", plan);
            for (i, &s) in plan.iter().enumerate() {
                assert!(s > 0, "volume {} has zero size: {:?}", i, plan);
                assert!(s <= max, "volume {} exceeds max: {} > {}", i, s, max);
                if i < plan.len() - 1 {
                    assert!(
                        s >= max / 2,
                        "non-final volume {} below lower bound: {} < {}",
                        i, s, max / 2
                    );
                }
            }
        }
    }

    #[test]
    fn plan_volume_sizes_fixed_exact_then_remainder() {
        // 100 bytes, max 30 → 30, 30, 30, 10.
        let plan = plan_volume_sizes(100, 30, SplitMode::Fixed);
        assert_eq!(plan, vec![30, 30, 30, 10]);
    }

    #[test]
    fn plan_volume_sizes_fixed_exact_division() {
        // 100 bytes, max 25 → 25, 25, 25, 25 (no tail).
        let plan = plan_volume_sizes(100, 25, SplitMode::Fixed);
        assert_eq!(plan, vec![25, 25, 25, 25]);
    }

    #[test]
    fn coerce_size_accepts_unit_strings_and_numbers() {
        let v = serde_json::json!("5M");
        assert_eq!(coerce_size(&v, "x").unwrap(), 5 * 1024 * 1024);
        let v = serde_json::json!(1024);
        assert_eq!(coerce_size(&v, "x").unwrap(), 1024);
        let v = serde_json::json!("512K");
        assert_eq!(coerce_size(&v, "x").unwrap(), 512 * 1024);
        let v = serde_json::json!("");
        assert_eq!(coerce_size(&v, "x").unwrap(), 0);
        let v = serde_json::Value::Null;
        assert_eq!(coerce_size(&v, "x").unwrap(), 0);
    }

    #[test]
    fn coerce_size_rejects_garbage() {
        assert!(coerce_size(&serde_json::json!("zzz"), "x").is_err());
        // Negative raw integer ⇒ serde_json can't fit into u64.
        assert!(coerce_size(&serde_json::json!(-1), "x").is_err());
    }

    #[test]
    fn substitute_volume_marker_pads_correctly() {
        assert_eq!(substitute_volume_marker(".part{N}.enc", 1, 2), ".part01.enc");
        assert_eq!(substitute_volume_marker(".part{N}.enc", 12, 2), ".part12.enc");
        assert_eq!(substitute_volume_marker("{N}_blob", 7, 3), "007_blob");
    }

    #[test]
    fn strip_volume_marker_collapses_dots() {
        assert_eq!(strip_volume_marker(".part{N}.enc"), ".part.enc");
        assert_eq!(strip_volume_marker("{N}.bin"), ".bin");
        assert_eq!(strip_volume_marker(".enc"), ".enc");
    }

    #[test]
    fn digit_count_basic() {
        assert_eq!(digit_count(0), 1);
        assert_eq!(digit_count(1), 1);
        assert_eq!(digit_count(9), 1);
        assert_eq!(digit_count(10), 2);
        assert_eq!(digit_count(99), 2);
        assert_eq!(digit_count(100), 3);
        assert_eq!(digit_count(999), 3);
    }
}
