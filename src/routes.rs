use crate::cache::{CacheEntry, CacheMeta, Staging};
use crate::download::{CacheJob, PersistedDownload};
use crate::engine::{Engine, UpstreamProbe, parse_range_header, suggest_volume_filename};
use crate::error::ProxyError;
use crate::fs_pick::{self, PickRequest, PickResponse};
use crate::models::{
    AppState, ConnectionGuard, ContentDispositionMode, GlobalSettingsUpdate, GlobalState,
    TaskConfig, TaskEntry, TaskInfo, TaskUpdate, short_id,
};
use crate::plugins::PluginInfo;
use crate::schedule::Strategy;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Serialize)]
struct CreateResp {
    task_id: String,
    proxy_url: String,
    /// 只有请求里要求了 `start_cache` 时才出现：整文件缓存有没有真的跑起来。
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_started: Option<bool>,
    /// 缓存那一步失败的原因。任务本身已经建好了 —— 分开报，脚本才能区分
    /// 「任务没建成」和「任务建好了但源站现在连不上」。
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_error: Option<String>,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[derive(Deserialize)]
struct ResolveQuery {
    /// 域名、IP，或者干脆整条 URL。
    host: String,
    /// 带上就按这个任务的生效表算（含它自己的任务级映射）。
    #[serde(default)]
    task_id: Option<String>,
}

/// `POST /api/hostmap/resolve` 的请求体。GET 那版只能测**已保存**的规则，于是
/// 「改完 target 再测一次，报的还是上一次的结果」—— 面板里最容易踩的一个坑。
/// 这里允许把编辑器里当前那份规则一起发过来，测的就是屏幕上写着的东西。
#[derive(Deserialize)]
struct ResolveReq {
    host: String,
    #[serde(default)]
    task_id: Option<String>,
    /// 编辑中的规则。`None` = 按已保存的算（等价于 GET）。
    #[serde(default)]
    mappings: Option<Vec<crate::hostmap::HostMapping>>,
    /// `mappings` 替换的是哪一层，见 [`ResolveScope`]。
    #[serde(default)]
    scope: ResolveScope,
}

/// 草稿规则替换哪一层。
#[derive(Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ResolveScope {
    /// 任务级：草稿盖在**当前生效的全局规则**之上，和任务真正跑起来时一样。
    #[default]
    Task,
    /// 全局级：草稿**就是**全部规则。全局设置面板要的是这个 —— 在那儿删掉一条
    /// 规则后再测，结果必须是「没有规则命中」。
    Global,
}

#[derive(Deserialize)]
struct ProbeReq {
    /// Structured volume layout to probe. Each inner Vec is one volume's
    /// mirror URL list. The probe walks every volume, returning the merged
    /// size + a suggested stitched filename.
    #[serde(default)]
    volumes: Vec<Vec<String>>,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
    /// 任务级域名映射。探测走的路径必须和真正播放时一致，否则「探测通过但一播
    /// 就 502」会非常费解。
    #[serde(default)]
    host_mappings: Vec<crate::hostmap::HostMapping>,
}

#[derive(Serialize)]
struct ProbeResp {
    /// Filename detected from an upstream Content-Disposition (or URL path
    /// fallback). `None` if probing produced nothing usable.
    detected_filename: Option<String>,
    /// What the UI should put in the output-filename input by default. For
    /// volume mode this is the longest common prefix of the per-volume
    /// filenames; for mirror mode it's the detected filename.
    suggested_filename: Option<String>,
    total_size: Option<u64>,
    content_type: Option<String>,
    accepts_ranges: bool,
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let status = match &self {
            ProxyError::TaskNotFound(_) => StatusCode::NOT_FOUND,
            ProxyError::NoUpstream => StatusCode::BAD_GATEWAY,
            ProxyError::InvalidRange(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            ProxyError::BadStatus(s) => StatusCode::from_u16(*s).unwrap_or(StatusCode::BAD_GATEWAY),
            // Pass the origin's throttle through, so a client that understands
            // 429/503 can back off too instead of retrying into the same wall.
            ProxyError::Throttled { status, .. } => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
            }
            ProxyError::Upstream(_) => StatusCode::BAD_GATEWAY,
            // The origin replaced the file while we were serving it. There is no
            // coherent response body left to give, and it is the upstream's
            // inconsistency, not the client's request, that is at fault.
            ProxyError::ContentChanged { .. } => StatusCode::BAD_GATEWAY,
            ProxyError::Io(_) | ProxyError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ApiError {
            error: self.to_string(),
        });
        (status, body).into_response()
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/tasks", post(create_task).get(list_tasks))
        .route(
            "/api/tasks/{task_id}",
            get(get_task).patch(patch_task).delete(delete_task),
        )
        .route("/api/tasks/{task_id}/pause", post(pause_task))
        .route("/api/tasks/{task_id}/resume", post(resume_task))
        .route(
            "/api/tasks/{task_id}/cache",
            post(start_task_cache).delete(clear_task_cache),
        )
        .route("/api/tasks/{task_id}/cache/pause", post(pause_task_cache))
        .route("/api/cache", delete(clear_all_cache))
        .route("/api/tasks/{task_id}/export", get(export_task))
        .route("/api/probe", post(probe_urls))
        .route("/api/settings", get(get_settings).put(put_settings))
        .route(
            "/api/hostmap/resolve",
            get(resolve_host).post(resolve_host_draft),
        )
        .route("/api/global", get(get_global))
        .route("/api/plugins", get(list_plugins))
        .route(
            "/api/plugins/{plugin_id}/global",
            get(get_plugin_global).put(put_plugin_global),
        )
        .route("/api/plugins/{plugin_id}/forward", post(plugin_forward))
        .route("/api/fs/pick", post(fs_pick_handler))
        .route("/api/fs/info", get(fs_info))
        .route("/stream/{task_id}", get(stream_task).head(stream_task_head))
        .route("/healthz", get(|| async { "ok" }))
        // Everything else is the embedded dashboard, with an SPA fallback.
        .fallback(crate::assets::static_handler)
        .with_state(state)
}

async fn get_settings(State(state): State<AppState>) -> Json<crate::models::GlobalSettings> {
    Json(state.settings.read().clone())
}

async fn put_settings(
    State(state): State<AppState>,
    Json(upd): Json<GlobalSettingsUpdate>,
) -> Result<Json<crate::models::GlobalSettings>, ProxyError> {
    let s = state.update_settings(upd).map_err(ProxyError::Internal)?;
    Ok(Json(s))
}

/// 诊断：这个 host 到底会被连到哪儿去。面板的「测试」按钮和排查时手动 curl 用的
/// 都是它 —— 光看日志得先复现一次请求，这里可以随时问。
///
/// `task_id` 传了就用那个任务的生效表（全局 ∪ 任务级），不传就只看全局。
/// 想测**还没保存**的规则用 POST，见 [`resolve_host_draft`]。
async fn resolve_host(
    State(state): State<AppState>,
    Query(q): Query<ResolveQuery>,
) -> Result<Json<crate::hostmap::Diagnosis>, ProxyError> {
    let host = probe_host(&q.host)?;
    let table = saved_table(&state, q.task_id.as_deref())?;
    Ok(Json(crate::hostmap::diagnose(&table, &host).await))
}

/// 同上，但可以带一份**编辑中**的规则。
///
/// 这是「改了 target，再测还是旧结果」的解法：那不是缓存，是 GET 那版只认已经
/// 落库的规则 —— 而按下测试的时机，恰恰是规则还没保存的时候。
async fn resolve_host_draft(
    State(state): State<AppState>,
    Json(req): Json<ResolveReq>,
) -> Result<Json<crate::hostmap::Diagnosis>, ProxyError> {
    let host = probe_host(&req.host)?;
    let table = match req.mappings {
        Some(draft) => draft_table(draft, req.scope).map_err(ProxyError::Internal)?,
        None => saved_table(&state, req.task_id.as_deref())?,
    };
    Ok(Json(crate::hostmap::diagnose(&table, &host).await))
}

/// 编辑中的规则 → 一张可以拿来解析的表。
///
/// 两个 scope 的差别就是「草稿是全部规则，还是盖在全局之上的一层」，而这个差别
/// 是有后果的：在全局设置里删掉一条规则再测，必须报「没有规则命中」，用
/// `Task` 的合并语义会把刚删掉的那条从全局又捞回来。
fn draft_table(
    draft: Vec<crate::hostmap::HostMapping>,
    scope: ResolveScope,
) -> Result<Arc<crate::hostmap::HostTable>, String> {
    // 只填了一半的行是表单里刚加出来的，测的人显然不是在问它们；留着只会让
    // build 因为「source must not be empty」整体失败。
    let draft: Vec<_> = draft
        .into_iter()
        .filter(|m| !m.from.trim().is_empty() && !m.to.trim().is_empty())
        .collect();
    let rules = match scope {
        ResolveScope::Task => crate::hostmap::merged_rules(&draft),
        ResolveScope::Global => draft,
    };
    // 规则本身写错时，这条错误正是用户要的答案，原样报回去。
    crate::hostmap::HostTable::build(&rules).map(Arc::new)
}

/// 已保存的生效表：给了任务就是「全局 ∪ 该任务」，否则只有全局那张。
fn saved_table(
    state: &AppState,
    task_id: Option<&str>,
) -> Result<Arc<crate::hostmap::HostTable>, ProxyError> {
    match task_id {
        Some(id) => {
            let entry = state
                .tasks
                .read()
                .get(id)
                .cloned()
                .ok_or_else(|| ProxyError::Internal(format!("no such task: {id}")))?;
            let rules = entry.config.read().host_mappings.clone();
            Ok(crate::hostmap::effective_for(&rules)
                .map_err(ProxyError::Internal)?
                .table)
        }
        None => Ok(crate::hostmap::global_table()),
    }
}

/// 顺手接受整条 URL —— 排查的时候手里有的通常是 URL，不是光秃秃一个域名。
fn probe_host(raw: &str) -> Result<String, ProxyError> {
    let host = raw.trim();
    if host.is_empty() {
        return Err(ProxyError::Internal("host must not be empty".into()));
    }
    Ok(reqwest::Url::parse(host)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_else(|| host.to_string()))
}

async fn get_global(State(state): State<AppState>) -> Json<GlobalState> {
    Json(state.global_state())
}

/// `POST /api/tasks` —— 建任务，返回代理短链。
///
/// 请求体就是一个 `TaskConfig`，但对**脚本**额外放宽了 URL 的写法：`volumes`
/// 是二维的（分卷 × 镜像），而绝大多数脚本要下发的只是「一个文件、一到几个
/// 镜像」，为此多写一层方括号（更常见的是写错）不值得。所以下面这些全都认，
/// 语义与 aria2 的 `addUri`、Motrix、Gopeed 一致：
///
/// ```jsonc
/// {"url":  "https://a/f.mp4"}                       // 一卷一镜像
/// {"urls": ["https://a/f.mp4", "https://b/f.mp4"]}  // 一卷两镜像（同一个文件）
/// {"volumes": [["https://a/p1"], ["https://a/p2"]]} // 两卷（顺序拼接）
/// ```
///
/// `uri` / `uris` 是 aria2 的叫法，一并接受。其余字段缺省即可 —— 服务端会填上
/// 和面板新建任务时一样的默认值。
///
/// `start_cache: true`（或 `?start_cache=1`）表示建完立刻开始把整个文件拉进
/// 缓存，也就是 aria2 那种「加进来就开始下」。它会失败（源站连不上、不支持
/// Range），但那不该让创建也跟着失败：任务已经建好了，短链是有效的，所以缓存
/// 的结果单独放在 `cache_started` / `cache_error` 里报。
async fn create_task(
    State(state): State<AppState>,
    Query(q): Query<CreateQuery>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<CreateResp>, ProxyError> {
    let mut body = match body {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(ProxyError::Internal(
                "request body must be a JSON object".into(),
            ));
        }
    };
    coerce_url_aliases(&mut body).map_err(ProxyError::Internal)?;
    let start_cache = q.start_cache()
        || matches!(
            body.remove("start_cache"),
            Some(serde_json::Value::Bool(true))
        );
    reject_unknown_fields(&body).map_err(ProxyError::Internal)?;

    let mut cfg: TaskConfig = serde_json::from_value(serde_json::Value::Object(body))
        .map_err(|e| ProxyError::Internal(format!("invalid task config: {e}")))?;
    cfg.normalize();
    if cfg.volumes.is_empty() {
        return Err(ProxyError::Internal(
            "at least one URL is required — pass \"url\", \"urls\" or \"volumes\"".into(),
        ));
    }
    cfg.validate().map_err(ProxyError::Internal)?;
    validate_task_plugins(&state, &cfg.plugins).map_err(ProxyError::Internal)?;
    let id = {
        let mut tries = 0;
        loop {
            let candidate = short_id();
            if !state.tasks.read().contains_key(&candidate) {
                break candidate;
            }
            tries += 1;
            if tries > 5 {
                break short_id();
            }
        }
    };
    let entry = Arc::new(TaskEntry::new(cfg));
    state.insert(id.clone(), Arc::clone(&entry));

    let (cache_started, cache_error) = if start_cache {
        match ensure_cache_job(&state, &id, &entry).await {
            Ok(job) => {
                job.start_cache();
                (Some(true), None)
            }
            Err(e) => (Some(false), Some(e.to_string())),
        }
    } else {
        (None, None)
    };
    Ok(Json(CreateResp {
        proxy_url: format!("http://{}/stream/{}", state.bind_addr, id),
        task_id: id,
        cache_started,
        cache_error,
    }))
}

#[derive(Deserialize)]
struct CreateQuery {
    /// 建完立刻开始整文件缓存。和请求体里的同名字段等价，两个给了任一个就算数。
    ///
    /// 收字符串再自己判真假，而不是让 serde 直接反序列化成 `bool`：那样只认
    /// `true` / `false`，而 `?start_cache=1` 是查询串里更常见的写法，报一句
    /// 「provided string was not `true` or `false`」纯属为难人。
    #[serde(default)]
    start_cache: Option<String>,
}

impl CreateQuery {
    fn start_cache(&self) -> bool {
        match self.start_cache.as_deref() {
            // `?start_cache`（没有等号）解出来是空串，写的人显然是想要它。
            Some(v) => !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            ),
            None => false,
        }
    }
}

/// 请求体里允许出现的字段：`TaskConfig` 的全部字段，加上创建 / 更新时额外接受的
/// 那几个别名与开关。
///
/// 和 `TaskConfig` 的定义放在一起看才有意义 —— 加字段时这里也要加，否则新字段会
/// 被下面的检查拒掉（测试 `every_task_config_field_is_reachable_through_the_api`
/// 会立刻发现）。
const ACCEPTED_TASK_FIELDS: &[&str] = &[
    // TaskConfig 本体
    "volumes",
    "max_threads",
    "max_per_volume",
    "max_split",
    "cache",
    "headers",
    "name",
    "output_filename",
    "auto_filename",
    "rate_limit_bps",
    "rate_limit_algorithm",
    "persist",
    "plugins",
    "content_disposition",
    "host_mappings",
    // 创建 / 更新时的便捷写法（`coerce_url_aliases` 会先把它们吸收掉，
    // 这里列出来只是为了错误信息里能提到它们）
    "url",
    "urls",
    "uri",
    "uris",
    "start_cache",
];

/// 未知字段直接报错，而不是静默忽略。
///
/// 静默忽略是脚本作者最难查的一类问题：`max_treads` 打错一个字母，请求返回 200，
/// 任务却在用默认值跑，而错误信息一个字都没有。控制面接口宁可吵一点。
///
/// 只在这两个入口检查，不用 `#[serde(deny_unknown_fields)]`：同一个 `TaskConfig`
/// 还要用来读磁盘上的持久化文件，那条路必须宽容 —— 用旧版本的状态文件启动新版本
/// （或反之）不该因为多一个字段就整个启动失败。
fn reject_unknown_fields(body: &serde_json::Map<String, serde_json::Value>) -> Result<(), String> {
    reject_unknown(body, ACCEPTED_TASK_FIELDS)
}

/// PATCH 版本：`start_cache` 在这里不作数（它只对创建有意义），所以也算未知字段。
/// 静默吃掉它的后果是「PATCH 里写了 start_cache，缓存却没动」。
fn reject_unknown_fields_for_update(
    body: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let accepted: Vec<&str> = ACCEPTED_TASK_FIELDS
        .iter()
        .copied()
        .filter(|k| *k != "start_cache")
        .collect();
    reject_unknown(body, &accepted)
}

fn reject_unknown(
    body: &serde_json::Map<String, serde_json::Value>,
    accepted: &[&str],
) -> Result<(), String> {
    let unknown: Vec<&str> = body
        .keys()
        .map(String::as_str)
        .filter(|k| !accepted.contains(k))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(format!(
        "unknown field(s): {} — accepted fields are: {}",
        unknown.join(", "),
        accepted.join(", "),
    ))
}

/// 建任务 / 改任务时先把插件配置过一遍插件自己的校验。
///
/// 见 [`crate::plugins::PluginRegistry::validate_task_configs`]：不做的话，一条写
/// 错的密钥要等到有人来播才炸成 500，而脚本早就拿着 200 走了。
fn validate_task_plugins(
    state: &AppState,
    plugins: &[crate::plugins::TaskPluginConfig],
) -> Result<(), String> {
    if plugins.is_empty() {
        return Ok(());
    }
    let globals = state.settings.read().plugin_globals.clone();
    state.plugins.validate_task_configs(plugins, &globals)
}

/// URL 字段的别名归一：把 `url` / `urls` / `uri` / `uris` 和扁平写法的 `volumes`
/// 统统收敛成 `volumes: [[…]]` 这一种形状。
///
/// 返回值表示「这次请求到底提没提 URL」—— PATCH 要用它来区分「改成这些 URL」
/// 和「这次不动 URL」，后者绝不能被写成一个空数组（那会把任务的源清空）。
fn coerce_url_aliases(
    body: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    const ALIASES: [&str; 4] = ["urls", "url", "uris", "uri"];
    let mut volumes = match body.get("volumes") {
        Some(v) => Some(volumes_from_value(v, "volumes")?),
        None => None,
    };
    for key in ALIASES {
        let Some(value) = body.remove(key) else {
            continue;
        };
        let parsed = volumes_from_value(&value, key)?;
        // `volumes` 显式给了就以它为准，别名只在它缺席（或为空）时补位 ——
        // 两个都写了通常是复制粘贴的残留，静默合并只会得到一个谁也没想要的任务。
        if volumes
            .as_ref()
            .is_none_or(|v: &Vec<Vec<String>>| v.is_empty())
            && !parsed.is_empty()
        {
            volumes = Some(parsed);
        }
    }
    match volumes {
        Some(v) => {
            body.insert(
                "volumes".into(),
                serde_json::to_value(v).map_err(|e| e.to_string())?,
            );
            Ok(true)
        }
        None => Ok(false),
    }
}

/// 一个 URL 字段能长成的所有样子 → 规范的分卷布局。
///
/// * `"https://a/f"`                → 一卷一镜像
/// * `["https://a/f", "https://b/f"]` → 一卷两镜像（**镜像**，不是两卷 ——
///   和 aria2 `addUri` 收一组 URI 的含义一致）
/// * `[["…"], ["…"]]`               → 两卷，顺序拼接
///
/// 混着写（既有字符串又有数组）是拒绝而不是猜：猜错的代价是一个看起来建成了、
/// 播出来却是错的任务。
fn volumes_from_value(value: &serde_json::Value, field: &str) -> Result<Vec<Vec<String>>, String> {
    use serde_json::Value;
    let as_url = |v: &Value| -> Result<String, String> {
        v.as_str()
            .map(|s| s.trim().to_string())
            .ok_or_else(|| format!("'{field}' must contain URL strings"))
    };
    match value {
        Value::Null => Ok(Vec::new()),
        Value::String(s) => Ok(vec![vec![s.trim().to_string()]]),
        Value::Array(items) if items.is_empty() => Ok(Vec::new()),
        Value::Array(items) => {
            if items.iter().all(|i| i.is_array()) {
                items
                    .iter()
                    .map(|inner| {
                        inner
                            .as_array()
                            .expect("checked above")
                            .iter()
                            .map(as_url)
                            .collect()
                    })
                    .collect()
            } else if items.iter().any(|i| i.is_array()) {
                Err(format!(
                    "'{field}' mixes URL strings and volume arrays — use either \
                     [\"url\", …] for mirrors of one file or [[\"url\"], …] for volumes"
                ))
            } else {
                Ok(vec![items.iter().map(as_url).collect::<Result<_, _>>()?])
            }
        }
        _ => Err(format!(
            "'{field}' must be a URL string, a list of URLs, or a list of volumes"
        )),
    }
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<TaskInfo>> {
    Json(state.list())
}

/// Cheap one-shot probe used by the create/edit form to preview the detected
/// filename and metadata before the task exists. Builds a throwaway `Engine`
/// with the supplied URLs/volumes/headers, runs the same probe path the
/// streamer would, then derives a "suggested" filename (LCP across volumes).
///
/// URL 字段认与建任务相同的那批别名（`url` / `urls` / `uri` / `uris`）—— 「先探测
/// 一下再决定要不要建」是脚本里很自然的一步，没道理在这里换一种写法。
async fn probe_urls(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<ProbeResp>, ProxyError> {
    let mut body = match body {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(ProxyError::Internal(
                "request body must be a JSON object".into(),
            ));
        }
    };
    coerce_url_aliases(&mut body).map_err(ProxyError::Internal)?;
    let req: ProbeReq = serde_json::from_value(serde_json::Value::Object(body))
        .map_err(|e| ProxyError::Internal(format!("invalid probe request: {e}")))?;
    if req.volumes.iter().all(|v| v.is_empty()) {
        return Err(ProxyError::Internal(
            "at least one URL is required — pass \"url\", \"urls\" or \"volumes\"".into(),
        ));
    }
    let mut cfg = TaskConfig {
        volumes: req.volumes.clone(),
        max_threads: 1,
        max_per_volume: 1,
        max_split: 5 * 1024 * 1024,
        cache: false,
        headers: req.headers.unwrap_or_default(),
        name: None,
        output_filename: None,
        auto_filename: true,
        rate_limit_bps: 0,
        rate_limit_algorithm: Default::default(),
        persist: false,
        plugins: Vec::new(),
        content_disposition: Default::default(),
        host_mappings: req.host_mappings.clone(),
    };
    cfg.normalize();
    cfg.validate_host_mappings().map_err(ProxyError::Internal)?;
    let layout = cfg.effective_volumes();
    let engine = Engine::new(Arc::new(cfg), state.upstream.clone());
    let probe = engine.probe().await?;

    // Suggested filename for the UI: when there are 2+ volumes, take the
    // longest common prefix of the per-volume filenames (the parts are
    // usually named "movie.part01", "movie.part02", … so the LCP is the
    // unsuffixed name the user wants). For one volume — i.e. the historic
    // mirror case — just use what the upstream advertised.
    let suggested = if layout.len() > 1 {
        // One representative URL per volume — pick the first mirror.
        let representatives: Vec<String> =
            layout.iter().filter_map(|v| v.first().cloned()).collect();
        suggest_volume_filename(&representatives).or_else(|| probe.filename.clone())
    } else {
        probe.filename.clone()
    };

    Ok(Json(ProbeResp {
        detected_filename: probe.filename,
        suggested_filename: suggested,
        total_size: probe.total_size,
        content_type: probe.content_type,
        accepts_ranges: probe.accepts_ranges,
    }))
}

async fn get_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    Ok(Json(state.task_info(&task_id, &entry)))
}

/// `PATCH /api/tasks/:id` —— 部分更新，只动请求里出现过的字段。
///
/// URL 字段和创建时认同一批别名（`url` / `urls` / `uri` / `uris` / `volumes`），
/// 所以「签名过期了，把地址换掉」在脚本里就是一行 `{"url": "…"}`。没提到 URL
/// 的 PATCH 不会碰任务的源列表。
async fn patch_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let mut body = match body {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(ProxyError::Internal(
                "request body must be a JSON object".into(),
            ));
        }
    };
    coerce_url_aliases(&mut body).map_err(ProxyError::Internal)?;
    // `start_cache` 只对创建有意义；PATCH 里出现它多半是复制了创建时的请求体，
    // 与其静默忽略，不如让它走下面的未知字段检查报出来。
    reject_unknown_fields_for_update(&body).map_err(ProxyError::Internal)?;
    let update: TaskUpdate = serde_json::from_value(serde_json::Value::Object(body))
        .map_err(|e| ProxyError::Internal(format!("invalid task update: {e}")))?;
    if let Some(plugins) = update.plugins.as_deref() {
        validate_task_plugins(&state, plugins).map_err(ProxyError::Internal)?;
    }
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    // Snapshot the cache key before the URL list changes so we can migrate
    // the on-disk entry if the user just rotated a signed link.
    let old_cache_key = crate::cache::CacheStore::key_for_task(&entry.config_snapshot());
    entry.apply_update(update).map_err(ProxyError::Internal)?;
    let new_cache_key = crate::cache::CacheStore::key_for_task(&entry.config_snapshot());
    if old_cache_key != new_cache_key {
        match state.cache.migrate_key(&old_cache_key, &new_cache_key) {
            Ok(true) => tracing::info!(
                "cache migrated for task {}: {} -> {}",
                task_id,
                old_cache_key,
                new_cache_key,
            ),
            Ok(false) => {}
            Err(e) => tracing::warn!(
                "cache migration failed for task {} ({} -> {}): {}",
                task_id,
                old_cache_key,
                new_cache_key,
                e,
            ),
        }
    }
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn delete_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<StatusCode, ProxyError> {
    state
        .remove(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    // The task is gone, so its coordinator must stop fetching rather than run on
    // as an orphan. Bytes already on disk stay: the cache is keyed by URL, not by
    // task id, so recreating the task resumes from them.
    if let Some(job) = state.downloads.remove(&task_id) {
        job.stop();
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn pause_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    entry.paused.store(true, Ordering::Relaxed);
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn resume_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    entry.paused.store(false, Ordering::Relaxed);
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn clear_task_cache(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<StatusCode, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    let cfg = entry.config_snapshot();
    let key = crate::cache::CacheStore::key_for_task(&cfg);
    // Tasks with the same URLs share one entry, so every coordinator on this key
    // has to let go before the directory disappears underneath it.
    state.downloads.stop_for_key(&key);
    state.cache.clear(&key)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn clear_all_cache(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ProxyError> {
    // Same reason as `clear_task_cache`: nothing may still be writing into a
    // directory that is about to be removed.
    state.downloads.stop_all();
    let freed = state.cache.clear_all()?;
    Ok(Json(serde_json::json!({ "bytes_freed": freed })))
}

/// Export a task's config as a downloadable JSON file. The body is a
/// `TaskConfig` (no runtime stats), so POSTing it back to `/api/tasks` on
/// any instance recreates the same task — round-trip portable.
async fn export_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Response, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    let cfg = entry.config_snapshot();
    let body = serde_json::to_vec_pretty(&cfg).map_err(|e| ProxyError::Internal(e.to_string()))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let disp = format!("attachment; filename=\"hydraria-task-{}.json\"", task_id);
    if let Ok(v) = HeaderValue::from_str(&disp) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    let mut resp = Response::new(Body::from(body));
    *resp.headers_mut() = headers;
    Ok(resp)
}

async fn stream_task_head(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Response, ProxyError> {
    handle_stream(state, task_id, None, true).await
}

async fn stream_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ProxyError> {
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    handle_stream(state, task_id, range, false).await
}

/// Serve the proxy stream. **Playback only** — latency-first scheduling that
/// prioritizes the bytes just past the client's Range start, because that is
/// what a player is blocked on.
///
/// Bulk downloads deliberately do *not* come through here. An HTTP response body
/// is an ordered byte stream, so a download served this way is hostage to
/// whichever request sits at the read head; `POST /api/tasks/:id/download` runs
/// a server-side job instead, writing out of order straight to a file.
async fn handle_stream(
    state: AppState,
    task_id: String,
    range_header: Option<String>,
    head_only: bool,
) -> Result<Response, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;

    tracing::info!(
        "stream start task={} range={} head_only={}",
        task_id,
        range_header.as_deref().unwrap_or("none"),
        head_only,
    );

    if entry.paused.load(Ordering::Relaxed) {
        tracing::debug!("stream task={} paused, returning 503", task_id);
        let body = Json(ApiError {
            error: format!("task {} is paused", task_id),
        });
        return Ok((StatusCode::SERVICE_UNAVAILABLE, body).into_response());
    }

    let (cfg, probe, engine) = prepare_engine(&state, &task_id, &entry).await?;

    let shared_cache = if !head_only && (cfg.cache || state.downloads.get(&task_id).is_some()) {
        Some(ensure_cache_job(&state, &task_id, &entry).await?)
    } else {
        None
    };
    // Acquire the staging substrate for the legacy per-stream path only when a
    // task-level cache coordinator is not already serving this content.
    let staging: Option<Staging> = if head_only || shared_cache.is_some() {
        None
    } else {
        match resolve_staging(&state, &cfg, &probe) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    "staging unavailable for task {} ({}); falling back to the \
                     in-memory ordered path",
                    task_id,
                    e,
                );
                None
            }
        }
    };
    // The in-memory fallback still benefits from a *persistent* cache entry;
    // an ephemeral scratch file is only meaningful to the staged path.
    let cache_entry: Option<Arc<CacheEntry>> = staging
        .as_ref()
        .filter(|s| !s.is_ephemeral())
        .map(|s| Arc::clone(s.entry()));
    tracing::debug!(
        "stream task={} staging={} mode={}",
        task_id,
        match &staging {
            Some(s) if s.is_ephemeral() => "ephemeral",
            Some(_) => "cache",
            None => "none",
        },
        if probe.accepts_ranges && probe.total_size.is_some() {
            "ranged"
        } else {
            "passthrough"
        },
    );

    let engine = Arc::new(engine.with_cache(cache_entry.clone()));

    build_stream_response(
        state,
        task_id,
        entry,
        engine,
        probe,
        range_header,
        head_only,
        cfg.content_disposition,
        staging,
        shared_cache,
        Strategy::latency_default(),
    )
    .await
}

/// Probe a task's upstream and assemble a fully-wired `Engine` for it: health
/// accumulators, volume layout, and the plugin transform pipeline.
///
/// Shared by the playback stream and the download API — both need the same
/// probe (which is cached per task, since players re-probe on every seek) and
/// the same engine, and neither should be duplicating this. The returned engine
/// deliberately has **no** cache/staging attached; that is the caller's choice.
async fn prepare_engine(
    state: &AppState,
    task_id: &str,
    entry: &Arc<TaskEntry>,
) -> Result<(Arc<TaskConfig>, UpstreamProbe, Engine), ProxyError> {
    let cfg = Arc::new(entry.config_snapshot());
    let engine = Engine::new(Arc::clone(&cfg), state.upstream.clone())
        .with_head_unsupported(Arc::clone(&entry.head_unsupported))
        .with_claim_wall(Arc::clone(&entry.claim_wall));

    // Probe is expensive on multi-volume tasks (N × HEAD + N × Range:0-0
    // round-trips) and players like PotPlayer open a fresh connection for
    // every seek. Cache the result on the TaskEntry for `PROBE_CACHE_TTL`
    // so subsequent seeks reuse the layout instead of re-probing every
    // volume. `apply_update` clears this cache whenever the volumes or
    // request headers change.
    const PROBE_CACHE_TTL: Duration = Duration::from_secs(300);
    let read_cached = || -> Option<Arc<UpstreamProbe>> {
        let guard = entry.probe_cache.lock();
        guard
            .as_ref()
            .filter(|(_, t)| t.elapsed() < PROBE_CACHE_TTL)
            .map(|(p, _)| Arc::clone(p))
    };
    let mut probe = if let Some(p) = read_cached() {
        tracing::debug!(
            "probe cache HIT task={} (age<{}s)",
            task_id,
            PROBE_CACHE_TTL.as_secs()
        );
        UpstreamProbe::clone(&p)
    } else {
        // Singleflight: serialize concurrent first-time probes so they don't
        // all hammer the upstream in parallel. The second caller will block
        // here briefly, then re-check the cache and find the freshly stored
        // result instead of starting its own probe.
        let _guard = entry.probe_inflight.lock().await;
        if let Some(p) = read_cached() {
            tracing::debug!("probe cache HIT task={} (after inflight wait)", task_id);
            UpstreamProbe::clone(&p)
        } else {
            let probe_t0 = Instant::now();
            let fresh = engine.probe().await?;
            tracing::info!(
                "probe ok task={} total={} vols={} accepts_ranges={} etag={:?} unreachable={:?} ({}ms)",
                task_id,
                fresh
                    .total_size
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                fresh.volumes.as_ref().map(|v| v.len()).unwrap_or(0),
                fresh.accepts_ranges,
                fresh.etag,
                fresh.probe_error,
                probe_t0.elapsed().as_millis(),
            );
            *entry.probe_cache.lock() = Some((Arc::new(fresh.clone()), Instant::now()));
            fresh
        }
    };

    // Resolve which filename to advertise. Precedence:
    //   auto_filename=true  → probe result → output_filename → name → URL guess
    //   auto_filename=false → output_filename (None ⇒ no Content-Disposition)
    probe.filename = resolve_served_filename(&cfg, probe.filename.take());

    // Plugin pipeline active? Then the upstream's Content-Type describes
    // CIPHERTEXT (typically `application/octet-stream` from a CDN), not the
    // payload we're about to serve. Override using the served filename's
    // extension when that produces something more specific — otherwise
    // <video>/<audio> tags happily download but refuse to render. The
    // `Inline` content-disposition mode applies the same override even when
    // no plugin is active — user explicitly asked for preview, so we lift
    // the upstream's generic MIME the same way.
    let force_mime_from_filename = cfg.plugins.iter().any(|p| p.enabled)
        || cfg.content_disposition == ContentDispositionMode::Inline;
    if force_mime_from_filename {
        if let Some(name) = &probe.filename {
            let guessed = mime_guess::from_path(name).first_or_octet_stream();
            let g = guessed.essence_str();
            // Override only when the upstream's CT was missing or generic —
            // a real `video/mp4` from the origin should not be clobbered.
            let upstream_is_generic = probe
                .content_type
                .as_deref()
                .map(|ct| ct.eq_ignore_ascii_case("application/octet-stream"))
                .unwrap_or(true);
            if g != "application/octet-stream"
                && upstream_is_generic
                && probe.content_type.as_deref() != Some(g)
            {
                tracing::debug!(
                    "task={} forcing Content-Type from upstream {:?} → {} (from filename '{}')",
                    task_id,
                    probe.content_type,
                    g,
                    name,
                );
                probe.content_type = Some(g.to_string());
            }
        }
    }

    let health = entry.url_health.read().iter().cloned().collect::<Vec<_>>();
    // Build the plugin transform pipeline from the task's enabled plugins
    // plus the global per-plugin config. A build failure here (e.g. wrong
    // key length) becomes a 5xx so the user notices immediately rather
    // than getting silently garbled bytes.
    let pipeline = {
        let globals = state.settings.read().plugin_globals.clone();
        state
            .plugins
            .build_pipeline(&cfg.plugins, &globals)
            .map_err(ProxyError::Internal)?
    };
    let pipeline = if pipeline.is_empty() {
        None
    } else {
        Some(Arc::new(pipeline))
    };
    let engine = engine
        .with_health(health)
        .with_volumes(probe.volumes.clone())
        .with_pipeline(pipeline);
    Ok((cfg, probe, engine))
}

/// Open the task's durable cache entry and install (or reuse) its shared
/// coordinator. Playback and explicit cache warming both call this helper, so
/// they always converge on the same bitmap and worker pool.
async fn ensure_cache_job(
    state: &AppState,
    task_id: &str,
    entry: &Arc<TaskEntry>,
) -> Result<Arc<CacheJob>, ProxyError> {
    if let Some(job) = state.downloads.get(task_id) {
        return Ok(job);
    }
    let (cfg, probe, engine) = prepare_engine(state, task_id, entry).await?;
    // Report why we can't cache in the user's terms. A probe that failed
    // outright comes back looking exactly like "the origin works but has no
    // range support" — telling someone whose URL is dead that their server
    // doesn't do byte ranges sends them debugging the wrong thing.
    if let Some(reason) = probe.probe_error {
        return Err(ProxyError::Internal(format!(
            "cannot reach the upstream: {reason}"
        )));
    }
    if !probe.accepts_ranges {
        return Err(ProxyError::Internal(
            "upstream does not support byte ranges; full caching needs ranges".into(),
        ));
    }
    let total = probe
        .total_size
        .filter(|value| *value > 0)
        .ok_or_else(|| ProxyError::Internal("upstream did not report a cacheable size".into()))?;
    let mut urls = cfg.urls();
    urls.sort_unstable();
    let meta = CacheMeta {
        etag: probe.etag,
        last_modified: probe.last_modified,
        total_size: total,
        content_type: probe.content_type,
        block_size: crate::cache::BLOCK_SIZE,
        urls,
    };
    let key = crate::cache::CacheStore::key_for_task(&cfg);
    let cache = state.cache.open(&key, meta)?;
    let job = CacheJob::new(task_id.to_string(), Arc::new(engine), cache)?;
    state.downloads.insert(Arc::clone(&job));
    Ok(job)
}
/// Start or continue filling the task's persistent cache. Idempotent while a
/// cache job is already running; already-cached blocks are skipped by bitmap.
async fn start_task_cache(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::download::CacheJobInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    let job = ensure_cache_job(&state, &task_id, &entry).await?;
    job.start_cache();
    Ok(Json(job.info()))
}
/// Pause only the active whole-file fill. Playback leases remain live and keep
/// using/filling the same cache around their seek positions.
async fn pause_task_cache(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::download::CacheJobInfo>, ProxyError> {
    let job = state
        .downloads
        .get(&task_id)
        .ok_or_else(|| ProxyError::Internal(format!("no cache job for task {task_id}")))?;
    job.pause_cache();
    Ok(Json(job.info()))
}
/// Recreate active cache fills restored from disk. The legacy persisted field
/// is still named `downloads`; old output-specific fields are ignored.
pub async fn restore_downloads(state: AppState, saved: Vec<PersistedDownload>) {
    for persisted in saved {
        if !persisted.was_running {
            continue;
        }
        let Some(entry) = state.get(&persisted.task_id) else {
            continue;
        };
        match ensure_cache_job(&state, &persisted.task_id, &entry).await {
            Ok(job) => job.start_cache(),
            Err(error) => tracing::warn!(
                "restoring cache job {} failed: {}",
                persisted.task_id,
                error,
            ),
        }
    }
}
/// Decide which filename (if any) to advertise on Content-Disposition.
///
/// * `auto_filename = true` → take whatever the upstream probe detected; if
///   probing returned nothing, fall back to the user's saved value, then to
///   the task's display name. This makes "auto" mean "always reflect the live
///   server" without losing a usable name when detection fails.
/// * `auto_filename = false` → use `output_filename` verbatim. Empty/None
///   means "no Content-Disposition header" — let the client pick its own.
fn resolve_served_filename(cfg: &TaskConfig, detected: Option<String>) -> Option<String> {
    let trim_opt = |s: Option<&str>| {
        s.map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    if cfg.auto_filename {
        detected
            .or_else(|| trim_opt(cfg.output_filename.as_deref()))
            .or_else(|| trim_opt(cfg.name.as_deref()))
    } else {
        trim_opt(cfg.output_filename.as_deref())
    }
}

/// Acquire the staging substrate for one stream.
///
/// Requires byte-range support and a known total size — without both there is
/// nothing to schedule and the caller falls back to a single passthrough
/// stream. `cache: true` yields the durable entry (bytes outlive the stream);
/// otherwise an ephemeral scratch file is used and removed when the last
/// concurrent stream on this content finishes.
fn resolve_staging(
    state: &AppState,
    cfg: &TaskConfig,
    probe: &UpstreamProbe,
) -> Result<Option<Staging>, ProxyError> {
    if !probe.accepts_ranges {
        tracing::trace!("staging skipped: upstream doesn't support ranges");
        return Ok(None);
    }
    let total = match probe.total_size {
        Some(t) if t > 0 => t,
        _ => {
            tracing::trace!("staging skipped: unknown total size");
            return Ok(None);
        }
    };
    let key = crate::cache::CacheStore::key_for_task(cfg);
    // Stored URL list is just a traceability hint — we record it sorted and
    // deduped (across all mirrors of all volumes) so it's stable regardless
    // of how the user ordered things.
    let mut urls = cfg.urls();
    urls.sort_unstable();
    let meta = CacheMeta {
        etag: probe.etag.clone(),
        last_modified: probe.last_modified.clone(),
        total_size: total,
        content_type: probe.content_type.clone(),
        block_size: crate::cache::BLOCK_SIZE,
        urls,
    };
    let staging = crate::cache::CacheStore::acquire_staging(&state.cache, &key, meta, cfg.cache)?;
    tracing::debug!(
        "staging ready key={} total={} persistent={} etag={:?}",
        key,
        total,
        !staging.is_ephemeral(),
        probe.etag,
    );
    Ok(Some(staging))
}

#[allow(clippy::too_many_arguments)]
async fn build_stream_response(
    state: AppState,
    task_id: String,
    entry: Arc<TaskEntry>,
    engine: Arc<Engine>,
    probe: UpstreamProbe,
    range_header: Option<String>,
    head_only: bool,
    disposition: ContentDispositionMode,
    staging: Option<Staging>,
    shared_cache: Option<Arc<CacheJob>>,
    strategy: Strategy,
) -> Result<Response, ProxyError> {
    let _ = state; // reserved for future per-state telemetry

    let mut resp_headers = HeaderMap::new();
    if let Some(ct) = &probe.content_type {
        if let Ok(v) = HeaderValue::from_str(ct) {
            resp_headers.insert(header::CONTENT_TYPE, v);
        }
    }
    if let Some(et) = &probe.etag {
        if let Ok(v) = HeaderValue::from_str(et) {
            resp_headers.insert(header::ETAG, v);
        }
    }
    if let Some(lm) = &probe.last_modified {
        if let Ok(v) = HeaderValue::from_str(lm) {
            resp_headers.insert(header::LAST_MODIFIED, v);
        }
    }
    if let Some(name) = &probe.filename {
        if let Some(cd) = build_content_disposition(name, disposition) {
            if let Ok(v) = HeaderValue::from_str(&cd) {
                resp_headers.insert(header::CONTENT_DISPOSITION, v);
            }
        }
    }
    resp_headers.insert(
        header::ACCEPT_RANGES,
        HeaderValue::from_static(if probe.accepts_ranges {
            "bytes"
        } else {
            "none"
        }),
    );
    resp_headers.insert(
        HeaderName::from_static("x-hydraria-task"),
        HeaderValue::from_str(&task_id).unwrap(),
    );

    // If upstream doesn't support ranges or size unknown, do passthrough.
    if !probe.accepts_ranges || probe.total_size.is_none() {
        if let Some(total) = probe.total_size {
            resp_headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&total.to_string()).unwrap(),
            );
        }
        if head_only {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::OK;
            *resp.headers_mut() = resp_headers;
            return Ok(resp);
        }
        let upstream = engine.open_passthrough(None).await?;
        let conn_guard = Arc::new(ConnectionGuard::new(Arc::clone(&entry)));
        let entry_for_count = Arc::clone(&entry);
        let state_for_count = state.clone();
        let task_limiter = Arc::clone(&entry.limiter);
        let global_limiter = Arc::clone(&state.global_limiter);
        let engine_for_transform = Arc::clone(&engine);
        // Passthrough = single linear stream from upstream byte 0. Track the
        // running merged offset across chunks so the plugin pipeline (if any)
        // sees each byte's true position in the file. `Mutex<u64>` rather
        // than atomic because we read+write atomically per chunk and the
        // mutex is uncontended (chunks process strictly in order here).
        let merged_cursor = Arc::new(parking_lot::Mutex::new(0u64));
        let stream = upstream.bytes_stream().then(move |item| {
            let entry_for_count = Arc::clone(&entry_for_count);
            let state_for_count = state_for_count.clone();
            let task_limiter = Arc::clone(&task_limiter);
            let global_limiter = Arc::clone(&global_limiter);
            let engine_for_transform = Arc::clone(&engine_for_transform);
            let merged_cursor = Arc::clone(&merged_cursor);
            // Keeps the connection gauge up until the stream is dropped.
            let conn_guard = Arc::clone(&conn_guard);
            async move {
                let _conn = conn_guard;
                let item = match item {
                    Ok(b) => {
                        let n = b.len() as u64;
                        let merged_offset = {
                            let mut c = merged_cursor.lock();
                            let off = *c;
                            *c += n;
                            off
                        };
                        let b = engine_for_transform.transform_outgoing_public(merged_offset, b);
                        global_limiter.acquire(n).await;
                        task_limiter.acquire(n).await;
                        entry_for_count.count_bytes(n);
                        state_for_count.count_bytes_global(n);
                        Ok(b)
                    }
                    Err(e) => Err(e),
                };
                item
            }
        });
        let body = Body::from_stream(stream);
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::OK;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

    let total = probe.total_size.unwrap();
    let had_range = range_header.is_some();
    // Open-ended: client either sent no Range header at all or sent
    // `Range: X-` (no explicit upper bound). These are the requests where a
    // seek is most likely to come next, so the engine applies its head-zone
    // + abort-on-disconnect optimizations to keep seek latency low.
    let mut open_ended = !had_range;
    let (start, end) = if let Some(rh) = range_header {
        let (s, e) = parse_range_header(&rh, Some(total))?;
        if e.is_none() {
            open_ended = true;
        }
        let end = e.unwrap_or(total - 1).min(total - 1);
        if s > end {
            return Err(ProxyError::InvalidRange(rh));
        }
        (s, end)
    } else {
        (0, total.saturating_sub(1))
    };

    let length = end - start + 1;
    resp_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).unwrap(),
    );

    // Per RFC 7233: respond 206 whenever the client sent a Range header that
    // we honor, even when it covers the entire file. Chrome's <video>
    // element uses 200-vs-206 to decide whether seeking is supported.
    let status = if had_range {
        resp_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, total)).unwrap(),
        );
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    if head_only {
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = status;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

    let conn_guard = Arc::new(ConnectionGuard::new(Arc::clone(&entry)));
    let entry_for_count = Arc::clone(&entry);
    let state_for_count = state.clone();
    let task_limiter = Arc::clone(&entry.limiter);
    let global_limiter = Arc::clone(&state.global_limiter);

    // Staged path when a staging file is available: workers fetch out of order
    // straight to disk and an ordered reader streams from it, so memory no
    // longer bounds how much reordering we can absorb. Falls back to the
    // in-memory ordered path (bounded lookahead, fixed splits) otherwise.
    let rx = match shared_cache {
        Some(job) => job.stream(start, end),
        None => match staging {
            Some(s) => engine.stream_staged(start, end, strategy, s),
            None => engine.stream_range(start, end, open_ended),
        },
    };
    let stream = ReceiverStream::new(rx).then(move |item| {
        let entry_for_count = Arc::clone(&entry_for_count);
        let state_for_count = state_for_count.clone();
        let task_limiter = Arc::clone(&task_limiter);
        let global_limiter = Arc::clone(&global_limiter);
        // Keeps the connection gauge up until the stream is dropped.
        let conn_guard = Arc::clone(&conn_guard);
        async move {
            let _conn = conn_guard;
            match item {
                Ok(b) => {
                    let n = b.len() as u64;
                    global_limiter.acquire(n).await;
                    task_limiter.acquire(n).await;
                    entry_for_count.count_bytes(n);
                    state_for_count.count_bytes_global(n);
                    Ok::<_, std::io::Error>(b)
                }
                Err(e) => Err(std::io::Error::other(e.to_string())),
            }
        }
    });

    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    *resp.headers_mut() = resp_headers;
    Ok(resp)
}

/// Build an RFC 6266-compliant Content-Disposition header value for a filename.
/// Emits both an ASCII `filename=` fallback and an RFC 5987 `filename*=UTF-8''…`
/// form whenever the name contains anything non-ASCII or quote-unsafe, so old
/// clients see something readable while modern ones get the exact name. The
/// `disposition` knob picks `inline` (the historic default and the explicit
/// preview mode) vs `attachment` (force-download).
fn build_content_disposition(
    filename: &str,
    disposition: ContentDispositionMode,
) -> Option<String> {
    let name = filename.trim();
    if name.is_empty() {
        return None;
    }
    let disp_token = match disposition {
        ContentDispositionMode::Attachment => "attachment",
        ContentDispositionMode::Auto | ContentDispositionMode::Inline => "inline",
    };
    let needs_encoding = name
        .bytes()
        .any(|b| !b.is_ascii() || b < 0x20 || b == b'"' || b == b'\\');
    let ascii_fallback: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if !needs_encoding {
        Some(format!("{}; filename=\"{}\"", disp_token, ascii_fallback))
    } else {
        let encoded = percent_encode_rfc5987(name);
        Some(format!(
            "{}; filename=\"{}\"; filename*=UTF-8''{}",
            disp_token, ascii_fallback, encoded
        ))
    }
}

fn percent_encode_rfc5987(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        // RFC 5987 attr-char set.
        let safe = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            );
        if safe {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// Aggregated metadata for one plugin: registry-defined fields + the
/// **current** global config in effect. `global` is always populated — when
/// the user hasn't customized anything, it's the plugin's defaults.
#[derive(Serialize)]
struct PluginEntry {
    #[serde(flatten)]
    info: PluginInfo,
    global: serde_json::Value,
}

/// `GET /api/plugins` — list every registered plugin with metadata + current
/// global config. The dashboard uses this to render config forms and decide
/// whether to expose a forward (tool) panel.
async fn list_plugins(State(state): State<AppState>) -> Json<Vec<PluginEntry>> {
    let globals = state.settings.read().plugin_globals.clone();
    let mut out: Vec<PluginEntry> = Vec::new();
    for info in state.plugins.info_list(&globals) {
        let global = globals
            .get(&info.id)
            .cloned()
            .unwrap_or_else(|| info.default_global.clone());
        out.push(PluginEntry { info, global });
    }
    Json(out)
}

/// `GET /api/plugins/:plugin_id/global` — single-plugin variant of the
/// listing endpoint, handy for refreshing one config block without
/// re-fetching the whole catalog.
async fn get_plugin_global(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
) -> Result<Json<serde_json::Value>, ProxyError> {
    state
        .plugins
        .get(&plugin_id)
        .ok_or_else(|| ProxyError::Internal(format!("unknown plugin: {plugin_id}")))?;
    let g = state
        .settings
        .read()
        .plugin_globals
        .get(&plugin_id)
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    Ok(Json(g))
}

/// `PUT /api/plugins/:plugin_id/global` — overwrite the global config for
/// one plugin. Validated against the plugin's own schema before being
/// committed; bad values surface as 400-ish errors back to the form.
async fn put_plugin_global(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
    Json(value): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ProxyError> {
    let plugin = state
        .plugins
        .get(&plugin_id)
        .ok_or_else(|| ProxyError::Internal(format!("unknown plugin: {plugin_id}")))?;
    plugin
        .validate_global_config(&value)
        .map_err(|e| ProxyError::Internal(format!("invalid global config: {e}")))?;
    {
        let mut s = state.settings.write();
        s.plugin_globals.insert(plugin_id.clone(), value.clone());
    }
    Ok(Json(value))
}

#[derive(Deserialize)]
struct ForwardReq {
    /// Per-task config — same shape that would go in `TaskConfig.plugins[i].config`.
    /// Optional: when omitted, the plugin's defaults are used (and if
    /// `generate_missing` is enabled, fresh secrets are generated).
    #[serde(default)]
    task: serde_json::Value,
    /// Forward-only parameters (input/output paths, etc.).
    #[serde(default)]
    params: serde_json::Value,
}

/// `POST /api/plugins/:plugin_id/forward` — run the plugin's sender-side
/// operation and stream **NDJSON** progress + final result back. Each line
/// is one JSON object:
///
///   * `{"type":"progress","bytes_done":..,"bytes_total":..,"phase":".."}`
///   * `{"type":"result", ...}`  — the final ForwardResult fields, flattened
///     onto the same object (`bytes_in`, `bytes_out`, `info`, `message`)
///   * `{"type":"error","error":".."}`
///
/// We chose NDJSON over SSE so the existing `fetch()` plumbing on the UI
/// (which already JSON-decodes responses) only needs a stream-reader added
/// around it — no `EventSource`, no second protocol.
///
/// File IO runs on a blocking pool so it doesn't stall the tokio runtime —
/// the operation can be multi-GB on a fast disk.
async fn plugin_forward(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
    Json(req): Json<ForwardReq>,
) -> Result<Response, ProxyError> {
    let plugin = state
        .plugins
        .get(&plugin_id)
        .ok_or_else(|| ProxyError::Internal(format!("unknown plugin: {plugin_id}")))?;
    if !plugin.has_forward() {
        return Err(ProxyError::Internal(format!(
            "plugin '{plugin_id}' does not expose a forward tool"
        )));
    }
    let global = state
        .settings
        .read()
        .plugin_globals
        .get(&plugin_id)
        .cloned()
        .unwrap_or_else(|| plugin.default_global_config());

    // Channel from blocking encrypt loop → SSE-style streaming response.
    // Buffered so a slow client doesn't backpressure the encrypt loop into
    // stalling — we'd rather drop progress events than slow the work down.
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(32);

    let plugin_for_blocking = Arc::clone(&plugin);
    let task = req.task;
    let params = req.params;
    let progress_tx = tx.clone();
    tokio::task::spawn_blocking(move || {
        // The progress callback runs synchronously inside the encrypt loop,
        // so we use the channel's `try_send` which never blocks. When the
        // buffer is full we drop the event — the next tick will catch up.
        let progress: crate::plugins::ProgressSender = Arc::new(move |p| {
            let payload = serde_json::json!({
                "type": "progress",
                "bytes_done": p.bytes_done,
                "bytes_total": p.bytes_total,
                "phase": p.phase,
            });
            if let Ok(mut s) = serde_json::to_vec(&payload) {
                s.push(b'\n');
                let _ = progress_tx.try_send(bytes::Bytes::from(s));
            }
        });
        let outcome = plugin_for_blocking.forward(&global, &task, &params, progress);
        let final_payload = match outcome {
            Ok(r) => serde_json::json!({
                "type": "result",
                "bytes_in": r.bytes_in,
                "bytes_out": r.bytes_out,
                "info": r.info,
                "message": r.message,
            }),
            Err(e) => serde_json::json!({
                "type": "error",
                "error": e,
            }),
        };
        if let Ok(mut s) = serde_json::to_vec(&final_payload) {
            s.push(b'\n');
            // Final frame: blocking_send so it can't be dropped by a full
            // buffer — without this the UI may never see the result on a
            // backpressured channel.
            let _ = tx.blocking_send(bytes::Bytes::from(s));
        }
    });

    let stream = ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    // Disable proxy buffering (nginx and friends will hold the whole body
    // otherwise, defeating the point of streaming).
    resp.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    Ok(resp)
}

/// `GET /api/fs/info` — small capability probe so the UI knows whether to
/// render "Browse..." buttons. Avoids having the UI guess from `navigator.userAgent`.
async fn fs_info() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "picker_supported": fs_pick::is_supported(),
        "platform": std::env::consts::OS,
    }))
}

/// `POST /api/fs/pick` — pop a native open-file / open-dir / save-as dialog
/// and return the user's selection. Runs on the blocking pool because the
/// OS dialog is synchronous (and may sit for minutes if the user takes
/// their time).
async fn fs_pick_handler(
    State(_state): State<AppState>,
    Json(req): Json<PickRequest>,
) -> Result<Json<PickResponse>, ProxyError> {
    let resp = tokio::task::spawn_blocking(move || fs_pick::pick(req))
        .await
        .map_err(|e| ProxyError::Internal(format!("fs/pick task join: {e}")))?
        .map_err(ProxyError::Internal)?;
    Ok(Json(resp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn coerce(value: serde_json::Value) -> Result<(bool, serde_json::Value), String> {
        let mut map = value.as_object().expect("object").clone();
        let touched = coerce_url_aliases(&mut map)?;
        Ok((touched, serde_json::Value::Object(map)))
    }

    /// 脚本下发任务时手里只有 URL，不该被迫先学会「分卷」这个概念。
    #[test]
    fn url_aliases_all_land_on_the_same_volume_layout() {
        let one = json!([["https://a/f.mp4"]]);
        for body in [
            json!({"url": "https://a/f.mp4"}),
            json!({"urls": ["https://a/f.mp4"]}),
            json!({"uri": "https://a/f.mp4"}),
            json!({"uris": ["https://a/f.mp4"]}),
            json!({"volumes": "https://a/f.mp4"}),
            json!({"volumes": ["https://a/f.mp4"]}),
            json!({"volumes": [["https://a/f.mp4"]]}),
        ] {
            let (touched, out) = coerce(body.clone()).expect("valid");
            assert!(touched, "{body} mentions URLs");
            assert_eq!(out["volumes"], one, "从 {body} 归一");
            // 别名不该原样留在体里 —— TaskConfig 忽略未知字段，但留着会让
            // 「到底哪个字段说了算」在日志里变成一个悬案。
            for key in ["url", "urls", "uri", "uris"] {
                assert!(out.get(key).is_none(), "{key} 应当已被吸收");
            }
        }

        // 一组 URL = 同一个文件的多个镜像（aria2 addUri 的含义），不是多卷。
        let (_, out) = coerce(json!({"urls": ["https://a/f", "https://b/f"]})).unwrap();
        assert_eq!(out["volumes"], json!([["https://a/f", "https://b/f"]]));

        // 二维写法保持分卷语义。
        let (_, out) = coerce(json!({"volumes": [["https://a/1"], ["https://a/2"]]})).unwrap();
        assert_eq!(out["volumes"], json!([["https://a/1"], ["https://a/2"]]));
    }

    #[test]
    fn a_patch_that_never_mentions_urls_leaves_them_alone() {
        // 关键：这里插一个空 volumes 的话，一次「只改限速」的 PATCH 会把任务的
        // 源清空 —— 而且报的错会是「至少要有一个 URL」，与用户做的事毫无关系。
        let (touched, out) = coerce(json!({"cache": true})).expect("valid");
        assert!(!touched);
        assert!(out.get("volumes").is_none());
    }

    #[test]
    fn ambiguous_or_malformed_url_fields_are_rejected() {
        assert!(coerce(json!({"urls": ["https://a/f", ["https://b/f"]]})).is_err());
        assert!(coerce(json!({"url": 42})).is_err());
        assert!(coerce(json!({"urls": [{"u": "x"}]})).is_err());
    }

    /// `volumes` 显式给了就以它为准；别名只在它缺席或为空时补位。
    #[test]
    fn explicit_volumes_beat_the_aliases() {
        let (_, out) = coerce(json!({
            "volumes": [["https://real/1"]],
            "url": "https://leftover/0",
        }))
        .unwrap();
        assert_eq!(out["volumes"], json!([["https://real/1"]]));

        let (_, out) = coerce(json!({"volumes": [], "url": "https://a/f"})).unwrap();
        assert_eq!(out["volumes"], json!([["https://a/f"]]));
    }

    fn mapping(from: &str, to: &str) -> crate::hostmap::HostMapping {
        crate::hostmap::HostMapping {
            from: from.into(),
            to: to.into(),
            enabled: true,
        }
    }

    /// 「改完 target 再测一次，报的还是上一次的」—— 这个 bug 的根因是测试只认
    /// 已保存的规则，而按下测试的时机恰恰是还没保存的时候。草稿必须说了算。
    #[test]
    fn testing_a_draft_rule_reflects_the_edit_not_the_saved_value() {
        let _guard = crate::hostmap::lock_global();
        crate::hostmap::install(&[mapping("cdn.example.com", "1.1.1.1")]).unwrap();

        let edited = vec![mapping("cdn.example.com", "2.2.2.2")];
        for scope in [ResolveScope::Task, ResolveScope::Global] {
            let table = draft_table(edited.clone(), scope).unwrap();
            assert_eq!(
                table.explain("cdn.example.com").as_deref(),
                Some("2.2.2.2"),
                "改了 target 就要报新的那个",
            );
        }

        // 半截的行（刚点「添加映射」加出来的那一条）不该让整次测试失败。
        let table = draft_table(
            vec![mapping("cdn.example.com", "2.2.2.2"), mapping("", "")],
            ResolveScope::Task,
        )
        .unwrap();
        assert_eq!(table.explain("cdn.example.com").as_deref(), Some("2.2.2.2"));

        crate::hostmap::install(&[]).unwrap();
    }

    /// 两个 scope 的差别在「删掉一条规则之后」才显出来。
    #[test]
    fn a_task_draft_layers_over_global_while_a_global_draft_replaces_it() {
        let _guard = crate::hostmap::lock_global();
        crate::hostmap::install(&[
            mapping("a.example.com", "1.1.1.1"),
            mapping("b.example.com", "2.2.2.2"),
        ])
        .unwrap();

        // 任务级草稿：只写了 b，a 仍然由全局提供 —— 和任务真跑起来时一致。
        let task = draft_table(
            vec![mapping("b.example.com", "9.9.9.9")],
            ResolveScope::Task,
        )
        .unwrap();
        assert_eq!(task.explain("a.example.com").as_deref(), Some("1.1.1.1"));
        assert_eq!(task.explain("b.example.com").as_deref(), Some("9.9.9.9"));

        // 全局草稿：这就是全部规则。在设置里删掉 a 再测，答案必须是「没命中」，
        // 而不是把刚删掉的那条从已保存的全局表里又捞回来。
        let global = draft_table(
            vec![mapping("b.example.com", "9.9.9.9")],
            ResolveScope::Global,
        )
        .unwrap();
        assert_eq!(global.explain("a.example.com"), None);
        assert_eq!(global.explain("b.example.com").as_deref(), Some("9.9.9.9"));

        crate::hostmap::install(&[]).unwrap();
    }

    /// 规则写错时，那条错误正是用户按下测试想知道的东西。
    #[test]
    fn an_invalid_draft_rule_surfaces_as_an_error() {
        let err = draft_table(
            vec![mapping("cdn.example.com", "https://backup.example.com")],
            ResolveScope::Global,
        )
        .expect_err("URL 不是合法的映射目标");
        assert!(err.contains("bare host"), "{err}");
    }
}
