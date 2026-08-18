//! 自定义域名 / IP 映射 —— 等价于 `curl --resolve`，但可在运行时热更新。
//!
//! 场景：URL 里的域名没被公共 DNS 收录（私有 CDN、内网回源域名、只在某地区
//! 解析的加速域名），而域名本身又动不得 —— 一改签名参数就对不上了。这里做的
//! 事因此只有一件：**只改 TCP 连到哪儿，不改应用层看到的是谁**。URL 原样发出，
//! Host 头、TLS SNI、证书校验用的仍然是原域名，所以签名、防盗链、CDN 的
//! vhost 路由全都照常工作。
//!
//! 两条实现路径，取决于原 URL 里写的是域名还是裸 IP：
//!
//! * **原地址是域名** → 走 [`MappedResolver`]（reqwest 的自定义 DNS 解析器）。
//!   URL 一个字节都不动，连 hyper 都不知道自己被换了地址，语义最干净。
//! * **原地址是裸 IP** → hyper 认出 IP 字面量后会直接跳过 DNS 解析器，钩子挂不
//!   上。这种情况改写 URL 的 host 并显式带上 `Host: <原 IP[:端口]>`，应用层看到
//!   的仍是原地址。代价是 https 下 TLS 证书校验会按目标地址走 —— 但原地址本来
//!   就是裸 IP，证书里带 IP SAN 的情况本就罕见，可接受。
//!
//! 两条路都从 [`HostTable::route`] 出发，调用方不用关心自己碰上的是哪一种。
//!
//! 配置有两层：全局设置里一份，任务上一份，任务级取并集并覆盖同名规则
//! （[`merged_rules`]）。全局那份是热更新的，任务那份在建 engine 时快照。
//!
//! 还有一件必须做的事：**命中映射的请求要绕开代理**。走代理时请求是整条交给代理
//! 发的，域名由代理去解析，映射根本没有机会生效 —— 而系统级代理（macOS 的
//! 网络设置、各种 fake-ip 代理工具）是默认开启且不易察觉的。既然用户已经明确
//! 指定了「连这里」，就不该再被代理接管，见 [`crate::engine::direct_client_for`]。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::{Deserialize, Serialize};

/// 一条映射规则。`from` 是 URL 里写的域名 / IP，`to` 是真正要连的地址。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostMapping {
    /// 原域名、原 IP，或 `*.example.com` 形式的通配后缀。
    pub from: String,
    /// 目标域名或 IP，可带 `:端口`。
    pub to: String,
    /// 关掉的规则保留在配置里但不参与解析 —— 排查问题时比删了再手打回来省事。
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// 解析后的目标。端口 `0` 表示「沿用原 URL 的端口」。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Ip(IpAddr, u16),
    Host(String, u16),
}

/// 把 `to` 解析成目标地址。接受 `1.2.3.4`、`1.2.3.4:8080`、`::1`、`[::1]:8080`、
/// `backup.example.com`、`backup.example.com:8443`。
fn parse_target(raw: &str) -> Result<Target, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("target must not be empty".into());
    }
    if s.contains("://") || s.contains('/') {
        return Err(format!("target '{s}' must be a bare host or IP, not a URL"));
    }
    // 顺序要紧：`::1` 既是合法 IPv6 也含冒号，先按纯 IP 试。
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(Target::Ip(ip, 0));
    }
    if let Ok(addr) = s.parse::<SocketAddr>() {
        return Ok(Target::Ip(addr.ip(), addr.port()));
    }
    let (host, port) = match s.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            let port: u16 = p
                .parse()
                .map_err(|_| format!("target '{s}' has an out-of-range port"))?;
            (h, port)
        }
        _ => (s, 0u16),
    };
    if host.contains(':') {
        // 带方括号的 IPv6 走不到这儿（上面 SocketAddr 已经吃掉），剩下的形如
        // `a:b:c` 的东西只可能是打错了。
        return Err(format!("target '{s}' is not a valid host or IP"));
    }
    if !host
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
    {
        return Err(format!(
            "target '{s}' contains characters invalid in a host"
        ));
    }
    Ok(Target::Host(normalize_host(host), port))
}

/// 主机名比较是大小写无关的，末尾的根点也不算区别。
fn normalize_host(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

/// 校验一条规则的 `from`。`to` 由 [`parse_target`] 负责。
fn parse_source(raw: &str) -> Result<String, String> {
    let s = normalize_host(raw);
    if s.is_empty() {
        return Err("source must not be empty".into());
    }
    if s.contains("://") || s.contains('/') {
        return Err(format!("source '{s}' must be a bare host or IP, not a URL"));
    }
    if s.contains(':') && s.parse::<IpAddr>().is_err() {
        return Err(format!(
            "source '{s}' must not carry a port — mapping is per host, and the port comes from the URL"
        ));
    }
    if let Some(rest) = s.strip_prefix("*.") {
        if rest.is_empty() {
            return Err("wildcard source needs a suffix, e.g. *.example.com".into());
        }
    } else if s.contains('*') {
        return Err(format!(
            "source '{s}': the only wildcard form is a leading '*.', e.g. *.example.com"
        ));
    }
    Ok(s)
}

/// 查表用的解析结果。一张表 = 一个任务实际生效的全部规则（全局 ∪ 任务级）。
#[derive(Debug, Default)]
pub struct HostTable {
    exact: HashMap<String, Target>,
    /// `(".example.com", 目标)`，即去掉前导 `*` 之后的后缀。命中取最长的一条。
    wildcards: Vec<(String, Target)>,
}

/// 一条命中的映射对某个具体请求意味着什么。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed {
    /// 真正要请求的 URL。原地址是域名时与原 URL 完全相同。
    pub url: String,
    /// 需要显式带上的 `Host`。只有原地址是裸 IP（URL 被改写了）时才有值。
    pub host_header: Option<String>,
    /// 命中的规则，形如 `cdn.example.com -> 1.2.3.4`。日志和诊断接口用。
    pub matched: String,
}

impl HostTable {
    /// 从规则列表构建。返回 `Err` 时整张表不生效 —— 半张表比没有表更难排查。
    ///
    /// 后面的规则覆盖前面的同名规则，任务级映射盖住全局映射就靠这个顺序
    /// （见 [`merged_rules`]）。
    pub fn build(rules: &[HostMapping]) -> Result<Self, String> {
        let mut table = HostTable::default();
        for rule in rules {
            if !rule.enabled {
                continue;
            }
            let from = parse_source(&rule.from)?;
            let to = parse_target(&rule.to)?;
            if let Some(suffix) = from.strip_prefix('*') {
                let suffix = suffix.to_string();
                table.wildcards.retain(|(s, _)| *s != suffix);
                table.wildcards.push((suffix, to));
            } else {
                table.exact.insert(from, to);
            }
        }
        // 长后缀优先：`*.cdn.example.com` 要盖过 `*.example.com`。
        table
            .wildcards
            .sort_by_key(|(s, _)| std::cmp::Reverse(s.len()));
        Ok(table)
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.wildcards.is_empty()
    }

    fn lookup(&self, host: &str) -> Option<Target> {
        let host = normalize_host(host);
        if let Some(t) = self.exact.get(&host) {
            return Some(t.clone());
        }
        self.wildcards
            .iter()
            .find(|(suffix, _)| host.ends_with(suffix.as_str()) && host.len() > suffix.len())
            .map(|(_, t)| t.clone())
    }

    /// 这个 URL 的 host 命中映射了吗？命中的话该怎么发这条请求？
    ///
    /// 两条路径在这里合流：
    /// * host 是域名 → URL 原样返回，改连接目标的活儿由 [`MappedResolver`] 在 DNS
    ///   层干，`Host`、SNI、证书校验用的都还是原域名。
    /// * host 是裸 IP → hyper 认出 IP 字面量后会跳过 DNS 解析器，钩子挂不上，所以
    ///   这里改写 URL 的 host，并把原地址放进 `host_header`，让应用层看到的不变。
    pub fn route(&self, url: &str) -> Option<Routed> {
        if self.is_empty() {
            return None;
        }
        let mut parsed = reqwest::Url::parse(url).ok()?;
        let host_str = parsed.host_str()?;
        // URL 里的 IPv6 带方括号，查表用的是裸地址。
        let bare = host_str.trim_start_matches('[').trim_end_matches(']');
        let target = self.lookup(bare)?;
        let matched = format!("{bare} -> {}", describe_target(&target));

        let Ok(ip) = bare.parse::<IpAddr>() else {
            // 域名：一个字节都不用动。
            return Some(Routed {
                url: url.to_string(),
                host_header: None,
                matched,
            });
        };
        let _ = ip;

        // 应用层要看到的仍然是原地址：URL 里显式写了端口就一并带上。
        let original = match parsed.port() {
            Some(p) => format!("{host_str}:{p}"),
            None => host_str.to_string(),
        };
        let (new_host, new_port) = match target {
            // url crate 只认带方括号的 IPv6 字面量。
            Target::Ip(ip @ IpAddr::V6(_), port) => (format!("[{ip}]"), port),
            Target::Ip(ip, port) => (ip.to_string(), port),
            Target::Host(h, port) => (h, port),
        };
        parsed.set_host(Some(&new_host)).ok()?;
        // 这里和域名那条路**不对称**，而且是故意的：改写 URL 意味着映射里的端口
        // 直接盖掉原 URL 的端口，而走 DNS 解析器时 hyper 会反过来让原 URL 的显式
        // 端口赢（见 `system_lookup`）。裸 IP 源本来就是「这台机器上的这个服务」，
        // 换机器常常连端口一起换，所以让写下来的端口说了算更符合意图。
        // 两条路的差别有 `tests/hostmap_port_e2e.rs` 盯着。
        if new_port != 0 {
            parsed.set_port(Some(new_port)).ok()?;
        }
        Some(Routed {
            url: parsed.to_string(),
            host_header: Some(original),
            matched,
        })
    }

    /// 诊断接口用：这个 host 会被解析到哪儿去。
    pub fn explain(&self, host: &str) -> Option<String> {
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        self.lookup(bare).map(|t| describe_target(&t))
    }
}

fn describe_target(t: &Target) -> String {
    match t {
        Target::Ip(ip, 0) => ip.to_string(),
        Target::Ip(ip, p) => format!("{ip}:{p}"),
        Target::Host(h, 0) => h.clone(),
        Target::Host(h, p) => format!("{h}:{p}"),
    }
}

/// 进程级的全局映射表 —— 每个任务的生效表都从它出发再叠上任务级规则。
static GLOBAL: LazyLock<RwLock<Arc<HostTable>>> =
    LazyLock::new(|| RwLock::new(Arc::new(HostTable::default())));
/// 全局规则的原始列表，合并任务级规则时要用。
static GLOBAL_RULES: LazyLock<RwLock<Arc<Vec<HostMapping>>>> =
    LazyLock::new(|| RwLock::new(Arc::new(Vec::new())));
/// 每换一次全局表加一。任务级的表和客户端是快照，靠它作废重建。
static GENERATION: AtomicU64 = AtomicU64::new(0);
/// 解析映射目标用的 DoT 解析器。`None` = 交给系统解析器（默认，老行为）。
///
/// 只在**映射命中、且目标是域名**时才会用到它，见 [`resolve_target`]。开着 TUN
/// 模式的代理时系统解析会给出 fake-ip，映射因此静默失效 —— 详见 [`crate::dns`]。
static DOT: LazyLock<RwLock<Option<Arc<crate::dns::DotResolver>>>> =
    LazyLock::new(|| RwLock::new(None));

/// 装载解析映射目标用的 DNS。与 [`install`] 一样，启动恢复配置和每次保存全局
/// 设置时各调一次。
pub fn install_dns(mode: &crate::dns::DnsMode) -> Result<(), String> {
    match mode {
        crate::dns::DnsMode::System => {
            if DOT.read().is_some() {
                tracing::info!("host map dns: back to the system resolver");
            }
            *DOT.write() = None;
        }
        crate::dns::DnsMode::Dot(server) => {
            let resolver = Arc::new(crate::dns::DotResolver::new(*server)?);
            tracing::info!("host map dns: resolving mapping targets over DoT via {server}");
            *DOT.write() = Some(resolver);
        }
    }
    GENERATION.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// 当前装载的解析器，没装就是 `None`。
fn dot() -> Option<Arc<crate::dns::DotResolver>> {
    DOT.read().clone()
}

/// 当前 DNS 设置的原样回显（给设置接口读回去）。
pub fn dns_setting() -> Option<String> {
    dot().map(|r| {
        crate::dns::describe(&crate::dns::DnsMode::Dot(r.server()))
            .unwrap_or_else(|| r.server().to_string())
    })
}

/// 校验并装载一组全局规则。启动恢复配置和每次保存全局设置时各调一次。
///
/// 换表不会掐断已经建立的连接：reqwest 的连接池按 (scheme, host, port) 索引，
/// 池子里的旧连接会一直复用到空闲超时。实践中够用 —— 会去改映射，通常正是因为
/// 现在这条根本连不上，也就不存在可复用的旧连接。
pub fn install(rules: &[HostMapping]) -> Result<(), String> {
    let table = HostTable::build(rules)?;
    for r in rules.iter().filter(|r| r.enabled) {
        tracing::info!(
            "host mapping (global): {} -> {}",
            r.from.trim(),
            r.to.trim()
        );
    }
    let active = table.exact.len() + table.wildcards.len();
    *GLOBAL.write() = Arc::new(table);
    *GLOBAL_RULES.write() = Arc::new(rules.to_vec());
    GENERATION.fetch_add(1, Ordering::Relaxed);
    if active == 0 {
        tracing::info!("host mappings cleared");
    }
    Ok(())
}

/// 只校验不装载，给保存前的入参检查用。
pub fn validate(rules: &[HostMapping]) -> Result<(), String> {
    HostTable::build(rules).map(|_| ())
}

pub fn global_table() -> Arc<HostTable> {
    Arc::clone(&GLOBAL.read())
}

/// 全局表当前的版本号。任务级快照带上它，全局一变就整体作废。
pub fn generation() -> u64 {
    GENERATION.load(Ordering::Relaxed)
}

/// 全局 ∪ 任务级：并集，`from` 撞车时以任务级为准（后写的覆盖先写的，
/// 见 [`HostTable::build`]）。
pub fn merged_rules(task_rules: &[HostMapping]) -> Vec<HostMapping> {
    let global = Arc::clone(&GLOBAL_RULES.read());
    if task_rules.is_empty() {
        return global.as_ref().clone();
    }
    let mut out = global.as_ref().clone();
    for r in task_rules {
        let key = normalize_host(&r.from);
        out.retain(|g| normalize_host(&g.from) != key);
        out.push(r.clone());
    }
    out
}

/// 一个任务实际生效的映射：表本身，外加一枚由「构成这张表的那批规则」算出来的
/// 指纹。
///
/// 指纹和表绑在一个值里，是因为它是直连客户端注册表的键（见
/// [`crate::engine::direct_client_for`]）。让调用方自己传键的话，两张不同的表
/// 用同一个键就会静默串到同一个客户端上 —— 这个 bug 在测试里真的写出来过。
#[derive(Clone)]
pub struct Effective {
    pub table: Arc<HostTable>,
    key: u64,
}

impl Effective {
    /// 直连客户端注册表的键。
    pub fn key(&self) -> u64 {
        self.key
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn route(&self, url: &str) -> Option<Routed> {
        self.table.route(url)
    }
}

/// 一个任务实际生效的表 = 全局 ∪ 任务级。`Err` 只会来自任务级规则 —— 全局的
/// 那批装载前已经校验过。
pub fn effective_for(task_rules: &[HostMapping]) -> Result<Effective, String> {
    let merged = merged_rules(task_rules);
    let table = if task_rules.is_empty() {
        // 没有任务级规则就直接复用全局那张表，省一次构建。
        global_table()
    } else {
        Arc::new(HostTable::build(&merged)?)
    };
    Ok(Effective {
        key: rules_key(&merged),
        table,
    })
}

/// 一组规则的指纹。同一批规则（顺序也相同）必须得到同一个值 —— 合并结果就是
/// 由顺序决定的，所以顺序参与哈希是对的。
fn rules_key(rules: &[HostMapping]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for r in rules {
        r.from.hash(&mut h);
        r.to.hash(&mut h);
        r.enabled.hash(&mut h);
    }
    h.finish()
}

/// 走代理时请求是交给代理去发的，域名由代理解析，映射根本没有机会生效。Hydraria
/// 因此让**命中映射的请求绕开代理**（见 [`crate::engine::direct_client_for`]），
/// 这条提醒留给那些绕不开的情况：只有系统级代理、且用户想不通为什么。
///
/// 只认得出环境变量这一种代理配置。macOS / Windows 的系统级代理 reqwest 也会用，
/// 但读它需要额外依赖，这里不做。
pub fn proxy_env() -> Option<String> {
    // `NO_PROXY=*` 关掉了所有代理，包括系统级的那份，这时候没什么好提醒的。
    let no_proxy = std::env::var("NO_PROXY")
        .or_else(|_| std::env::var("no_proxy"))
        .unwrap_or_default();
    if no_proxy.trim() == "*" {
        return None;
    }
    const VARS: &[&str] = &[
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
    ];
    VARS.iter()
        .find(|v| std::env::var_os(v).is_some())
        .map(|v| (*v).to_string())
}

/// 系统解析：映射没命中，或者命中了但目标是个域名，都落到这里。
async fn system_lookup(host: &str, port: u16) -> Result<Vec<SocketAddr>, std::io::Error> {
    // 端口 0 是给 hyper 的信号：用原 URL / scheme 的端口。反过来，映射里写死的
    // 端口只在原 URL 没显式写端口时生效（hyper 的 `set_port`）。
    tokio::net::lookup_host((host, port))
        .await
        .map(|it| it.collect())
}

/// 解析**映射目标**的域名。装了 DoT 就走 DoT，否则（或 DoT 失败）走系统解析。
///
/// 这是 DoT 的唯一作用点，故意不放进 [`system_lookup`]：那个函数也负责「没命中
/// 映射」的那条路，而没命中映射的请求不该改变任何行为 —— 命中映射才意味着用户
/// 明确指定了「连这里」，也只有那条链路会被 TUN 的 fake-ip 搞坏。
///
/// DoT 失败一律退回系统解析，宁可回到老行为，也不要因为 DNS 配错就把整条下载
/// 判死。
async fn resolve_target(host: &str, port: u16) -> Result<Vec<SocketAddr>, std::io::Error> {
    let Some(resolver) = dot() else {
        return system_lookup(host, port).await;
    };
    match resolver.lookup(host).await {
        Ok(ips) => {
            tracing::debug!(
                "host map dns: {host} resolved over DoT to {}",
                ips.iter()
                    .map(IpAddr::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            Ok(ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect())
        }
        Err(e) => {
            tracing::warn!(
                "host map dns: DoT lookup for {host} failed, falling back to the system resolver: {e}"
            );
            system_lookup(host, port).await
        }
    }
}

/// 装在上游 `reqwest::Client` 上的 DNS 解析器：命中映射就返回目标地址，否则原样
/// 交给系统解析器。
///
/// 表可以是全局那张（跟着设置热更新），也可以是某个任务的快照。
pub struct MappedResolver(Source);

enum Source {
    Global,
    Fixed(Arc<HostTable>),
}

impl MappedResolver {
    /// 跟随全局表，永远读最新的一份。
    pub fn global() -> Self {
        MappedResolver(Source::Global)
    }

    /// 绑定一张固定的表，给带任务级映射的客户端用。
    pub fn fixed(table: Arc<HostTable>) -> Self {
        MappedResolver(Source::Fixed(table))
    }

    fn table(&self) -> Arc<HostTable> {
        match &self.0 {
            Source::Global => global_table(),
            Source::Fixed(t) => Arc::clone(t),
        }
    }
}

impl Resolve for MappedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let target = self.table().lookup(&host);
        Box::pin(async move {
            let (lookup_host, port, mapped) = match target {
                Some(Target::Ip(ip, port)) => {
                    let addr = SocketAddr::new(ip, port);
                    tracing::info!("host map: {host} resolved to {ip} (mapped, no DNS lookup)");
                    return Ok(Box::new(std::iter::once(addr)) as Addrs);
                }
                Some(Target::Host(h, port)) => (h, port, true),
                None => (host.clone(), 0, false),
            };
            // 命中映射的目标走 resolve_target（可能是 DoT）；没命中的一律系统解析。
            let looked_up = if mapped {
                resolve_target(&lookup_host, port).await
            } else {
                system_lookup(&lookup_host, port).await
            };
            match looked_up {
                Ok(addrs) if mapped => {
                    tracing::info!(
                        "host map: {host} resolved via {lookup_host} to {}",
                        fmt_addrs(&addrs)
                    );
                    Ok(Box::new(addrs.into_iter()) as Addrs)
                }
                Ok(addrs) => {
                    tracing::debug!("no host mapping for {host}, DNS gave {}", fmt_addrs(&addrs));
                    Ok(Box::new(addrs.into_iter()) as Addrs)
                }
                Err(e) => {
                    if mapped {
                        // 映射本身生效了，是目标地址解析不出来 —— 这个区别很重要，
                        // 否则用户会以为映射没配对。
                        tracing::warn!(
                            "host map: {host} -> {lookup_host}, but the target failed to resolve: {e}"
                        );
                    }
                    Err(Box::new(std::io::Error::new(
                        e.kind(),
                        format!("resolve {lookup_host}: {e}"),
                    ))
                        as Box<dyn std::error::Error + Send + Sync>)
                }
            }
        })
    }
}

fn fmt_addrs(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(|a| a.ip().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// 诊断用：给一个 host，报告它会被映射到哪儿、最终解析出什么地址。
/// 面板的「测试」按钮和 `GET /api/hostmap/resolve` 走的都是这里。
pub async fn diagnose(table: &HostTable, host: &str) -> Diagnosis {
    let bare = host.trim().trim_start_matches('[').trim_end_matches(']');
    let mapped_to = table.explain(bare);
    let lookup = match table.lookup(bare) {
        // 目标就是 IP，没有解析这一步。
        Some(Target::Ip(ip, _)) => Ok(vec![SocketAddr::new(ip, 0)]),
        // 命中映射：这一跳与真实请求走同一条解析路径（可能是 DoT），
        // 否则「测试」测的就不是实际会发生的事。
        Some(Target::Host(h, _)) => resolve_target(&h, 0).await,
        None if bare.parse::<IpAddr>().is_ok() => {
            Ok(vec![SocketAddr::new(bare.parse().unwrap(), 0)])
        }
        None => system_lookup(bare, 0).await,
    };
    let (addresses, error) = match lookup {
        Ok(a) => (a.iter().map(|s| s.ip().to_string()).collect(), None),
        Err(e) => (Vec::new(), Some(e.to_string())),
    };
    Diagnosis {
        host: bare.to_string(),
        mapped_to,
        addresses,
        error,
        proxy_env: proxy_env(),
        // 只有命中映射的目标域名才会走 DoT，所以这里如实反映「这次是谁解析的」。
        resolver: match (&mapped_to_is_host(table, bare), dns_setting()) {
            (true, Some(dns)) => dns,
            _ => "system".to_owned(),
        },
    }
}

/// 这个 host 命中的映射目标是不是一个「还需要解析」的域名。
fn mapped_to_is_host(table: &HostTable, host: &str) -> bool {
    matches!(table.lookup(host), Some(Target::Host(_, _)))
}

/// [`diagnose`] 的结果，也是 `GET /api/hostmap/resolve` 的响应体。
#[derive(Debug, Clone, Serialize)]
pub struct Diagnosis {
    pub host: String,
    /// 命中的映射目标；`None` = 没有规则命中，走的是正常 DNS。
    pub mapped_to: Option<String>,
    /// 最终解析出来的地址（映射命中时是目标的地址）。
    pub addresses: Vec<String>,
    pub error: Option<String>,
    /// 检测到的代理环境变量名。命中映射的请求会绕开代理，这里只是告知。
    pub proxy_env: Option<String>,
    /// 这次解析是谁做的：`system`，或者具体的 DoT 服务器（`tls://1.1.1.1`）。
    /// 只有命中映射、且目标还是域名时才可能不是 `system`。
    pub resolver: String,
}

/// 装载全局表的用例互相会踩 —— 同一个二进制里的测试是并行跑的，而全局表是
/// 进程唯一的一份。凡是碰 [`install`] 的用例都先拿这把锁，包括 `routes` 那边
/// 测「草稿规则叠在全局之上」的那几个，所以它必须在这里、而不是某个测试模块内部。
#[cfg(test)]
pub(crate) static GLOBAL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(crate) fn lock_global() -> std::sync::MutexGuard<'static, ()> {
    GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(from: &str, to: &str) -> HostMapping {
        HostMapping {
            from: from.into(),
            to: to.into(),
            enabled: true,
        }
    }

    #[test]
    fn parses_targets() {
        assert_eq!(
            parse_target("1.2.3.4").unwrap(),
            Target::Ip("1.2.3.4".parse().unwrap(), 0)
        );
        assert_eq!(
            parse_target("1.2.3.4:8080").unwrap(),
            Target::Ip("1.2.3.4".parse().unwrap(), 8080)
        );
        assert_eq!(
            parse_target("::1").unwrap(),
            Target::Ip("::1".parse().unwrap(), 0)
        );
        assert_eq!(
            parse_target("[::1]:8080").unwrap(),
            Target::Ip("::1".parse().unwrap(), 8080)
        );
        assert_eq!(
            parse_target("Backup.Example.COM.").unwrap(),
            Target::Host("backup.example.com".into(), 0)
        );
        assert_eq!(
            parse_target("backup.example.com:8443").unwrap(),
            Target::Host("backup.example.com".into(), 8443)
        );
        assert!(parse_target("https://backup.example.com").is_err());
        assert!(parse_target("backup.example.com/x").is_err());
        assert!(parse_target("").is_err());
        assert!(parse_target("host:99999").is_err());
    }

    #[test]
    fn rejects_bad_sources() {
        assert!(parse_source("example.com:8080").is_err());
        assert!(parse_source("http://example.com").is_err());
        assert!(parse_source("ex*ample.com").is_err());
        assert!(parse_source("*.").is_err());
        assert_eq!(parse_source("*.Example.com").unwrap(), "*.example.com");
        // 裸 IPv6 作为原地址是合法的（走 URL 改写那条路）。
        assert_eq!(parse_source("::1").unwrap(), "::1");
    }

    #[test]
    fn exact_beats_wildcard_and_longest_wildcard_wins() {
        let table = HostTable::build(&[
            rule("*.example.com", "1.1.1.1"),
            rule("*.cdn.example.com", "2.2.2.2"),
            rule("a.cdn.example.com", "3.3.3.3"),
        ])
        .unwrap();
        assert_eq!(
            table.lookup("a.cdn.example.com"),
            Some(Target::Ip("3.3.3.3".parse().unwrap(), 0))
        );
        assert_eq!(
            table.lookup("b.cdn.example.com"),
            Some(Target::Ip("2.2.2.2".parse().unwrap(), 0))
        );
        assert_eq!(
            table.lookup("b.example.com"),
            Some(Target::Ip("1.1.1.1".parse().unwrap(), 0))
        );
        // 通配不匹配裸后缀本身。
        assert_eq!(table.lookup("example.com"), None);
        assert_eq!(table.lookup("other.com"), None);
    }

    #[test]
    fn disabled_rules_are_ignored() {
        let mut r = rule("example.com", "1.1.1.1");
        r.enabled = false;
        let table = HostTable::build(&[r]).unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let table = HostTable::build(&[rule("Example.COM", "1.1.1.1")]).unwrap();
        assert!(table.lookup("EXAMPLE.com.").is_some());
    }

    #[test]
    fn route_leaves_domains_alone_and_rewrites_ip_literals() {
        let table = HostTable::build(&[
            rule("10.0.0.1", "backup.example.com"),
            rule("10.0.0.2", "192.168.1.9:8080"),
            rule("example.com", "1.2.3.4"),
        ])
        .unwrap();

        // 裸 IP：URL 改写 + 把原地址放进 Host。
        let r = table
            .route("https://10.0.0.1/v/1.mp4?sign=abc&t=1")
            .expect("mapped");
        assert_eq!(r.url, "https://backup.example.com/v/1.mp4?sign=abc&t=1");
        assert_eq!(r.host_header.as_deref(), Some("10.0.0.1"));
        assert_eq!(r.matched, "10.0.0.1 -> backup.example.com");

        // 原 URL 显式写了端口时，Host 头要连端口一起保留。
        let r = table.route("http://10.0.0.2:8000/a").expect("mapped");
        assert_eq!(r.url, "http://192.168.1.9:8080/a");
        assert_eq!(r.host_header.as_deref(), Some("10.0.0.2:8000"));

        // 域名：命中了，但 URL 一个字节都不动，Host 也不用补 —— 那条路走 DNS。
        let r = table.route("https://example.com/a?s=1").expect("mapped");
        assert_eq!(r.url, "https://example.com/a?s=1");
        assert_eq!(r.host_header, None);

        // 没命中的地址完全不受影响。
        assert!(table.route("https://10.9.9.9/a").is_none());
        assert!(table.route("https://other.com/a").is_none());
        assert!(
            HostTable::default()
                .route("https://example.com/a")
                .is_none()
        );
    }

    #[test]
    fn task_rules_union_with_global_and_win_on_conflict() {
        let _guard = lock_global();
        install(&[
            rule("a.example.com", "1.1.1.1"),
            rule("b.example.com", "2.2.2.2"),
        ])
        .unwrap();

        let merged = merged_rules(&[
            rule("b.example.com", "9.9.9.9"), // 覆盖全局的同名规则
            rule("c.example.com", "3.3.3.3"), // 追加
        ]);
        let table = HostTable::build(&merged).unwrap();
        assert_eq!(
            table.explain("a.example.com").as_deref(),
            Some("1.1.1.1"),
            "全局独有的规则要保留"
        );
        assert_eq!(
            table.explain("b.example.com").as_deref(),
            Some("9.9.9.9"),
            "撞名时任务级说了算"
        );
        assert_eq!(table.explain("c.example.com").as_deref(), Some("3.3.3.3"));

        // 通配规则同样能被任务级盖掉。
        install(&[rule("*.cdn.com", "1.1.1.1")]).unwrap();
        let table = HostTable::build(&merged_rules(&[rule("*.cdn.com", "8.8.8.8")])).unwrap();
        assert_eq!(table.explain("x.cdn.com").as_deref(), Some("8.8.8.8"));

        // 任务没有自己的规则时，直接复用全局那张表（不用重建）。
        assert!(Arc::ptr_eq(
            &effective_for(&[]).unwrap().table,
            &global_table()
        ));

        install(&[]).unwrap();
    }

    #[test]
    fn generation_moves_when_the_global_table_changes() {
        let _guard = lock_global();
        let before = generation();
        install(&[rule("gen.example.com", "1.1.1.1")]).unwrap();
        assert!(generation() > before, "换表必须让任务级快照作废");
        install(&[]).unwrap();
    }
}
