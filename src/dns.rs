//! DNS-over-TLS 解析器 —— 让域名映射在 TUN 模式的代理下也能用。
//!
//! [`crate::hostmap`] 做的事是「只改 TCP 连到哪儿，URL / `Host` / SNI 一律不动」。
//! 这在 TUN 模式的代理面前会失效，原因不在映射本身，而在**域名解析被劫持**：
//!
//! * 映射规则写的是 `内网域名 → cdn-node.example.com`，目标是个域名，于是要先
//!   解析它；
//! * TUN 环境下系统解析器返回的是 fake-ip（`198.18.0.0/15` 那一段），连接因此
//!   落到 TUN 上；
//! * 代理拿请求里的域名去路由 —— 而带签名的下载地址里那个主机名往往是公网根本
//!   查不到的（私有回源域名、内网机房域名），于是代理回一个空响应。表现就是
//!   「连上了、状态码也对、但一个字节都不来」，而不是一个干脆的解析失败。
//!
//! 绕开的办法是**自己解析**：把映射目标解析成真实 IP 交给 hyper，系统解析器和
//! TUN 都没有插手的机会。`Host` 头与 SNI 照旧，所以签名照旧成立。
//!
//! 为什么是 DoT 而不是普通 UDP 53：TUN 环境里 UDP 53 往往被一并劫持或直接丢弃
//! （实测 `1.1.1.1` / `8.8.8.8` 的 UDP 查询全部超时，而 853 端口的 TLS 连接畅通）。
//! 走 TLS 还顺带保证了应答没被中间人改过。
//!
//! **作用范围只有「映射命中、且目标是域名」这一次解析。** 没命中映射的请求走的
//! 是原本的系统解析，行为一个字节都不变 —— 命中映射意味着用户已经明确指定了
//! 「连这里」，那条链路才是被 TUN 搞坏的那条。解析失败也一律退回系统解析：宁可
//! 回到老行为，也不要因为 DNS 配错就把整条下载判死。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::RngCore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 解析结果最长缓存多久 —— 再长也不会超过应答里的 TTL。
const MAX_TTL: Duration = Duration::from_secs(10 * 60);
/// TTL 为 0 或缺失时兜底缓存这么久，免得每个分片都去查一次。
const MIN_TTL: Duration = Duration::from_secs(30);
/// 单次查询（连接 + 收发）的整体超时。
const QUERY_TIMEOUT: Duration = Duration::from_secs(6);
const DOT_PORT: u16 = 853;

/// 解析方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsMode {
    /// 交给系统解析器 —— 也就是老行为。TUN 环境下会拿到 fake-ip。
    System,
    /// 自己走 DoT 查。
    Dot(SocketAddr),
}

/// 解析 `dns` 设置。
///
/// 接受 `tls://1.1.1.1`、`tls://1.1.1.1:853`、`1.1.1.1`（省略前缀即 DoT），
/// 以及 `system` / 留空（用系统解析器）。**只收 IP** —— 解析器地址本身要是还得
/// 先解析一次域名，就又回到被劫持的起点了。
pub fn parse_mode(raw: Option<&str>) -> Result<DnsMode, String> {
    let raw = raw.unwrap_or("").trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("system") {
        return Ok(DnsMode::System);
    }
    let body = raw
        .strip_prefix("tls://")
        .or_else(|| raw.strip_prefix("dot://"))
        .unwrap_or(raw);
    // 先按「带端口」试，`[::1]:853` 这种也一并吃掉。
    if let Ok(addr) = body.parse::<SocketAddr>() {
        return Ok(DnsMode::Dot(addr));
    }
    let bare = body.trim_start_matches('[').trim_end_matches(']');
    match bare.parse::<IpAddr>() {
        Ok(ip) => Ok(DnsMode::Dot(SocketAddr::new(ip, DOT_PORT))),
        Err(_) => Err(format!(
            "dns must be an IP address (e.g. tls://1.1.1.1), got: {raw}"
        )),
    }
}

/// 配置原样回显用（设置接口把它读回去）。
pub fn describe(mode: &DnsMode) -> Option<String> {
    match mode {
        DnsMode::System => None,
        DnsMode::Dot(addr) if addr.port() == DOT_PORT => Some(format!("tls://{}", addr.ip())),
        DnsMode::Dot(addr) => Some(format!("tls://{addr}")),
    }
}

#[derive(Clone)]
struct Cached {
    ips: Vec<IpAddr>,
    expires_at: Instant,
}

/// 带 TTL 缓存的 DoT 解析器。
pub struct DotResolver {
    server: SocketAddr,
    cache: Mutex<HashMap<String, Cached>>,
    tls: Arc<rustls::ClientConfig>,
}

impl DotResolver {
    /// 建一个指向 `server` 的解析器。
    ///
    /// 证书按 IP 严格校验（`1.1.1.1` / `8.8.8.8` / `223.5.5.5` 的 DoT 证书都带
    /// IP SAN，实测校验得过）。根证书用 reqwest 已经在用的那套系统信任库
    /// （`rustls-platform-verifier`），不额外引入一份证书数据。
    ///
    /// 配了个没有 IP SAN 的解析器会在握手时失败，调用方随即退回系统解析 ——
    /// 宁可退回去，也不悄悄把校验关掉。
    pub fn new(server: SocketAddr) -> Result<Self, String> {
        use rustls_platform_verifier::BuilderVerifierExt;
        // provider 显式选 aws-lc-rs：与 reqwest 编进来的那份一致，别再拉一个
        // crypto 后端进来。进程级默认 provider 装没装过不归这里管。
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("dns: aws-lc-rs rejected the default protocol versions: {e}"))?
            .with_platform_verifier()
            .map_err(|e| format!("dns: cannot build a TLS config: {e}"))?
            .with_no_client_auth();
        Ok(Self {
            server,
            cache: Mutex::new(HashMap::new()),
            tls: Arc::new(tls),
        })
    }

    pub fn server(&self) -> SocketAddr {
        self.server
    }

    /// 查 `host` 的 A 记录。命中缓存就不出网。
    pub async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        if let Some(hit) = self.cached(host) {
            return Ok(hit);
        }
        let (ips, ttl) = tokio::time::timeout(QUERY_TIMEOUT, self.query(host))
            .await
            .map_err(|_| format!("DoT query for {host} timed out"))??;
        if ips.is_empty() {
            return Err(format!("DoT query for {host} returned no A records"));
        }
        let ttl = ttl.clamp(MIN_TTL, MAX_TTL);
        self.cache.lock().unwrap().insert(
            host.to_owned(),
            Cached {
                ips: ips.clone(),
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(ips)
    }

    fn cached(&self, host: &str) -> Option<Vec<IpAddr>> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(host) {
            Some(hit) if hit.expires_at > Instant::now() => Some(hit.ips.clone()),
            Some(_) => {
                cache.remove(host);
                None
            }
            None => None,
        }
    }

    async fn query(&self, host: &str) -> Result<(Vec<IpAddr>, Duration), String> {
        let (id, question) = build_query(host)?;
        let stream = tokio::net::TcpStream::connect(self.server)
            .await
            .map_err(|e| format!("cannot reach the DoT server {}: {e}", self.server))?;
        let _ = stream.set_nodelay(true);
        let name = rustls::pki_types::ServerName::IpAddress(self.server.ip().into());
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls));
        let mut tls = connector.connect(name, stream).await.map_err(|e| {
            format!(
                "TLS handshake with the DoT server {} failed: {e}",
                self.server
            )
        })?;

        // RFC 7858：TCP/TLS 上的 DNS 报文前面加两字节长度。
        let mut framed = Vec::with_capacity(question.len() + 2);
        framed.extend_from_slice(&(question.len() as u16).to_be_bytes());
        framed.extend_from_slice(&question);
        tls.write_all(&framed)
            .await
            .map_err(|e| format!("DoT write failed: {e}"))?;
        tls.flush()
            .await
            .map_err(|e| format!("DoT flush failed: {e}"))?;

        let mut len = [0u8; 2];
        tls.read_exact(&mut len)
            .await
            .map_err(|e| format!("DoT length prefix read failed: {e}"))?;
        let mut body = vec![0u8; u16::from_be_bytes(len) as usize];
        tls.read_exact(&mut body)
            .await
            .map_err(|e| format!("DoT response read failed: {e}"))?;
        parse_answer(&body, id)
    }
}

/// 拼一个只问 A 记录的查询报文，返回 (事务 ID, 报文)。
fn build_query(host: &str) -> Result<(u16, Vec<u8>), String> {
    let id = rand::thread_rng().next_u32() as u16;
    let mut packet = Vec::with_capacity(host.len() + 18);
    packet.extend_from_slice(&id.to_be_bytes());
    packet.extend_from_slice(&0x0100u16.to_be_bytes()); // 标准查询 + 期望递归
    packet.extend_from_slice(&1u16.to_be_bytes()); // qdcount
    packet.extend_from_slice(&[0; 6]); // an/ns/ar count 全 0
    for label in host.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(format!("not a valid hostname: {host}"));
        }
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    packet.extend_from_slice(&1u16.to_be_bytes()); // QTYPE = A
    packet.extend_from_slice(&1u16.to_be_bytes()); // QCLASS = IN
    Ok((id, packet))
}

/// 从应答里挑出 A 记录。CNAME 不必单独跟 —— 递归解析器会把链尾的 A 一起塞在
/// 同一个应答里。
fn parse_answer(data: &[u8], want_id: u16) -> Result<(Vec<IpAddr>, Duration), String> {
    let short = || "the DoT response was truncated".to_owned();
    if data.len() < 12 {
        return Err(short());
    }
    if u16::from_be_bytes([data[0], data[1]]) != want_id {
        return Err("the DoT response carried a different transaction id".into());
    }
    let rcode = data[3] & 0x0f;
    if rcode != 0 {
        return Err(format!("the DoT server answered with rcode={rcode}"));
    }
    let qdcount = u16::from_be_bytes([data[4], data[5]]);
    let ancount = u16::from_be_bytes([data[6], data[7]]);

    let mut at = 12usize;
    for _ in 0..qdcount {
        at = skip_name(data, at)?;
        at = at.checked_add(4).ok_or_else(short)?; // QTYPE + QCLASS
    }

    let mut ips = Vec::new();
    let mut min_ttl = MAX_TTL;
    for _ in 0..ancount {
        at = skip_name(data, at)?;
        if at + 10 > data.len() {
            return Err(short());
        }
        let rtype = u16::from_be_bytes([data[at], data[at + 1]]);
        let ttl = u32::from_be_bytes([data[at + 4], data[at + 5], data[at + 6], data[at + 7]]);
        let rdlen = u16::from_be_bytes([data[at + 8], data[at + 9]]) as usize;
        at += 10;
        if at + rdlen > data.len() {
            return Err(short());
        }
        if rtype == 1 && rdlen == 4 {
            ips.push(IpAddr::from([
                data[at],
                data[at + 1],
                data[at + 2],
                data[at + 3],
            ]));
            min_ttl = min_ttl.min(Duration::from_secs(u64::from(ttl)));
        }
        at += rdlen;
    }
    Ok((ips, min_ttl))
}

/// 跳过一个域名字段，处理 RFC 1035 的压缩指针。
fn skip_name(data: &[u8], mut at: usize) -> Result<usize, String> {
    loop {
        let len = *data.get(at).ok_or("the DoT response was truncated")? as usize;
        if len == 0 {
            return Ok(at + 1);
        }
        if len & 0xC0 == 0xC0 {
            // 压缩指针占两字节，且必然是名字的最后一段。
            return Ok(at + 2);
        }
        at = at + 1 + len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_config_is_parsed() {
        assert_eq!(parse_mode(None).unwrap(), DnsMode::System);
        assert_eq!(parse_mode(Some("  ")).unwrap(), DnsMode::System);
        assert_eq!(parse_mode(Some("system")).unwrap(), DnsMode::System);
        let want = DnsMode::Dot(SocketAddr::from(([1, 1, 1, 1], 853)));
        assert_eq!(parse_mode(Some("tls://1.1.1.1")).unwrap(), want);
        assert_eq!(parse_mode(Some("dot://1.1.1.1")).unwrap(), want);
        assert_eq!(parse_mode(Some("1.1.1.1")).unwrap(), want);
        assert_eq!(
            parse_mode(Some("tls://1.1.1.1:8853")).unwrap(),
            DnsMode::Dot(SocketAddr::from(([1, 1, 1, 1], 8853)))
        );
        // 解析器地址本身必须是 IP，否则又得先解析一次域名。
        assert!(parse_mode(Some("tls://dns.example.com")).is_err());
        assert!(parse_mode(Some("not an address")).is_err());
    }

    /// 回显给设置接口的形态：默认端口省掉，非默认端口带上。
    #[test]
    fn describe_round_trips() {
        assert_eq!(describe(&DnsMode::System), None);
        let mode = parse_mode(Some("tls://1.1.1.1")).unwrap();
        assert_eq!(describe(&mode).as_deref(), Some("tls://1.1.1.1"));
        let mode = parse_mode(Some("tls://1.1.1.1:8853")).unwrap();
        assert_eq!(describe(&mode).as_deref(), Some("tls://1.1.1.1:8853"));
        // 回显出来的字符串必须还能再解析回同一个值。
        let again = parse_mode(describe(&mode).as_deref()).unwrap();
        assert_eq!(again, mode);
    }

    /// 报文构造与解析对得上：拿自己拼的查询当样本，接一段手写的应答。
    #[test]
    fn a_records_are_parsed_with_compression_pointers() {
        let (id, query) = build_query("cdn.example.com").unwrap();
        assert_eq!(u16::from_be_bytes([query[0], query[1]]), id);

        let mut answer = query.clone();
        answer[2] = 0x81; // QR + RD
        answer[3] = 0x80; // RA，rcode = 0
        answer[6..8].copy_from_slice(&2u16.to_be_bytes()); // ancount = 2
        // 第一条 CNAME（应当被忽略），名字用压缩指针指回问题段
        answer.extend_from_slice(&[0xC0, 0x0C]);
        answer.extend_from_slice(&5u16.to_be_bytes()); // TYPE = CNAME
        answer.extend_from_slice(&1u16.to_be_bytes());
        answer.extend_from_slice(&600u32.to_be_bytes());
        answer.extend_from_slice(&2u16.to_be_bytes());
        answer.extend_from_slice(&[0xC0, 0x0C]);
        // 第二条 A 记录
        answer.extend_from_slice(&[0xC0, 0x0C]);
        answer.extend_from_slice(&1u16.to_be_bytes()); // TYPE = A
        answer.extend_from_slice(&1u16.to_be_bytes());
        answer.extend_from_slice(&120u32.to_be_bytes());
        answer.extend_from_slice(&4u16.to_be_bytes());
        answer.extend_from_slice(&[119, 249, 103, 71]);

        let (ips, ttl) = parse_answer(&answer, id).unwrap();
        assert_eq!(ips, vec![IpAddr::from([119, 249, 103, 71])]);
        assert_eq!(ttl, Duration::from_secs(120));
        // 事务 ID 对不上必须拒收（防投毒的最低要求）。
        assert!(parse_answer(&answer, id.wrapping_add(1)).is_err());
    }

    #[test]
    fn rcode_and_truncation_are_rejected() {
        let (id, query) = build_query("example.com").unwrap();
        let mut nxdomain = query.clone();
        nxdomain[3] = 0x83; // rcode = 3
        assert!(parse_answer(&nxdomain, id).is_err());
        assert!(parse_answer(&query[..8], id).is_err());
    }

    /// 连不上的解析器必须**报错而不是挂住** —— 调用方靠这个错误退回系统解析。
    #[tokio::test]
    async fn an_unreachable_resolver_fails_instead_of_hanging() {
        // 9 号端口是 discard，连上也不会有 DNS 应答；这里用一个几乎必然没人听的
        // 高位端口，让它尽快失败。
        let resolver = DotResolver::new(SocketAddr::from(([127, 0, 0, 1], 1))).unwrap();
        assert!(resolver.lookup("cdn.example.com").await.is_err());
    }
}

#[cfg(test)]
mod live {
    use super::*;

    /// 真连一次 1.1.1.1:853。默认忽略（要联网），排查时 `--ignored` 单跑。
    #[tokio::test]
    #[ignore]
    async fn resolves_over_the_wire() {
        let DnsMode::Dot(server) = parse_mode(Some("tls://1.1.1.1")).unwrap() else {
            panic!("should have parsed as DoT");
        };
        let resolver = DotResolver::new(server).unwrap();
        let ips = resolver.lookup("bdcu01.baidupcs.com").await.unwrap();
        println!("bdcu01.baidupcs.com -> {ips:?}");
        assert!(!ips.is_empty());
        // 不能是 fake-ip（198.18.0.0/15）——那正是要绕开的东西。
        for ip in &ips {
            let IpAddr::V4(v4) = ip else { continue };
            let o = v4.octets();
            assert!(
                !(o[0] == 198 && (o[1] == 18 || o[1] == 19)),
                "got a fake-ip back: {ip}"
            );
        }
        // 第二次必须命中缓存（不再出网）。
        assert_eq!(ips, resolver.lookup("bdcu01.baidupcs.com").await.unwrap());
    }
}
