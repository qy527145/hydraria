//! 域名映射的端到端验证：真的起一个 TCP 服务，看请求最后落到哪个端口、
//! 服务端读到的 `Host` 又是什么。
//!
//! 单元测试只能证明表查得对，证明不了「TCP 连的是目标、应用层看到的是原地址」
//! —— 而那正是这个功能唯一的卖点。这里三件事各走一遍：域名（DNS 解析器）、
//! 裸 IP（URL 改写 + 显式 Host），以及**配了代理时映射依然生效**（那是实际
//! 踩到的坑：系统代理会把整条请求接管过去，DNS 钩子根本没机会被调用）。
//!
//! 映射表和代理配置都是进程级的，所以全部串在一个测试里按顺序走。

use std::net::SocketAddr;

use hydraria::hostmap::{self, Effective, HostMapping};
use reqwest::header::{HOST, HeaderMap, HeaderValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn rule(from: &str, to: &str) -> HostMapping {
    HostMapping {
        from: from.into(),
        to: to.into(),
        enabled: true,
    }
}

/// 一个只会说一句话的 HTTP 服务：把它读到的 `Host` 头原样作为响应体发回去。
async fn spawn_echo_host_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                // 请求头读完即止 —— 这些请求都没有 body。
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&buf).to_string();
                let host = head
                    .lines()
                    .find_map(|l| {
                        l.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("host"))
                    })
                    .map(|(_, v)| v.trim().to_string())
                    .unwrap_or_default();
                let body = host.as_bytes();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.write_all(body).await;
                let _ = sock.flush().await;
                // 显式半关闭再走人。直接 drop 会在接收队列还有字节时发 RST，
                // 连带把还没送出去的响应一起丢掉 —— 客户端看到的就是
                // `IncompleteMessage`。
                let _ = sock.shutdown().await;
            });
        }
    });
    addr
}

/// 按映射把请求发出去，返回源站看到的 `Host`。走的是和 `Engine::upstream`
/// 完全一样的两步：查表拿 [`hostmap::Routed`]，命中就换直连客户端。
async fn get_via(effective: &Effective, url: &str) -> reqwest::Result<String> {
    let mut headers = HeaderMap::new();
    let (client, url) = match effective.route(url) {
        Some(r) => {
            if let Some(h) = &r.host_header {
                headers.insert(HOST, HeaderValue::from_str(h).unwrap());
            }
            (
                hydraria::engine::direct_client_for(effective).unwrap(),
                r.url,
            )
        }
        None => (
            hydraria::engine::build_upstream_client().unwrap(),
            url.to_string(),
        ),
    };
    client.get(url).headers(headers).send().await?.text().await
}

#[tokio::test]
async fn mapping_moves_the_socket_but_not_the_request() {
    // 全程假装机器上配了个代理 —— 而且是个连不上的代理。命中映射的请求必须绕开
    // 它；没命中的请求本来就该走代理，所以下面对未命中路径的断言是「失败」。
    // 必须赶在任何 client 构建之前：代理配置是 build 时读的。
    //
    // SAFETY: 这个二进制里就这一个测试，此刻还没有别的线程在读环境变量。
    unsafe {
        std::env::set_var("HTTP_PROXY", "http://127.0.0.1:1");
        std::env::set_var("HTTPS_PROXY", "http://127.0.0.1:1");
        std::env::remove_var("NO_PROXY");
        std::env::remove_var("no_proxy");
    }

    let addr = spawn_echo_host_server().await;
    let to = format!("127.0.0.1:{}", addr.port());

    // --- 域名源：解析器把它指到本地服务，URL 和 Host 一个字都不动。
    // `.invalid` 是 RFC 2606 保留的顶级域，公共 DNS 永远解析不出来 —— 请求能
    // 到达服务端，本身就说明映射生效了，而不是碰巧被某个 DNS 兜底了。
    let table = hostmap::effective_for(&[rule("origin.invalid", &to)]).unwrap();

    let routed = table
        .route("http://origin.invalid/v/1.mp4?sign=abc")
        .expect("mapped");
    assert_eq!(
        routed.url, "http://origin.invalid/v/1.mp4?sign=abc",
        "域名走 DNS 那条路，URL 不该被改写"
    );
    assert_eq!(routed.host_header, None, "域名不需要显式 Host");

    let body = get_via(&table, "http://origin.invalid/v/1.mp4?sign=abc")
        .await
        .expect("命中映射的请求必须绕开代理");
    assert_eq!(body, "origin.invalid", "源站看到的必须还是原域名");

    // --- 同一条 URL 交给主客户端（走代理）就该失败。这一条断言的是那个坑本身：
    // 走代理时域名由代理解析，映射根本没机会生效。
    let proxied = hydraria::engine::build_upstream_client()
        .unwrap()
        .get("http://origin.invalid/v/1.mp4?sign=abc")
        .send()
        .await;
    assert!(
        proxied.is_err(),
        "走代理的请求不该连上本地源站 —— 否则这个测试没在测它该测的东西"
    );

    // --- 通配后缀同样只影响连到哪儿。
    let table = hostmap::effective_for(&[rule("*.origin.invalid", &to)]).unwrap();
    let body = get_via(&table, "http://cdn.origin.invalid/v/1.mp4")
        .await
        .unwrap();
    assert_eq!(body, "cdn.origin.invalid");

    // --- 裸 IP 源：hyper 会跳过 DNS 解析器，改走 URL 改写 + 显式 Host。
    // 10.0.0.1 是私网地址，本机上连不通，所以请求能成功同样只可能是映射生效。
    let table = hostmap::effective_for(&[rule("10.0.0.1", &to)]).unwrap();
    let routed = table
        .route("http://10.0.0.1/v/1.mp4?sign=abc")
        .expect("mapped");
    assert_eq!(
        routed.url,
        format!("http://127.0.0.1:{}/v/1.mp4?sign=abc", addr.port())
    );
    assert_eq!(routed.host_header.as_deref(), Some("10.0.0.1"));

    let body = get_via(&table, "http://10.0.0.1/v/1.mp4?sign=abc")
        .await
        .unwrap();
    assert_eq!(body, "10.0.0.1", "源站看到的必须还是原 IP");

    // --- 任务级规则盖住全局规则，两层并集一起生效。
    hostmap::install(&[
        rule("origin.invalid", "10.255.255.1"), // 全局指到一个连不上的地址
        rule("other.invalid", "10.255.255.2"),
    ])
    .unwrap();
    let table = hostmap::effective_for(&[rule("origin.invalid", &to)]).unwrap();
    let body = get_via(&table, "http://origin.invalid/x")
        .await
        .expect("任务级规则必须盖过全局那条");
    assert_eq!(body, "origin.invalid");
    assert_eq!(
        table.table.explain("other.invalid").as_deref(),
        Some("10.255.255.2"),
        "全局独有的规则要一起生效"
    );

    hostmap::install(&[]).unwrap();
}
