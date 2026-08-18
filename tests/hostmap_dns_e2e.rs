//! 映射目标的 DNS 走 DoT 时的端到端行为。
//!
//! 单测能证明报文拼得对、解析得对，证明不了「装了 DoT 之后请求还发得出去」——
//! 而这正是唯一会伤到人的失败模式：一个连不上的 DoT 服务器如果让解析直接失败，
//! 那就是把原本好用的下载全都弄坏了。所以这里真起一个 TCP 服务当上游，把 DoT
//! 指到一个必然连不上的地址，看请求最后能不能照样落到上游身上。
//!
//! 另外验证「没命中映射的请求完全不受影响」—— DoT 的作用范围只有映射目标那一次
//! 解析，这是设计上刻意划的界。
//!
//! DNS 设置和映射表都是进程级的，所以全部串在一个测试里按顺序走。

use std::net::SocketAddr;

use hydraria::dns::DnsMode;
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

/// 把读到的 `Host` 头原样发回去的最小 HTTP 服务。
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
                let _ = sock.shutdown().await;
            });
        }
    });
    addr
}

/// 与 `Engine::upstream` 同款的两步：查表，命中就换直连客户端。
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
async fn a_dead_dot_server_falls_back_instead_of_breaking_downloads() {
    let addr = spawn_echo_host_server().await;
    // 目标写成 `localhost:<port>`，这样它是个**域名**，必须经过解析这一步 ——
    // 裸 IP 会走 URL 改写那条路，压根不碰 DNS，也就测不到 DoT。
    let to = format!("localhost:{}", addr.port());

    // 把 DoT 指到一个必然连不上的地址（127.0.0.1:1）。
    hostmap::install_dns(&DnsMode::Dot(SocketAddr::from(([127, 0, 0, 1], 1)))).unwrap();
    assert_eq!(
        hostmap::dns_setting().as_deref(),
        Some("tls://127.0.0.1:1"),
        "设置应当能原样读回来"
    );

    let table = hostmap::effective_for(&[rule("origin.invalid", &to)]).unwrap();
    let host = get_via(&table, "http://origin.invalid/v/1.mp4?sign=abc")
        .await
        .expect("DoT 连不上时必须退回系统解析，而不是让请求失败");
    assert_eq!(
        host, "origin.invalid",
        "退回系统解析后，应用层看到的仍然是原域名"
    );

    // 诊断接口如实说明这次是谁解析的：命中映射 + 装了 DoT。
    let saved = hostmap::effective_for(&[rule("origin.invalid", &to)]).unwrap();
    let diag = hostmap::diagnose(&saved.table, "origin.invalid").await;
    assert_eq!(diag.resolver, "tls://127.0.0.1:1");
    assert_eq!(diag.mapped_to.as_deref(), Some(to.as_str()));
    assert!(
        !diag.addresses.is_empty(),
        "退回系统解析之后仍该给出地址: {diag:?}"
    );

    // --- 没命中映射的 host：完全不该碰 DoT，诊断里也如实写 system。
    let diag = hostmap::diagnose(&saved.table, "localhost").await;
    assert_eq!(
        diag.resolver, "system",
        "DoT 只作用于映射目标，没命中的一律系统解析"
    );
    assert_eq!(diag.mapped_to, None);

    // --- 关掉 DoT 之后回到纯系统解析。
    hostmap::install_dns(&DnsMode::System).unwrap();
    assert_eq!(hostmap::dns_setting(), None);
    let diag = hostmap::diagnose(&saved.table, "origin.invalid").await;
    assert_eq!(diag.resolver, "system");
    let host = get_via(&table, "http://origin.invalid/v/1.mp4")
        .await
        .expect("关掉 DoT 后照常");
    assert_eq!(host, "origin.invalid");
}
