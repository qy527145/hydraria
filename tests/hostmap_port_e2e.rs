//! 映射目标里的端口到底什么时候说了算 —— 两条实现路径的答案不一样，而这件事
//! 只能实测：`to` 里写的端口最终生不生效，取决于 hyper 在解析结果上怎么设端口，
//! 光读表结构看不出来。
//!
//! 单独一个测试二进制：另一个 e2e 会往进程环境里塞代理配置，而端口这件事和代理
//! 无关，混在一起只会让失败原因变模糊。

use std::net::SocketAddr;

use hydraria::hostmap::{self, HostMapping};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn rule(from: &str, to: &str) -> HostMapping {
    HostMapping {
        from: from.into(),
        to: to.into(),
        enabled: true,
    }
}

/// 一个只报自己端口号的 HTTP 服务。请求落在谁身上，响应体就是谁的端口。
async fn spawn_port_echo() -> SocketAddr {
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
                let body = addr.port().to_string();
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            });
        }
    });
    addr
}

/// 按映射发一条请求，返回**实际连上的那个端口**。
async fn port_reached(rules: &[HostMapping], url: &str) -> String {
    let effective = hostmap::effective_for(rules).unwrap();
    let routed = effective.route(url);
    let (client, url) = match &routed {
        Some(r) => (
            hydraria::engine::direct_client_for(&effective).unwrap(),
            r.url.clone(),
        ),
        None => (
            hydraria::engine::build_upstream_client().unwrap(),
            url.to_string(),
        ),
    };
    let mut req = client.get(url);
    if let Some(h) = routed.as_ref().and_then(|r| r.host_header.as_deref()) {
        req = req.header(reqwest::header::HOST, h);
    }
    req.send().await.unwrap().text().await.unwrap()
}

/// `to` 里的端口在两条路径上的效力**不一样**，文档必须分开写：
///
/// * 原地址是**域名** → 走 DNS 解析器，端口最终由 hyper 决定：原 URL 显式写了
///   端口就用原 URL 的，没写才用映射里的。
/// * 原地址是**裸 IP** → URL 被整条改写，映射里的端口**总是**说了算。
#[tokio::test]
async fn a_port_on_the_target_wins_only_when_the_url_did_not_name_one() {
    let mapped = spawn_port_echo().await; // 映射想把流量送到这里
    let named = spawn_port_echo().await; // URL 里显式写的是这里
    let target = format!("127.0.0.1:{}", mapped.port());

    // --- 域名源，URL 没写端口：映射里的端口生效。
    let reached = port_reached(
        &[rule("origin.invalid", &target)],
        "http://origin.invalid/a.mp4",
    )
    .await;
    assert_eq!(
        reached,
        mapped.port().to_string(),
        "URL 没写端口时，映射里的端口就是最终端口"
    );

    // --- 域名源，URL 显式写了端口：**原 URL 赢**，映射里的端口被忽略。
    // 这一条正是文档里那句「端口只在原 URL 没有显式写端口时才生效」的来源。
    let reached = port_reached(
        &[rule("origin.invalid", &target)],
        &format!("http://origin.invalid:{}/a.mp4", named.port()),
    )
    .await;
    assert_eq!(
        reached,
        named.port().to_string(),
        "URL 显式写了端口时，它盖过映射里的端口（只有主机被换掉）"
    );

    // --- 裸 IP 源：URL 整条被改写，映射里的端口总是赢，哪怕原 URL 写了别的。
    let reached = port_reached(
        &[rule("127.0.0.2", &target)],
        &format!("http://127.0.0.2:{}/a.mp4", named.port()),
    )
    .await;
    assert_eq!(
        reached,
        mapped.port().to_string(),
        "原地址是裸 IP 时走 URL 改写那条路，映射里的端口说了算"
    );

    // 改写之后应用层看到的仍然是原地址（含原端口）—— 端口换了，Host 头没换。
    let routed = hostmap::effective_for(&[rule("127.0.0.2", &target)])
        .unwrap()
        .route(&format!("http://127.0.0.2:{}/a.mp4", named.port()))
        .expect("mapped");
    assert_eq!(
        routed.host_header.as_deref(),
        Some(format!("127.0.0.2:{}", named.port()).as_str()),
    );
}
