//! `POST /api/tasks` 的端到端验证：脚本手里只有 URL，不该被迫先学会「分卷」。
//!
//! 单元测试只覆盖了别名归一那个纯函数，证明不了整条路 —— 请求体经过 axum 的
//! `Json` 抽取、`TaskConfig` 的 serde 默认值、`normalize()` 之后，任务到底建成
//! 什么样。这里直接把请求打进 router，看 `/api/tasks` 里躺着的是什么。
//!
//! 不碰网络：所有 URL 都指向一个不存在的地址，而建任务本身不需要探测源站。

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use hydraria::models::{AppState, GlobalSettings};
use hydraria::routes::build_router;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

/// 一个只活在临时目录里的实例。每个用例一份，互不干扰。
fn app(dir: &std::path::Path) -> axum::Router {
    let cache = Arc::new(hydraria::cache::CacheStore::new(dir.join("cache")).unwrap());
    let state = AppState::new(
        "127.0.0.1:0".into(),
        cache,
        dir.join("tasks.json"),
        GlobalSettings::default(),
        hydraria::plugins::default_registry(),
        Arc::new(hydraria::download::DownloadManager::new()),
        hydraria::engine::build_upstream_client().unwrap(),
    );
    build_router(state)
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hydraria-api-e2e-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn send(app: &axum::Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

async fn tasks(app: &axum::Router) -> Vec<Value> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/tasks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// aria2 / Motrix / Gopeed 那种「给个 URL 就能下发任务」的写法，都要认。
#[tokio::test]
async fn a_script_can_create_a_task_from_a_bare_url() {
    let dir = tmpdir("create");
    let app = app(&dir);

    let (status, body) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"url": "https://origin.invalid/movie.mp4", "name": "from-script"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let id = body["task_id"].as_str().expect("task_id").to_string();
    assert!(
        body["proxy_url"].as_str().unwrap().ends_with(&id),
        "短链要指向刚建的任务：{body}"
    );
    // 没要求 start_cache 就不该出现这两个字段 —— 脚本据此判断「要不要看缓存结果」。
    assert!(body.get("cache_started").is_none());
    assert!(body.get("cache_error").is_none());

    let all = tasks(&app).await;
    assert_eq!(all.len(), 1);
    let config = &all[0]["config"];
    assert_eq!(
        config["volumes"],
        json!([["https://origin.invalid/movie.mp4"]]),
        "一个 url 就是一卷一镜像"
    );
    // 其余字段由服务端补齐，脚本一个都不用写。
    assert_eq!(config["name"], "from-script");
    assert_eq!(config["persist"], json!(true), "持久化默认开着");
    assert_eq!(config["max_per_volume"], json!(4));
    assert_eq!(
        config["max_threads"],
        json!(4),
        "单卷任务的总线程数 = 单卷上限"
    );
    assert_eq!(config["max_split"], json!(0), "分片大小默认自动");
}

/// 一组 URL 是同一个文件的多个镜像（aria2 `addUri` 的含义），二维才是分卷。
#[tokio::test]
async fn url_lists_are_mirrors_and_nested_lists_are_volumes() {
    let dir = tmpdir("shapes");
    let app = app(&dir);

    let (status, _) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"urls": ["https://a.invalid/f", "https://b.invalid/f"], "name": "mirrors"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"volumes": [["https://a.invalid/p1"], ["https://a.invalid/p2"]], "name": "volumes"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let all = tasks(&app).await;
    let by_name = |name: &str| -> Value {
        all.iter()
            .find(|t| t["config"]["name"] == name)
            .expect("task")
            .clone()
    };
    assert_eq!(
        by_name("mirrors")["config"]["volumes"],
        json!([["https://a.invalid/f", "https://b.invalid/f"]]),
    );
    let volumes = by_name("volumes");
    assert_eq!(
        volumes["config"]["volumes"],
        json!([["https://a.invalid/p1"], ["https://a.invalid/p2"]]),
    );
    assert_eq!(
        volumes["config"]["max_threads"],
        json!(8),
        "两卷 × 单卷 4 = 8 条线程"
    );
}

/// 换签名过期的地址在脚本里应该是一行 `{"url": …}`；而一次只改别的字段的
/// PATCH 绝不能把源清空。
#[tokio::test]
async fn patch_accepts_the_same_url_aliases_and_leaves_urls_alone_otherwise() {
    let dir = tmpdir("patch");
    let app = app(&dir);

    let (_, created) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"url": "https://origin.invalid/old.mp4?sign=expired"}),
    )
    .await;
    let id = created["task_id"].as_str().unwrap().to_string();

    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/tasks/{id}"),
        json!({"url": "https://origin.invalid/new.mp4?sign=fresh"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["config"]["volumes"],
        json!([["https://origin.invalid/new.mp4?sign=fresh"]]),
    );

    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/tasks/{id}"),
        json!({"name": "renamed", "cache": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["config"]["volumes"],
        json!([["https://origin.invalid/new.mp4?sign=fresh"]]),
        "没提到 URL 的 PATCH 不该动源列表",
    );
    assert_eq!(body["config"]["name"], "renamed");
    assert_eq!(body["config"]["cache"], json!(true));
}

/// 面板的 ⚡ 走的是这条 POST：测的必须是**屏幕上**那份规则，包括还没保存的改动。
/// 这曾经只有 GET 一个版本，于是「改完 target 再测一次，报的还是上一次的结果」。
#[tokio::test]
async fn testing_a_host_mapping_uses_the_rules_in_the_editor() {
    let dir = tmpdir("hostmap");
    let app = app(&dir);

    // scope=global 表示「这就是全部规则」，所以这次测试完全不看已保存的那份。
    let probe = |to: &str| {
        let app = app.clone();
        let to = to.to_string();
        async move {
            let (status, body) = send(
                &app,
                "POST",
                "/api/hostmap/resolve",
                json!({
                    "host": "cdn.invalid",
                    "scope": "global",
                    "mappings": [{"from": "cdn.invalid", "to": to, "enabled": true}],
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            body
        }
    };

    let first = probe("10.0.0.1").await;
    assert_eq!(first["mapped_to"], "10.0.0.1");
    // 改一个字符再测，结果必须跟着变 —— 这正是原来那个 bug 的形状。
    let second = probe("10.0.0.2:8443").await;
    assert_eq!(second["mapped_to"], "10.0.0.2:8443");

    // 空规则集（在设置里把最后一条删掉了）要如实报「没有规则命中」。
    let (status, body) = send(
        &app,
        "POST",
        "/api/hostmap/resolve",
        json!({"host": "cdn.invalid", "scope": "global", "mappings": []}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["mapped_to"], Value::Null);

    // 写坏的规则：这条错误正是按下测试的人想知道的东西。
    let (status, body) = send(
        &app,
        "POST",
        "/api/hostmap/resolve",
        json!({
            "host": "cdn.invalid",
            "scope": "global",
            "mappings": [{"from": "cdn.invalid", "to": "https://x.invalid/y", "enabled": true}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body["error"].as_str().unwrap().contains("bare host"),
        "{body}"
    );
}

/// 报错要说人话，而且要指向脚本真正该改的地方。
#[tokio::test]
async fn bad_requests_say_what_to_fix() {
    let dir = tmpdir("errors");
    let app = app(&dir);

    let (status, body) = send(&app, "POST", "/api/tasks", json!({"name": "no urls"})).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let error = body["error"].as_str().unwrap();
    assert!(error.contains("url"), "要提示该传哪个字段：{error}");

    // 混着写是拒绝而不是猜：猜错的代价是一个看起来建成了、播出来却是错的任务。
    let (status, body) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"urls": ["https://a.invalid/f", ["https://b.invalid/f"]]}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body["error"].as_str().unwrap().contains("mirrors"),
        "{body}"
    );

    // 一条写坏的域名映射应该让整次创建失败，而不是建一个连不上的任务出来。
    let (status, body) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({
            "url": "https://origin.invalid/f",
            "host_mappings": [{"from": "origin.invalid", "to": "https://elsewhere.invalid/x"}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body["error"].as_str().unwrap().contains("host mapping"),
        "{body}"
    );
    assert!(tasks(&app).await.is_empty(), "失败的创建不该留下任务");
}

/// 一份**把每个字段都写满**的任务配置。文档里那份完整示例就是它，两边必须一致 ——
/// 文档里的示例跑不通是最坏的一种文档。
fn every_field() -> Value {
    json!({
        "volumes": [
            ["https://cdn1.invalid/movie.part01", "https://cdn2.invalid/movie.part01"],
            ["https://cdn1.invalid/movie.part02"]
        ],
        "max_per_volume": 6,
        "max_split": "8M",
        "cache": true,
        "persist": true,
        "headers": {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://example.invalid/play/123",
            "Cookie": "session=xxxx"
        },
        "name": "每个字段都写满",
        "output_filename": "movie.mkv",
        "auto_filename": false,
        "rate_limit_bps": "2M",
        "rate_limit_algorithm": "sliding_window",
        "content_disposition": "attachment",
        "host_mappings": [
            { "from": "cdn1.invalid", "to": "10.0.0.1:8443", "enabled": true },
            { "from": "*.cdn2.invalid", "to": "backup.invalid", "enabled": false }
        ],
        "plugins": [
            {
                "id": "chacha20",
                "enabled": true,
                "config": {
                    "secret": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff000102030405060708090a0b"
                }
            }
        ]
    })
}

/// 任务能配的每一样东西，都必须能只靠 API 配出来 —— 面板能设、API 设不了的字段
/// 等于逼人去点界面，脚本就白写了。
///
/// 这个用例是那句承诺的执行版本：写满一份配置发过去，再读回来逐字段比对。少接一个
/// 字段（或者哪天 `TaskConfig` 新增字段忘了在文档 / 白名单里登记）这里就会红。
#[tokio::test]
async fn every_task_config_field_is_reachable_through_the_api() {
    let dir = tmpdir("allfields");
    let app = app(&dir);

    let (status, created) = send(&app, "POST", "/api/tasks", every_field()).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created["task_id"].as_str().unwrap().to_string();

    let (status, info) = send(&app, "GET", &format!("/api/tasks/{id}"), Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{info}");
    let config = &info["config"];

    let sent = every_field();
    // 逐字段比对「发进去的」和「读回来的」。派生字段和会被规整的字段单独说明，
    // 其余必须一模一样。
    for (key, value) in sent.as_object().unwrap() {
        match key.as_str() {
            // 大小字段收人类写法，存的是字节数。
            "max_split" => assert_eq!(config["max_split"], json!(8 * 1024 * 1024)),
            "rate_limit_bps" => assert_eq!(config["rate_limit_bps"], json!(2 * 1024 * 1024)),
            _ => assert_eq!(&config[key], value, "字段 {key} 没有原样存下来"),
        }
    }
    // 派生值：单卷并发 6 × 2 卷。请求里根本没写它。
    assert_eq!(config["max_threads"], json!(12));
    // 导出的就是这份配置，POST 回去能再建一个一样的任务 —— 迁移就靠这个闭环。
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/tasks/{id}/export"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let exported: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(&exported, config, "导出的应当就是任务当前的配置");
    let (status, reimported) = send(&app, "POST", "/api/tasks", exported).await;
    assert_eq!(status, StatusCode::OK, "{reimported}");
}

/// PATCH 也要覆盖全部字段，而且清空语义要明确：`null` 清掉可空字段，空集合清掉
/// 列表 —— 否则「把请求头去掉」这种事只能删了任务重建。
#[tokio::test]
async fn every_field_can_also_be_patched_and_cleared() {
    let dir = tmpdir("patchall");
    let app = app(&dir);

    let (_, created) = send(&app, "POST", "/api/tasks", every_field()).await;
    let id = created["task_id"].as_str().unwrap().to_string();

    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/tasks/{id}"),
        json!({
            "volumes": [["https://other.invalid/f"]],
            "max_per_volume": 2,
            "max_split": 0,
            "cache": false,
            "persist": false,
            "headers": {},
            "name": null,
            "output_filename": null,
            "auto_filename": true,
            "rate_limit_bps": 0,
            "rate_limit_algorithm": "token_bucket",
            "content_disposition": "inline",
            "host_mappings": [],
            "plugins": []
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let config = &body["config"];
    assert_eq!(config["volumes"], json!([["https://other.invalid/f"]]));
    assert_eq!(config["max_per_volume"], json!(2));
    assert_eq!(config["max_threads"], json!(2), "改了单卷上限要重新派生");
    assert_eq!(config["max_split"], json!(0));
    assert_eq!(config["cache"], json!(false));
    assert_eq!(config["persist"], json!(false));
    assert_eq!(config["headers"], json!({}), "空对象 = 清掉所有请求头");
    assert_eq!(config["name"], Value::Null, "null = 清掉任务名");
    assert_eq!(config["output_filename"], Value::Null);
    assert_eq!(config["auto_filename"], json!(true));
    assert_eq!(config["rate_limit_bps"], json!(0));
    assert_eq!(config["rate_limit_algorithm"], "token_bucket");
    assert_eq!(config["content_disposition"], "inline");
    assert_eq!(config["host_mappings"], json!([]));
    assert_eq!(config["plugins"], json!([]));
}

/// 打错的字段名要当场报错，而不是静默用默认值跑。
///
/// 静默忽略是脚本作者最难查的一类问题：`max_treads` 少一个字母，接口回 200，
/// 任务却按默认并发跑，日志里一个字都没有。
#[tokio::test]
async fn typos_in_field_names_are_rejected_instead_of_ignored() {
    let dir = tmpdir("typos");
    let app = app(&dir);

    let (status, body) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"url": "https://a.invalid/f", "max_treads": 32}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let error = body["error"].as_str().unwrap();
    assert!(error.contains("max_treads"), "要点出是哪个字段：{error}");
    assert!(
        error.contains("max_per_volume"),
        "并且要列出认得的字段：{error}"
    );
    assert!(tasks(&app).await.is_empty(), "被拒的请求不该留下任务");

    // `start_cache` 只对创建有意义，出现在 PATCH 里同样要报出来 —— 静默吃掉它
    // 的结果是「PATCH 里写了 start_cache，缓存却没动」。
    let (_, created) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"url": "https://a.invalid/f"}),
    )
    .await;
    let id = created["task_id"].as_str().unwrap();
    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/tasks/{id}"),
        json!({"start_cache": true}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body["error"].as_str().unwrap().contains("start_cache"),
        "{body}"
    );
}

/// 插件配置在**建任务时**就要校验。
///
/// 不校验的话，一条写错的密钥会被原样收下并回 200，然后在某个客户端来播的时候
/// 变成 500 —— 而脚本拿到 200 就认为任务能用了。
#[tokio::test]
async fn a_broken_plugin_config_fails_at_create_time() {
    let dir = tmpdir("plugins");
    let app = app(&dir);

    let bad = json!({
        "url": "https://a.invalid/f",
        "plugins": [{"id": "chacha20", "enabled": true, "config": {"secret": "not-hex"}}]
    });
    let (status, body) = send(&app, "POST", "/api/tasks", bad).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let error = body["error"].as_str().unwrap();
    assert!(error.contains("chacha20"), "要说清是哪个插件：{error}");
    assert!(tasks(&app).await.is_empty());

    // 不存在的插件 id 同样要报（启用状态下）。
    let (status, body) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({
            "url": "https://a.invalid/f",
            "plugins": [{"id": "nope", "enabled": true, "config": {}}]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body["error"].as_str().unwrap().contains("unknown plugin"),
        "{body}"
    );

    // 但**停用**的槽位要放过：那通常是换了个不带该插件的构建，配置留着无害，
    // 而拒绝会让这个任务再也编辑不了。
    let (status, body) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({
            "url": "https://a.invalid/f",
            "plugins": [
                {"id": "nope", "enabled": false, "config": {}},
                {"id": "chacha20", "enabled": false, "config": {"secret": ""}}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// 同一个值不能「POST 放过、PATCH 拒绝」。
///
/// 脚本通常两条路都走（先建好，再按情况调），两边尺度不一致的结果是「一次建好」
/// 和「先建再改」得到两个不同的任务，而报错只在其中一条路上出现。
#[tokio::test]
async fn create_and_patch_apply_the_same_value_checks() {
    let dir = tmpdir("checks");
    let app = app(&dir);

    let (_, created) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({"url": "https://a.invalid/f"}),
    )
    .await;
    let id = created["task_id"].as_str().unwrap().to_string();

    for bad in [
        json!({"max_split": 1000}),   // 手填时下限是 64K
        json!({"max_per_volume": 0}), // 至少要有一个并发
        json!({"host_mappings": [{"from": "a.invalid", "to": "https://b/c"}]}), // 目标不能是 URL
    ] {
        let mut create = bad.as_object().unwrap().clone();
        create.insert("url".into(), json!("https://a.invalid/f"));
        let (status, body) = send(&app, "POST", "/api/tasks", Value::Object(create)).await;
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "POST 应当拒绝 {bad}：{body}"
        );

        let (status, body) = send(&app, "PATCH", &format!("/api/tasks/{id}"), bad.clone()).await;
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "PATCH 也应当拒绝 {bad}：{body}"
        );
    }

    // 只建成了最开始那一个任务：被拒的创建不留痕迹。
    assert_eq!(tasks(&app).await.len(), 1);
}

/// `?start_cache=` 收查询串里常见的几种真假写法。
///
/// 只认 `true` / `false` 的话，`?start_cache=1` 会撞上一句 serde 的
/// 「provided string was not `true` or `false`」—— 而 `1` 恰恰是查询串里最常见的写法。
#[tokio::test]
async fn start_cache_accepts_the_usual_truthy_spellings() {
    let dir = tmpdir("truthy");
    let app = app(&dir);
    let body = json!({"url": "https://origin.invalid/f"});

    // 真值：任务建成，并且**尝试过**缓存（源站是不可达的假地址，所以必然带
    // cache_error —— 有这个字段就证明那一步真的走了）。
    for query in [
        "?start_cache=1",
        "?start_cache=true",
        "?start_cache=yes",
        "?start_cache",
    ] {
        let (status, resp) = send(&app, "POST", &format!("/api/tasks{query}"), body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{query} → {resp}");
        assert_eq!(
            resp["cache_started"],
            json!(false),
            "{query} 应当试过缓存：{resp}"
        );
        assert!(resp["cache_error"].is_string(), "{query} → {resp}");
    }

    // 假值和不写：完全不碰缓存，响应里连这两个字段都不该有。
    for query in [
        "",
        "?start_cache=0",
        "?start_cache=false",
        "?start_cache=off",
    ] {
        let (status, resp) = send(&app, "POST", &format!("/api/tasks{query}"), body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{query} → {resp}");
        assert!(resp.get("cache_started").is_none(), "{query} → {resp}");
        assert!(resp.get("cache_error").is_none(), "{query} → {resp}");
    }
}

/// 既分卷又镜像 —— 文档 §4「URL 怎么写」最后一行那种形状。
///
/// 简写表达不了两层，所以这是二维 `volumes` 存在的理由；而两层的顺序含义完全不同
/// （卷序 = 字节顺序，镜像序只是偏好），写错卷序拼出来的文件是坏的，服务端无从察觉。
#[tokio::test]
async fn volumes_and_mirrors_can_be_combined() {
    let dir = tmpdir("combined");
    let app = app(&dir);

    // 一部电影切两段，每段各有两个 CDN。
    let (status, created) = send(
        &app,
        "POST",
        "/api/tasks",
        json!({
            "volumes": [
                ["https://cdn-a.invalid/movie.part01", "https://cdn-b.invalid/movie.part01"],
                ["https://cdn-a.invalid/movie.part02", "https://cdn-b.invalid/movie.part02"]
            ],
            "max_per_volume": 4
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created["task_id"].as_str().unwrap();

    let (_, info) = send(&app, "GET", &format!("/api/tasks/{id}"), Value::Null).await;
    let config = &info["config"];
    // 两层都原样保留，**顺序一个字节都不能动** —— 卷序决定拼出来的文件对不对。
    assert_eq!(
        config["volumes"],
        json!([
            [
                "https://cdn-a.invalid/movie.part01",
                "https://cdn-b.invalid/movie.part01"
            ],
            [
                "https://cdn-a.invalid/movie.part02",
                "https://cdn-b.invalid/movie.part02"
            ]
        ]),
    );
    // 文档里写的「4 × 2 卷 = 8 条线程」。
    assert_eq!(config["max_threads"], json!(8));
    // 每个去重后的源各有一条健康度记录：2 卷 × 2 镜像 = 4 条。
    assert_eq!(info["url_health"].as_array().unwrap().len(), 4);
}
