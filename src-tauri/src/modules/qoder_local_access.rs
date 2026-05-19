use crate::models::qoder::QoderAccount;
use crate::models::qoder_local_access::{
    QoderLocalAccessCollection, QoderLocalAccessRoutingStrategy, QoderLocalAccessScope,
    QoderLocalAccessState, QoderLocalAccessStats, QoderLocalAccessUsageStats,
};
use crate::modules::atomic_write::write_string_atomic;
use crate::modules::{logger, qoder_account, qoder_oauth};
use futures_util::StreamExt;
use rand::{distributions::Alphanumeric, Rng};
use reqwest::header::{HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex as TokioMutex};
use tokio::time::{timeout, Duration};

const QODER_LOCAL_ACCESS_FILE: &str = "qoder_local_access.json";
const QODER_LOCAL_ACCESS_STATS_FILE: &str = "qoder_local_access_stats.json";
const LOCALHOST_BIND: &str = "127.0.0.1";
const LAN_BIND: &str = "0.0.0.0";
const MAX_HTTP_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(15);
const UPSTREAM_BASE_URL: &str = "https://api2-v2.qoder.sh/model/v1";
const MAX_RETRY_ACCOUNTS: usize = 8;
const GATEWAY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const GATEWAY_PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

const DEFAULT_QODER_MODELS: &[&str] = &["auto", "lite", "performance", "ultimate"];

static GATEWAY_RUNTIME: OnceLock<TokioMutex<GatewayRuntime>> = OnceLock::new();
static ROUND_ROBIN_CURSOR: AtomicUsize = AtomicUsize::new(0);
static HTTP_CLIENT: OnceLock<Mutex<Option<Client>>> = OnceLock::new();

#[derive(Default)]
struct GatewayRuntime {
    loaded: bool,
    collection: Option<QoderLocalAccessCollection>,
    stats: QoderLocalAccessStats,
    stats_dirty: bool,
    running: bool,
    actual_port: Option<u16>,
    actual_bind_host: Option<String>,
    last_error: Option<String>,
    shutdown_sender: Option<watch::Sender<bool>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

fn gateway_runtime() -> &'static TokioMutex<GatewayRuntime> {
    GATEWAY_RUNTIME.get_or_init(|| TokioMutex::new(GatewayRuntime::default()))
}

fn http_client_cache() -> &'static Mutex<Option<Client>> {
    HTTP_CLIENT.get_or_init(|| Mutex::new(None))
}

fn get_http_client() -> Result<Client, String> {
    let mut guard = http_client_cache().lock().unwrap();
    if let Some(c) = guard.as_ref() {
        return Ok(c.clone());
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| format!("构建 HTTP 客户端失败: {}", e))?;
    *guard = Some(client.clone());
    Ok(client)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn local_access_file_path() -> Result<PathBuf, String> {
    let dir = crate::modules::config::get_data_dir()?;
    Ok(dir.join(QODER_LOCAL_ACCESS_FILE))
}

fn local_access_stats_file_path() -> Result<PathBuf, String> {
    let dir = crate::modules::config::get_data_dir()?;
    Ok(dir.join(QODER_LOCAL_ACCESS_STATS_FILE))
}

fn generate_api_key() -> String {
    let random: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    format!("agt_qoder_{}", random)
}

fn allocate_random_port() -> Result<u16, String> {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0u16))
        .map_err(|e| format!("分配随机端口失败: {}", e))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("获取端口失败: {}", e))?
        .port();
    Ok(port)
}

fn bind_host_for_scope(scope: QoderLocalAccessScope) -> &'static str {
    match scope {
        QoderLocalAccessScope::Localhost => LOCALHOST_BIND,
        QoderLocalAccessScope::Lan => LAN_BIND,
    }
}

// ─── 配置加载/保存 ───

fn load_collection_from_disk() -> Option<QoderLocalAccessCollection> {
    let path = local_access_file_path().ok()?;
    let content = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

fn save_collection_to_disk(collection: &QoderLocalAccessCollection) -> Result<(), String> {
    let path = local_access_file_path()?;
    let json = serde_json::to_string_pretty(collection)
        .map_err(|e| format!("序列化配置失败: {}", e))?;
    write_string_atomic(&path, &json)
}

fn load_stats_from_disk() -> QoderLocalAccessStats {
    let path = match local_access_stats_file_path() {
        Ok(p) => p,
        Err(_) => return QoderLocalAccessStats::default(),
    };
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return QoderLocalAccessStats::default(),
    };
    serde_json::from_str(&content).unwrap_or_default()
}

fn save_stats_to_disk(stats: &QoderLocalAccessStats) -> Result<(), String> {
    let path = local_access_stats_file_path()?;
    let json =
        serde_json::to_string_pretty(stats).map_err(|e| format!("序列化统计失败: {}", e))?;
    write_string_atomic(&path, &json)
}

async fn ensure_runtime_loaded() {
    let mut rt = gateway_runtime().lock().await;
    if rt.loaded {
        return;
    }
    rt.loaded = true;
    rt.collection = load_collection_from_disk();
    rt.stats = load_stats_from_disk();
    if rt.stats.since == 0 {
        rt.stats.since = now_ms();
    }
}

fn build_state_snapshot(rt: &GatewayRuntime) -> QoderLocalAccessState {
    let base_url = if rt.running {
        rt.actual_port.map(|p| {
            let host = rt.actual_bind_host.as_deref().unwrap_or(LOCALHOST_BIND);
            let display_host = if host == LAN_BIND { LOCALHOST_BIND } else { host };
            format!("http://{}:{}/v1", display_host, p)
        })
    } else {
        None
    };
    let member_count = rt
        .collection
        .as_ref()
        .map(|c| c.account_ids.len())
        .unwrap_or(0);
    QoderLocalAccessState {
        collection: rt.collection.clone(),
        running: rt.running,
        base_url,
        model_ids: DEFAULT_QODER_MODELS.iter().map(|s| s.to_string()).collect(),
        last_error: rt.last_error.clone(),
        member_count,
        stats: rt.stats.clone(),
    }
}

// ─── Token 提取 ───

fn extract_access_token(account: &QoderAccount) -> Option<String> {
    let user_info = account.auth_user_info_raw.as_ref()?;
    let paths: &[&[&str]] = &[
        &["token"],
        &["securityOauthToken"],
        &["accessToken"],
        &["access_token"],
        &["result", "token"],
        &["data", "token"],
        &["result", "accessToken"],
        &["data", "accessToken"],
    ];
    for path in paths {
        let mut current = user_info;
        let mut found = true;
        for key in *path {
            match current.get(*key) {
                Some(v) => current = v,
                None => {
                    found = false;
                    break;
                }
            }
        }
        if found {
            if let Some(s) = current.as_str() {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

// ─── 账号选择（Round-Robin） ───

fn select_account_ids(
    collection: &QoderLocalAccessCollection,
    skip: &[String],
) -> Vec<String> {
    let candidates: Vec<&String> = collection
        .account_ids
        .iter()
        .filter(|id| !skip.contains(id))
        .collect();
    if candidates.is_empty() {
        return Vec::new();
    }
    let cursor = ROUND_ROBIN_CURSOR.fetch_add(1, Ordering::Relaxed);
    let start = cursor % candidates.len();
    let mut result = Vec::with_capacity(candidates.len());
    for i in 0..candidates.len() {
        result.push(candidates[(start + i) % candidates.len()].clone());
    }
    result
}

// ─── 请求透传 ───

struct ProxyResult {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    is_stream: bool,
}

async fn proxy_to_upstream(
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
    access_token: &str,
) -> Result<ProxyResult, String> {
    let client = get_http_client()?;
    let url = format!("{}{}", UPSTREAM_BASE_URL, path);

    let mut req = if method.eq_ignore_ascii_case("POST") {
        client.post(&url)
    } else {
        client.get(&url)
    };

    req = req.header(AUTHORIZATION, format!("Bearer {}", access_token));
    req = req.header(CONTENT_TYPE, "application/json");

    // 透传部分请求头
    for (name, value) in headers {
        let lower = name.to_lowercase();
        if lower == "authorization"
            || lower == "host"
            || lower == "content-length"
            || lower == "connection"
            || lower == "accept-encoding"
            || lower == "x-api-key"
        {
            continue;
        }
        if let (Ok(hn), Ok(hv)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            req = req.header(hn, hv);
        }
    }

    if !body.is_empty() && method.eq_ignore_ascii_case("POST") {
        req = req.body(body.to_vec());
    }

    let resp = req
        .send()
        .await
        .map_err(|e| format!("上游请求失败: {}", e))?;

    let status = resp.status().as_u16();
    let mut resp_headers = Vec::new();
    let is_stream = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false);

    for (name, value) in resp.headers() {
        if let Ok(v) = value.to_str() {
            resp_headers.push((name.to_string(), v.to_string()));
        }
    }

    let resp_body = resp
        .bytes()
        .await
        .map_err(|e| format!("读取上游响应失败: {}", e))?
        .to_vec();

    Ok(ProxyResult {
        status,
        headers: resp_headers,
        body: resp_body,
        is_stream,
    })
}

// ─── HTTP 请求解析 ───

struct ParsedRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

async fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + REQUEST_READ_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("请求读取超时".into());
        }
        let n = match timeout(remaining, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => return Err("连接关闭".into()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(format!("读取错误: {}", e)),
            Err(_) => return Err("请求读取超时".into()),
        };
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HTTP_REQUEST_BYTES {
            return Err("请求体过大".into());
        }
        // 检查是否读完 headers
        if let Some(header_end) = find_header_end(&buf) {
            let content_length = parse_content_length(&buf[..header_end]);
            let total = header_end + 4 + content_length;
            if buf.len() >= total {
                return Ok(buf);
            }
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
}

fn parse_content_length(header_bytes: &[u8]) -> usize {
    let header_str = String::from_utf8_lossy(header_bytes);
    for line in header_str.lines() {
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(val) = line.split(':').nth(1) {
                return val.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

fn parse_http_request(raw: &[u8]) -> Result<ParsedRequest, String> {
    let header_end = find_header_end(raw).ok_or("无效 HTTP 请求")?;
    let header_str = String::from_utf8_lossy(&raw[..header_end]);
    let mut lines = header_str.lines();
    let request_line = lines.next().ok_or("缺少请求行")?;
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("无效请求行".into());
    }
    let method = parts[0].to_string();
    let target = parts[1].to_string();
    let mut headers = Vec::new();
    for line in lines {
        if let Some(colon) = line.find(':') {
            let name = line[..colon].trim().to_string();
            let value = line[colon + 1..].trim().to_string();
            headers.push((name, value));
        }
    }
    let body = raw[header_end + 4..].to_vec();
    Ok(ParsedRequest { method, target, headers, body })
}

fn extract_api_key(headers: &[(String, String)]) -> Option<String> {
    for (name, value) in headers {
        let lower = name.to_lowercase();
        if lower == "authorization" {
            if let Some(token) = value.strip_prefix("Bearer ") {
                return Some(token.trim().to_string());
            }
        }
        if lower == "x-api-key" {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn is_stream_request(body: &[u8]) -> bool {
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false)
    } else {
        false
    }
}

// ─── 连接处理 ───

async fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    let raw = read_http_request(&mut stream).await?;
    let parsed = parse_http_request(&raw)?;

    // OPTIONS
    if parsed.method.eq_ignore_ascii_case("OPTIONS") {
        let resp = b"HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Authorization, Content-Type, X-API-Key\r\nAccess-Control-Max-Age: 86400\r\n\r\n";
        stream.write_all(resp).await.ok();
        return Ok(());
    }

    if !parsed.method.eq_ignore_ascii_case("GET") && !parsed.method.eq_ignore_ascii_case("POST") {
        write_error(&mut stream, 405, "Method Not Allowed").await;
        return Ok(());
    }

    if !parsed.target.starts_with("/v1/") {
        write_error(&mut stream, 404, "Not Found").await;
        return Ok(());
    }

    // 验证 API Key
    let Some(api_key) = extract_api_key(&parsed.headers) else {
        write_error(&mut stream, 401, "缺少 Authorization Bearer 或 X-API-Key").await;
        return Ok(());
    };

    let (collection, _running) = {
        let rt = gateway_runtime().lock().await;
        let col = rt.collection.clone();
        let running = rt.running;
        (col, running)
    };

    let Some(collection) = collection else {
        write_error(&mut stream, 503, "服务未配置").await;
        return Ok(());
    };

    if !collection.enabled {
        write_error(&mut stream, 503, "服务未启用").await;
        return Ok(());
    }

    if api_key != collection.api_key {
        write_error(&mut stream, 401, "API Key 无效").await;
        return Ok(());
    }

    // /v1/models
    if parsed.target == "/v1/models" || parsed.target.starts_with("/v1/models?") {
        let models_resp = build_models_response();
        write_json_response(&mut stream, 200, &models_resp).await;
        return Ok(());
    }

    // /v1/chat/completions - 透传
    if !parsed.target.starts_with("/v1/chat/completions") {
        write_error(&mut stream, 404, "仅支持 /v1/models 和 /v1/chat/completions").await;
        return Ok(());
    }

    // 多账号轮询
    let start_time = Instant::now();
    let mut tried: Vec<String> = Vec::new();
    let max_tries = collection.account_ids.len().min(MAX_RETRY_ACCOUNTS);

    for _ in 0..max_tries {
        let candidates = select_account_ids(&collection, &tried);
        let Some(account_id) = candidates.first() else {
            break;
        };
        tried.push(account_id.clone());

        let account = match qoder_account::load_account(account_id) {
            Some(a) => a,
            None => continue,
        };

        let Some(token) = extract_access_token(&account) else {
            logger::log_warn(&format!(
                "[QoderLocalAccess] 账号 {} 无有效 token，跳过",
                account_id
            ));
            continue;
        };

        let upstream_path = &parsed.target[3..]; // 去掉 /v1 前缀
        match proxy_to_upstream(
            &parsed.method,
            upstream_path,
            &parsed.headers,
            &parsed.body,
            &token,
        )
        .await
        {
            Ok(result) => {
                if result.status == 401 {
                    logger::log_warn(&format!(
                        "[QoderLocalAccess] 账号 {} token 已过期 (401)，跳过",
                        account_id
                    ));
                    continue;
                }
                if result.status == 429 {
                    logger::log_warn(&format!(
                        "[QoderLocalAccess] 账号 {} 配额耗尽 (429)，跳过",
                        account_id
                    ));
                    continue;
                }

                // 成功，写回响应
                write_upstream_response(&mut stream, &result).await;
                record_usage(true, start_time.elapsed().as_millis() as u64).await;
                return Ok(());
            }
            Err(e) => {
                logger::log_warn(&format!(
                    "[QoderLocalAccess] 账号 {} 请求失败: {}",
                    account_id, e
                ));
                continue;
            }
        }
    }

    record_usage(false, start_time.elapsed().as_millis() as u64).await;
    write_error(&mut stream, 502, "所有账号均不可用").await;
    Ok(())
}

fn build_models_response() -> Value {
    let models: Vec<Value> = DEFAULT_QODER_MODELS
        .iter()
        .map(|id| {
            json!({
                "id": id,
                "object": "model",
                "created": 1700000000i64,
                "owned_by": "qoder"
            })
        })
        .collect();
    json!({ "object": "list", "data": models })
}

async fn write_error(stream: &mut TcpStream, status: u16, message: &str) {
    let body = serde_json::to_vec(&json!({
        "error": { "message": message, "type": "qoder_local_access_error", "code": status }
    }))
    .unwrap_or_default();
    let resp = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n",
        status,
        status_text(status),
        body.len()
    );
    stream.write_all(resp.as_bytes()).await.ok();
    stream.write_all(&body).await.ok();
}

async fn write_json_response(stream: &mut TcpStream, status: u16, value: &Value) {
    let body = serde_json::to_vec(value).unwrap_or_default();
    let resp = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n",
        status,
        status_text(status),
        body.len()
    );
    stream.write_all(resp.as_bytes()).await.ok();
    stream.write_all(&body).await.ok();
}

async fn write_upstream_response(stream: &mut TcpStream, result: &ProxyResult) {
    let mut header_str = format!("HTTP/1.1 {} {}\r\n", result.status, status_text(result.status));
    header_str.push_str("Access-Control-Allow-Origin: *\r\n");
    for (name, value) in &result.headers {
        let lower = name.to_lowercase();
        if lower == "transfer-encoding" || lower == "connection" || lower == "access-control-allow-origin" {
            continue;
        }
        header_str.push_str(&format!("{}: {}\r\n", name, value));
    }
    header_str.push_str(&format!("Content-Length: {}\r\n", result.body.len()));
    header_str.push_str("\r\n");
    stream.write_all(header_str.as_bytes()).await.ok();
    stream.write_all(&result.body).await.ok();
}

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

async fn record_usage(success: bool, latency_ms: u64) {
    let mut rt = gateway_runtime().lock().await;
    rt.stats.totals.request_count += 1;
    if success {
        rt.stats.totals.success_count += 1;
    } else {
        rt.stats.totals.failure_count += 1;
    }
    rt.stats.totals.total_latency_ms += latency_ms;
    rt.stats.updated_at = now_ms();
    rt.stats_dirty = true;
}

// ─── 网关启停 ───

async fn start_gateway(bind_host: &str, port: u16) -> Result<(), String> {
    let listener = TcpListener::bind((bind_host, port))
        .await
        .map_err(|e| format!("绑定 {}:{} 失败: {}", bind_host, port, e))?;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let actual_port = listener
        .local_addr()
        .map(|a| a.port())
        .unwrap_or(port);

    {
        let mut rt = gateway_runtime().lock().await;
        rt.running = true;
        rt.actual_port = Some(actual_port);
        rt.actual_bind_host = Some(bind_host.to_string());
        rt.shutdown_sender = Some(shutdown_tx);
        rt.last_error = None;
    }

    logger::log_info(&format!(
        "[QoderLocalAccess] 本地接入服务已启动: bind={}:{} base=http://{}:{}/v1",
        bind_host, actual_port, LOCALHOST_BIND, actual_port
    ));

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream).await {
                                    logger::log_warn(&format!("[QoderLocalAccess] 连接处理错误: {}", e));
                                }
                            });
                        }
                        Err(e) => {
                            logger::log_warn(&format!("[QoderLocalAccess] accept 错误: {}", e));
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
        logger::log_info("[QoderLocalAccess] 网关已停止");
    });

    {
        let mut rt = gateway_runtime().lock().await;
        rt.task = Some(task);
    }

    Ok(())
}

async fn stop_gateway() {
    let (sender, task) = {
        let mut rt = gateway_runtime().lock().await;
        rt.running = false;
        rt.actual_port = None;
        rt.actual_bind_host = None;
        let sender = rt.shutdown_sender.take();
        let task = rt.task.take();
        (sender, task)
    };

    if let Some(tx) = sender {
        tx.send(true).ok();
    }
    if let Some(t) = task {
        let _ = timeout(GATEWAY_SHUTDOWN_TIMEOUT, t).await;
    }
}

async fn ensure_gateway_running() -> Result<(), String> {
    let rt = gateway_runtime().lock().await;
    let Some(collection) = rt.collection.as_ref() else {
        return Ok(());
    };
    if !collection.enabled {
        return Ok(());
    }
    let port = collection.port;
    let scope = collection.access_scope;
    let already_running = rt.running;
    let current_port = rt.actual_port;
    let current_host = rt.actual_bind_host.clone();
    drop(rt);

    let bind_host = bind_host_for_scope(scope);

    if already_running
        && current_port == Some(port)
        && current_host.as_deref() == Some(bind_host)
    {
        return Ok(());
    }

    if already_running {
        stop_gateway().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    start_gateway(bind_host, port).await
}

// ─── 公开 API ───

pub async fn get_local_access_state() -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let rt = gateway_runtime().lock().await;
    Ok(build_state_snapshot(&rt))
}

pub async fn set_local_access_enabled(enabled: bool) -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;

    if let Some(ref mut col) = rt.collection {
        col.enabled = enabled;
        col.updated_at = now_ms();
        save_collection_to_disk(col)?;
    } else if enabled {
        // 首次启用，创建默认配置
        let port = allocate_random_port()?;
        let col = QoderLocalAccessCollection {
            enabled: true,
            port,
            api_key: generate_api_key(),
            access_scope: QoderLocalAccessScope::Localhost,
            routing_strategy: QoderLocalAccessRoutingStrategy::Auto,
            account_ids: Vec::new(),
            created_at: now_ms(),
            updated_at: now_ms(),
        };
        save_collection_to_disk(&col)?;
        rt.collection = Some(col);
    }

    let state = build_state_snapshot(&rt);
    drop(rt);

    if enabled {
        ensure_gateway_running().await?;
    } else {
        stop_gateway().await;
    }

    get_local_access_state().await
}

pub async fn save_local_access_accounts(
    account_ids: Vec<String>,
) -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(ref mut col) = rt.collection else {
        return Err("服务未配置，请先启用".into());
    };
    col.account_ids = account_ids;
    col.updated_at = now_ms();
    save_collection_to_disk(col)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn remove_local_access_account(
    account_id: &str,
) -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(ref mut col) = rt.collection else {
        return Err("服务未配置".into());
    };
    col.account_ids.retain(|id| id != account_id);
    col.updated_at = now_ms();
    save_collection_to_disk(col)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn rotate_local_access_api_key() -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(ref mut col) = rt.collection else {
        return Err("服务未配置".into());
    };
    col.api_key = generate_api_key();
    col.updated_at = now_ms();
    save_collection_to_disk(col)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn update_local_access_port(port: u16) -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    {
        let mut rt = gateway_runtime().lock().await;
        let Some(ref mut col) = rt.collection else {
            return Err("服务未配置".into());
        };
        col.port = port;
        col.updated_at = now_ms();
        save_collection_to_disk(col)?;
    }
    ensure_gateway_running().await?;
    get_local_access_state().await
}

pub async fn update_local_access_routing_strategy(
    strategy: QoderLocalAccessRoutingStrategy,
) -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(ref mut col) = rt.collection else {
        return Err("服务未配置".into());
    };
    col.routing_strategy = strategy;
    col.updated_at = now_ms();
    save_collection_to_disk(col)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn update_local_access_scope(
    scope: QoderLocalAccessScope,
) -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    {
        let mut rt = gateway_runtime().lock().await;
        let Some(ref mut col) = rt.collection else {
            return Err("服务未配置".into());
        };
        col.access_scope = scope;
        col.updated_at = now_ms();
        save_collection_to_disk(col)?;
    }
    ensure_gateway_running().await?;
    get_local_access_state().await
}

pub async fn clear_local_access_stats() -> Result<QoderLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    rt.stats = QoderLocalAccessStats {
        since: now_ms(),
        ..Default::default()
    };
    rt.stats_dirty = true;
    let _ = save_stats_to_disk(&rt.stats);
    Ok(build_state_snapshot(&rt))
}

pub async fn restore_local_access_gateway() {
    ensure_runtime_loaded().await;
    let enabled = {
        let rt = gateway_runtime().lock().await;
        rt.collection.as_ref().map(|c| c.enabled).unwrap_or(false)
    };
    if enabled {
        if let Err(e) = ensure_gateway_running().await {
            logger::log_warn(&format!("[QoderLocalAccess] 恢复网关失败: {}", e));
            let mut rt = gateway_runtime().lock().await;
            rt.last_error = Some(e);
        }
    }
}
