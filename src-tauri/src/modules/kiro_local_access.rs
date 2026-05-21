use crate::models::kiro::KiroAccount;
use crate::models::kiro_local_access::{
    KiroLocalAccessCollection, KiroLocalAccessState, KiroLocalAccessStats,
    KiroLocalAccessTestFailure, KiroLocalAccessTestResult,
};
use crate::modules::atomic_write::write_string_atomic;
use crate::modules::{kiro_account, logger};
use chrono::{SecondsFormat, TimeZone};
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex as TokioMutex};
use tokio::time::{timeout, Duration};

const KIRO_LOCAL_ACCESS_FILE: &str = "kiro_local_access.json";
const KIRO_LOCAL_ACCESS_STATS_FILE: &str = "kiro_local_access_stats.json";
const LOCALHOST_BIND: &str = "127.0.0.1";
const MAX_HTTP_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(15);
const GATEWAY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const KNOWN_AUTH_KEYS: [&str; 3] = [
    "kirocli:social:token",
    "kirocli:external-idp:token",
    "kirocli:odic:token",
];
const PROFILE_STATE_KEY: &str = "api.codewhisperer.profile";
const SOCIAL_AUTH_KEY: &str = "kirocli:social:token";
const EXTERNAL_IDP_AUTH_KEY: &str = "kirocli:external-idp:token";
const OIDC_AUTH_KEY: &str = "kirocli:odic:token";
const DEFAULT_KIRO_MODELS: &[&str] = &["auto"];

static GATEWAY_RUNTIME: OnceLock<TokioMutex<GatewayRuntime>> = OnceLock::new();
static ROUND_ROBIN_CURSOR: AtomicUsize = AtomicUsize::new(0);
static REQUEST_LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();
static ANSI_ESCAPE_REGEX: OnceLock<Regex> = OnceLock::new();
static OSC_ESCAPE_REGEX: OnceLock<Regex> = OnceLock::new();

#[derive(Default)]
struct GatewayRuntime {
    loaded: bool,
    collection: Option<KiroLocalAccessCollection>,
    stats: KiroLocalAccessStats,
    running: bool,
    actual_port: Option<u16>,
    last_error: Option<String>,
    shutdown_sender: Option<watch::Sender<bool>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
struct ParsedRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct CompletionRequest {
    model: String,
    stream: bool,
    prompt: String,
}

#[derive(Debug)]
struct ProxyResult {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    is_stream: bool,
}

#[derive(Debug, Default)]
struct KiroCliDbSnapshot {
    auth_values: HashMap<String, Option<String>>,
    profile_value: Option<String>,
}

fn gateway_runtime() -> &'static TokioMutex<GatewayRuntime> {
    GATEWAY_RUNTIME.get_or_init(|| TokioMutex::new(GatewayRuntime::default()))
}

fn request_lock() -> &'static TokioMutex<()> {
    REQUEST_LOCK.get_or_init(|| TokioMutex::new(()))
}

fn ansi_escape_regex() -> &'static Regex {
    ANSI_ESCAPE_REGEX.get_or_init(|| {
        Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").expect("ansi escape regex should compile")
    })
}

fn osc_escape_regex() -> &'static Regex {
    OSC_ESCAPE_REGEX.get_or_init(|| {
        Regex::new(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")
            .expect("osc escape regex should compile")
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn local_access_file_path() -> Result<PathBuf, String> {
    let dir = crate::modules::config::get_data_dir()?;
    Ok(dir.join(KIRO_LOCAL_ACCESS_FILE))
}

fn local_access_stats_file_path() -> Result<PathBuf, String> {
    let dir = crate::modules::config::get_data_dir()?;
    Ok(dir.join(KIRO_LOCAL_ACCESS_STATS_FILE))
}

fn generate_api_key() -> String {
    let random: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    format!("agt_kiro_{}", random)
}

fn allocate_random_port() -> Result<u16, String> {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0u16))
        .map_err(|e| format!("分配随机端口失败: {}", e))?;
    listener
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|e| format!("读取端口失败: {}", e))
}

fn load_collection_from_disk() -> Option<KiroLocalAccessCollection> {
    let path = local_access_file_path().ok()?;
    let content = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

fn save_collection_to_disk(collection: &KiroLocalAccessCollection) -> Result<(), String> {
    let path = local_access_file_path()?;
    let json = serde_json::to_string_pretty(collection)
        .map_err(|e| format!("序列化配置失败: {}", e))?;
    write_string_atomic(&path, &json)
}

fn load_stats_from_disk() -> KiroLocalAccessStats {
    let path = match local_access_stats_file_path() {
        Ok(path) => path,
        Err(_) => return KiroLocalAccessStats::default(),
    };
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(_) => return KiroLocalAccessStats::default(),
    };
    serde_json::from_str(&content).unwrap_or_default()
}

fn save_stats_to_disk(stats: &KiroLocalAccessStats) -> Result<(), String> {
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

fn build_state_snapshot(rt: &GatewayRuntime) -> KiroLocalAccessState {
    KiroLocalAccessState {
        collection: rt.collection.clone(),
        running: rt.running,
        base_url: rt
            .actual_port
            .map(|port| format!("http://{}:{}/v1", LOCALHOST_BIND, port)),
        model_ids: DEFAULT_KIRO_MODELS
            .iter()
            .map(|item| item.to_string())
            .collect(),
        last_error: rt.last_error.clone(),
        member_count: rt
            .collection
            .as_ref()
            .map(|collection| collection.account_ids.len())
            .unwrap_or(0),
        stats: rt.stats.clone(),
    }
}

fn build_test_failure(
    title: &str,
    stage: &str,
    cause: &str,
    suggestion: &str,
    status: Option<u16>,
    model_id: Option<String>,
    detail: Option<String>,
) -> KiroLocalAccessTestFailure {
    KiroLocalAccessTestFailure {
        title: title.to_string(),
        stage: stage.to_string(),
        cause: cause.to_string(),
        suggestion: suggestion.to_string(),
        status,
        model_id,
        detail,
    }
}

fn build_failure_result(failure: KiroLocalAccessTestFailure) -> KiroLocalAccessTestResult {
    KiroLocalAccessTestResult {
        model_id: failure.model_id.clone(),
        latency_ms: None,
        output: None,
        failure: Some(failure),
    }
}

fn truncate_detail(input: &str, max_chars: usize) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut result = String::new();
    let mut count = 0usize;
    for ch in trimmed.chars() {
        if count >= max_chars {
            result.push('…');
            break;
        }
        result.push(ch);
        count += 1;
    }
    Some(result)
}

fn extract_completion_text(body: &Value) -> Option<String> {
    body.get("choices")
        .and_then(|value| value.as_array())
        .and_then(|items| items.first())
        .and_then(|item| item.get("message"))
        .and_then(|item| item.get("content"))
        .and_then(|item| item.as_str())
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn dedupe_account_ids(account_ids: Vec<String>) -> Vec<String> {
    let mut result = Vec::new();
    for account_id in account_ids {
        let trimmed = account_id.trim();
        if trimmed.is_empty() || result.iter().any(|item| item == trimmed) {
            continue;
        }
        result.push(trimmed.to_string());
    }
    result
}

fn select_account_ids(collection: &KiroLocalAccessCollection, skip: &[String]) -> Vec<String> {
    let candidates: Vec<&String> = collection
        .account_ids
        .iter()
        .filter(|account_id| !skip.contains(account_id))
        .collect();
    if candidates.is_empty() {
        return Vec::new();
    }

    let cursor = ROUND_ROBIN_CURSOR.fetch_add(1, Ordering::Relaxed);
    let start = cursor % candidates.len();
    let mut ordered = Vec::with_capacity(candidates.len());
    for index in 0..candidates.len() {
        ordered.push(candidates[(start + index) % candidates.len()].clone());
    }
    ordered
}

async fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + REQUEST_READ_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("请求读取超时".to_string());
        }
        let n = match timeout(remaining, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => return Err("连接已关闭".to_string()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(format!("读取请求失败: {}", e)),
            Err(_) => return Err("请求读取超时".to_string()),
        };
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HTTP_REQUEST_BYTES {
            return Err("请求体过大".to_string());
        }

        if let Some(header_end) = find_header_end(&buf) {
            let content_length = parse_content_length(&buf[..header_end]);
            let total_size = header_end + 4 + content_length;
            if buf.len() >= total_size {
                return Ok(buf);
            }
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(header_bytes: &[u8]) -> usize {
    let header_text = String::from_utf8_lossy(header_bytes);
    for line in header_text.lines() {
        if line.to_ascii_lowercase().starts_with("content-length:") {
            if let Some(value) = line.split(':').nth(1) {
                return value.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

fn parse_http_request(raw: &[u8]) -> Result<ParsedRequest, String> {
    let header_end = find_header_end(raw).ok_or_else(|| "无效 HTTP 请求".to_string())?;
    let header_text = String::from_utf8_lossy(&raw[..header_end]);
    let mut lines = header_text.lines();
    let request_line = lines.next().ok_or_else(|| "缺少请求行".to_string())?;
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("请求行格式无效".to_string());
    }

    let mut headers = Vec::new();
    for line in lines {
        if let Some(colon) = line.find(':') {
            headers.push((
                line[..colon].trim().to_string(),
                line[colon + 1..].trim().to_string(),
            ));
        }
    }

    Ok(ParsedRequest {
        method: parts[0].to_string(),
        target: parts[1].to_string(),
        headers,
        body: raw[header_end + 4..].to_vec(),
    })
}

fn extract_api_key(headers: &[(String, String)]) -> Option<String> {
    for (name, value) in headers {
        match name.to_ascii_lowercase().as_str() {
            "authorization" => {
                if let Some(token) = value.strip_prefix("Bearer ") {
                    return Some(token.trim().to_string());
                }
            }
            "x-api-key" => return Some(value.trim().to_string()),
            _ => {}
        }
    }
    None
}

fn build_models_response() -> Value {
    json!({
        "object": "list",
        "data": DEFAULT_KIRO_MODELS.iter().map(|id| json!({
            "id": id,
            "object": "model",
            "created": 1700000000i64,
            "owned_by": "kiro"
        })).collect::<Vec<Value>>()
    })
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

async fn write_error(stream: &mut TcpStream, status: u16, message: &str) {
    let body = serde_json::to_vec(&json!({
        "error": {
            "message": message,
            "type": "kiro_local_access_error",
            "code": status
        }
    }))
    .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Authorization, Content-Type, X-API-Key\r\nContent-Length: {}\r\n\r\n",
        status,
        status_text(status),
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.write_all(&body).await;
}

async fn write_json_response(stream: &mut TcpStream, status: u16, value: &Value) {
    let body = serde_json::to_vec(value).unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Authorization, Content-Type, X-API-Key\r\nContent-Length: {}\r\n\r\n",
        status,
        status_text(status),
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.write_all(&body).await;
}

async fn write_upstream_response(stream: &mut TcpStream, result: &ProxyResult) {
    let mut response = format!("HTTP/1.1 {} {}\r\n", result.status, status_text(result.status));
    response.push_str("Access-Control-Allow-Origin: *\r\n");
    response.push_str("Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n");
    response.push_str("Access-Control-Allow-Headers: Authorization, Content-Type, X-API-Key\r\n");

    if result.is_stream {
        response.push_str("Content-Type: text/event-stream\r\n");
        response.push_str("Cache-Control: no-cache\r\n");
        response.push_str("Connection: close\r\n\r\n");
    } else {
        for (name, value) in &result.headers {
            let lower = name.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "connection" | "transfer-encoding" | "access-control-allow-origin"
            ) {
                continue;
            }
            response.push_str(&format!("{}: {}\r\n", name, value));
        }
        response.push_str(&format!("Content-Length: {}\r\n\r\n", result.body.len()));
    }

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.write_all(&result.body).await;
}

fn normalize_text(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn extract_text_part(part: &Value) -> Option<String> {
    match part {
        Value::String(text) => normalize_text(text),
        Value::Object(obj) => {
            let part_type = obj
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            match part_type {
                "text" | "input_text" | "output_text" => obj
                    .get("text")
                    .and_then(|value| value.as_str())
                    .and_then(normalize_text),
                "image_url" | "input_image" => Some("[图片内容已省略]".to_string()),
                _ => obj
                    .get("text")
                    .and_then(|value| value.as_str())
                    .and_then(normalize_text),
            }
        }
        _ => None,
    }
}

fn message_content_to_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.trim().to_string(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(extract_text_part)
            .collect::<Vec<String>>()
            .join("\n"),
        Value::Object(obj) => obj
            .get("text")
            .and_then(|value| value.as_str())
            .map(|text| text.trim().to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn role_label(role: &str) -> &str {
    match role {
        "system" => "System",
        "assistant" => "Assistant",
        "tool" => "Tool",
        _ => "User",
    }
}

fn build_prompt_from_messages(messages: &Value) -> Result<String, String> {
    let items = messages
        .as_array()
        .ok_or_else(|| "messages 必须是数组".to_string())?;

    let mut system_segments = Vec::new();
    let mut transcript = Vec::new();

    for item in items {
        let role = item
            .get("role")
            .and_then(|value| value.as_str())
            .unwrap_or("user");
        let content = message_content_to_text(item.get("content").unwrap_or(&Value::Null));
        if content.trim().is_empty() {
            continue;
        }

        if role == "system" {
            system_segments.push(content);
        } else {
            transcript.push(format!("{}:\n{}", role_label(role), content));
        }
    }

    if transcript.is_empty() {
        return Err("messages 中没有可用的文本内容".to_string());
    }

    let mut prompt = String::new();
    if !system_segments.is_empty() {
        prompt.push_str("System instructions:\n");
        prompt.push_str(&system_segments.join("\n\n"));
        prompt.push_str("\n\n");
    }
    prompt.push_str("Conversation:\n");
    prompt.push_str(&transcript.join("\n\n"));
    prompt.push_str("\n\nRespond as the assistant to the final user message. Keep prior context when it matters.");
    Ok(prompt)
}

fn parse_completion_request(body: &[u8]) -> Result<CompletionRequest, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("解析请求体失败: {}", e))?;
    let model = value
        .get("model")
        .and_then(|item| item.as_str())
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .unwrap_or_else(|| "auto".to_string());
    let stream = value
        .get("stream")
        .and_then(|item| item.as_bool())
        .unwrap_or(false);
    let messages = value
        .get("messages")
        .ok_or_else(|| "缺少 messages 字段".to_string())?;
    let prompt = build_prompt_from_messages(messages)?;
    Ok(CompletionRequest {
        model,
        stream,
        prompt,
    })
}

fn chunk_text_for_stream(text: &str, max_chars: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }

    let limit = max_chars.max(1);
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;

    for ch in text.chars() {
        current.push(ch);
        current_chars += 1;
        if current_chars >= limit {
            chunks.push(current);
            current = String::new();
            current_chars = 0;
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn build_completion_response(model: &str, content: &str, stream: bool) -> ProxyResult {
    let request_id = format!(
        "chatcmpl-{}",
        uuid::Uuid::new_v4()
            .to_string()
            .replace('-', "")
            .chars()
            .take(24)
            .collect::<String>()
    );
    let created = chrono::Utc::now().timestamp();

    if stream {
        let mut body = String::new();
        let role_chunk = json!({
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": { "role": "assistant" },
                "finish_reason": null
            }]
        });
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&role_chunk).unwrap_or_default());
        body.push_str("\n\n");

        for chunk in chunk_text_for_stream(content, 160) {
            let item = json!({
                "id": request_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": { "content": chunk },
                    "finish_reason": null
                }]
            });
            body.push_str("data: ");
            body.push_str(&serde_json::to_string(&item).unwrap_or_default());
            body.push_str("\n\n");
        }

        let stop_chunk = json!({
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }]
        });
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&stop_chunk).unwrap_or_default());
        body.push_str("\n\n");
        body.push_str("data: [DONE]\n\n");

        ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "text/event-stream".to_string())],
            body: body.into_bytes(),
            is_stream: true,
        }
    } else {
        let body = json!({
            "id": request_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            }
        });
        ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: serde_json::to_vec(&body).unwrap_or_default(),
            is_stream: false,
        }
    }
}

fn get_path_value<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = root;
    for segment in path {
        current = current.as_object()?.get(*segment)?;
    }
    Some(current)
}

fn pick_string(root: Option<&Value>, paths: &[&[&str]]) -> Option<String> {
    let root = root?;
    for path in paths {
        if let Some(value) = get_path_value(root, path) {
            if let Some(text) = value.as_str() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

fn normalize_ascii_lower(value: Option<&str>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_ascii_lowercase())
        }
    })
}

fn extract_profile_arn(account: &KiroAccount) -> Option<String> {
    pick_string(
        account.kiro_profile_raw.as_ref(),
        &[
            &["arn"],
            &["profileArn"],
            &["profile", "arn"],
            &["account", "arn"],
        ],
    )
    .or_else(|| {
        pick_string(
            account.kiro_auth_token_raw.as_ref(),
            &[&["profileArn"], &["profile_arn"], &["arn"]],
        )
    })
}

fn resolve_auth_storage_key(account: &KiroAccount) -> &'static str {
    let auth_method = normalize_ascii_lower(
        pick_string(
            account.kiro_auth_token_raw.as_ref(),
            &[&["authMethod"], &["auth_method"]],
        )
        .as_deref(),
    );
    if auth_method.as_deref() == Some("idc") {
        return OIDC_AUTH_KEY;
    }

    let provider = normalize_ascii_lower(
        pick_string(
            account.kiro_auth_token_raw.as_ref(),
            &[&["provider"], &["loginProvider"], &["login_option"]],
        )
        .as_deref()
        .or(account.login_provider.as_deref()),
    );

    match provider.as_deref() {
        Some("external_idp") => EXTERNAL_IDP_AUTH_KEY,
        Some("enterprise") | Some("builderid") | Some("internal") | Some("awsidc") => {
            OIDC_AUTH_KEY
        }
        _ => SOCIAL_AUTH_KEY,
    }
}

fn provider_value_for_record(account: &KiroAccount, auth_key: &str) -> String {
    if let Some(provider) = normalize_ascii_lower(
        pick_string(
            account.kiro_auth_token_raw.as_ref(),
            &[&["provider"], &["loginProvider"], &["login_option"]],
        )
        .as_deref()
        .or(account.login_provider.as_deref()),
    ) {
        return provider;
    }

    match auth_key {
        EXTERNAL_IDP_AUTH_KEY => "external_idp".to_string(),
        OIDC_AUTH_KEY => "builderid".to_string(),
        _ => "social".to_string(),
    }
}

fn default_profile_name(auth_key: &str) -> &'static str {
    match auth_key {
        EXTERNAL_IDP_AUTH_KEY => "ExternalIdP_Default_Profile",
        OIDC_AUTH_KEY => "BuilderId_Default_Profile",
        _ => "Social_Default_Profile",
    }
}

fn build_auth_record_value(account: &KiroAccount, auth_key: &str) -> Result<String, String> {
    let access_token = account.access_token.trim();
    if access_token.is_empty() {
        return Err(format!("账号 {} 缺少 access_token", account.email));
    }

    let mut obj = account
        .kiro_auth_token_raw
        .as_ref()
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_else(Map::new);

    obj.insert(
        "access_token".to_string(),
        Value::String(access_token.to_string()),
    );
    obj.insert(
        "accessToken".to_string(),
        Value::String(access_token.to_string()),
    );

    if let Some(refresh_token) = account
        .refresh_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        obj.insert(
            "refresh_token".to_string(),
            Value::String(refresh_token.to_string()),
        );
        obj.insert(
            "refreshToken".to_string(),
            Value::String(refresh_token.to_string()),
        );
    }

    let provider = provider_value_for_record(account, auth_key);
    obj.insert("provider".to_string(), Value::String(provider.clone()));
    obj.entry("loginProvider".to_string())
        .or_insert_with(|| Value::String(provider));

    if let Some(profile_arn) = extract_profile_arn(account) {
        obj.insert(
            "profile_arn".to_string(),
            Value::String(profile_arn.clone()),
        );
        obj.insert("profileArn".to_string(), Value::String(profile_arn));
    }

    if let Some(expires_at) = account.expires_at {
        if let Some(date_time) = chrono::Utc.timestamp_opt(expires_at, 0).single() {
            obj.insert(
                "expires_at".to_string(),
                Value::String(date_time.to_rfc3339_opts(SecondsFormat::Millis, true)),
            );
        }
    }

    if let Some(value) = account.idc_region.as_ref().filter(|value| !value.trim().is_empty()) {
        obj.insert("idc_region".to_string(), Value::String(value.clone()));
        obj.insert("region".to_string(), Value::String(value.clone()));
    }
    if let Some(value) = account
        .issuer_url
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        obj.insert("issuer_url".to_string(), Value::String(value.clone()));
    }
    if let Some(value) = account.client_id.as_ref().filter(|value| !value.trim().is_empty()) {
        obj.insert("client_id".to_string(), Value::String(value.clone()));
    }
    if let Some(value) = account.scopes.as_ref().filter(|value| !value.trim().is_empty()) {
        obj.insert("scope".to_string(), Value::String(value.clone()));
        obj.insert("scopes".to_string(), Value::String(value.clone()));
    }
    if let Some(value) = account
        .login_provider
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        obj.entry("loginProvider".to_string())
            .or_insert_with(|| Value::String(value.clone()));
    }

    serde_json::to_string(&Value::Object(obj)).map_err(|e| format!("序列化 Kiro token 失败: {}", e))
}

fn build_profile_record_value(account: &KiroAccount, auth_key: &str) -> Result<String, String> {
    let mut obj = account
        .kiro_profile_raw
        .as_ref()
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_else(Map::new);

    let profile_arn =
        extract_profile_arn(account).ok_or_else(|| format!("账号 {} 缺少 profileArn", account.email))?;
    obj.insert("arn".to_string(), Value::String(profile_arn));

    let profile_name = obj
        .get("profile_name")
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            obj.get("profileName")
                .and_then(|value| value.as_str())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| default_profile_name(auth_key).to_string());

    obj.insert(
        "profile_name".to_string(),
        Value::String(profile_name.clone()),
    );
    obj.entry("profileName".to_string())
        .or_insert_with(|| Value::String(profile_name));

    serde_json::to_string(&Value::Object(obj))
        .map_err(|e| format!("序列化 Kiro profile 失败: {}", e))
}

fn kiro_cli_db_path() -> Result<PathBuf, String> {
    let base_dir = dirs::data_dir().ok_or_else(|| "无法定位 Kiro CLI 数据目录".to_string())?;
    Ok(base_dir.join("kiro-cli").join("data.sqlite3"))
}

fn load_optional_text(
    conn: &Connection,
    sql: &str,
    key: &str,
) -> Result<Option<String>, String> {
    conn.query_row(sql, params![key], |row| row.get::<_, String>(0))
        .optional()
        .map_err(|e| format!("读取 Kiro CLI 数据库失败: {}", e))
}

fn prepare_kiro_cli_auth(account: &KiroAccount) -> Result<KiroCliDbSnapshot, String> {
    let db_path = kiro_cli_db_path()?;
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("创建 Kiro CLI 数据目录失败: {}", e))?;
    }

    let mut conn = Connection::open(&db_path)
        .map_err(|e| format!("打开 Kiro CLI 数据库失败: {}", e))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("开启 Kiro CLI 事务失败: {}", e))?;

    tx.execute(
        "CREATE TABLE IF NOT EXISTS auth_kv (key TEXT PRIMARY KEY, value TEXT)",
        [],
    )
    .map_err(|e| format!("初始化 auth_kv 表失败: {}", e))?;
    tx.execute(
        "CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value BLOB)",
        [],
    )
    .map_err(|e| format!("初始化 state 表失败: {}", e))?;

    let mut snapshot = KiroCliDbSnapshot::default();
    for key in KNOWN_AUTH_KEYS {
        let value = load_optional_text(
            &tx,
            "SELECT value FROM auth_kv WHERE key = ?1",
            key,
        )?;
        snapshot.auth_values.insert(key.to_string(), value);
    }
    snapshot.profile_value = load_optional_text(
        &tx,
        "SELECT CAST(value AS TEXT) FROM state WHERE key = ?1",
        PROFILE_STATE_KEY,
    )?;

    let auth_key = resolve_auth_storage_key(account);
    let auth_value = build_auth_record_value(account, auth_key)?;
    let profile_value = build_profile_record_value(account, auth_key)?;

    for key in KNOWN_AUTH_KEYS {
        tx.execute("DELETE FROM auth_kv WHERE key = ?1", params![key])
            .map_err(|e| format!("清理旧 Kiro token 失败: {}", e))?;
    }

    tx.execute(
        "INSERT OR REPLACE INTO auth_kv (key, value) VALUES (?1, ?2)",
        params![auth_key, auth_value],
    )
    .map_err(|e| format!("写入 Kiro token 失败: {}", e))?;
    tx.execute(
        "INSERT OR REPLACE INTO state (key, value) VALUES (?1, ?2)",
        params![PROFILE_STATE_KEY, profile_value.into_bytes()],
    )
    .map_err(|e| format!("写入 Kiro profile 失败: {}", e))?;

    tx.commit()
        .map_err(|e| format!("提交 Kiro CLI 事务失败: {}", e))?;
    Ok(snapshot)
}

fn restore_kiro_cli_auth(snapshot: KiroCliDbSnapshot) -> Result<(), String> {
    let db_path = kiro_cli_db_path()?;
    let mut conn = Connection::open(&db_path)
        .map_err(|e| format!("重新打开 Kiro CLI 数据库失败: {}", e))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("开启恢复事务失败: {}", e))?;

    tx.execute(
        "CREATE TABLE IF NOT EXISTS auth_kv (key TEXT PRIMARY KEY, value TEXT)",
        [],
    )
    .map_err(|e| format!("初始化 auth_kv 表失败: {}", e))?;
    tx.execute(
        "CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value BLOB)",
        [],
    )
    .map_err(|e| format!("初始化 state 表失败: {}", e))?;

    for key in KNOWN_AUTH_KEYS {
        match snapshot.auth_values.get(key).cloned().flatten() {
            Some(value) => {
                tx.execute(
                    "INSERT OR REPLACE INTO auth_kv (key, value) VALUES (?1, ?2)",
                    params![key, value],
                )
                .map_err(|e| format!("恢复 auth_kv 失败: {}", e))?;
            }
            None => {
                tx.execute("DELETE FROM auth_kv WHERE key = ?1", params![key])
                    .map_err(|e| format!("清理注入 auth_kv 失败: {}", e))?;
            }
        }
    }

    match snapshot.profile_value {
        Some(value) => {
            tx.execute(
                "INSERT OR REPLACE INTO state (key, value) VALUES (?1, ?2)",
                params![PROFILE_STATE_KEY, value.into_bytes()],
            )
            .map_err(|e| format!("恢复 profile 失败: {}", e))?;
        }
        None => {
            tx.execute("DELETE FROM state WHERE key = ?1", params![PROFILE_STATE_KEY])
                .map_err(|e| format!("清理注入 profile 失败: {}", e))?;
        }
    }

    tx.commit()
        .map_err(|e| format!("提交恢复事务失败: {}", e))
}

fn resolve_kiro_cli_path() -> String {
    if let Ok(path) = std::env::var("KIRO_CLI_PATH") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Some(home_dir) = dirs::home_dir() {
        let preferred = home_dir.join(".local/bin/kiro-cli");
        if preferred.is_file() {
            return preferred.display().to_string();
        }
    }

    "kiro-cli".to_string()
}

fn strip_terminal_artifacts(text: &str) -> String {
    let without_osc = osc_escape_regex().replace_all(text, "");
    let without_ansi = ansi_escape_regex().replace_all(&without_osc, "");
    without_ansi
        .chars()
        .filter(|ch| !ch.is_control() || matches!(*ch, '\n' | '\r' | '\t'))
        .collect::<String>()
        .lines()
        .map(str::trim_end)
        .collect::<Vec<&str>>()
        .join("\n")
        .trim()
        .to_string()
}

async fn invoke_kiro_cli(prompt: String, model: String) -> Result<String, String> {
    tokio::task::spawn_blocking(move || {
        let mut command = Command::new(resolve_kiro_cli_path());
        command
            .arg("chat")
            .arg("--no-interactive")
            .arg("--wrap")
            .arg("never")
            .arg("--trust-tools=")
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .env("CI", "1")
            .env_remove("CLICOLOR")
            .env_remove("CLICOLOR_FORCE");

        if let Some(home_dir) = dirs::home_dir() {
            command.current_dir(home_dir);
        }
        if !model.trim().is_empty() && model != "auto" {
            command.arg("--model").arg(model.trim());
        }

        let output = command
            .arg(prompt)
            .output()
            .map_err(|e| format!("启动 kiro-cli 失败: {}", e))?;

        let stdout = strip_terminal_artifacts(&String::from_utf8_lossy(&output.stdout));
        let stderr = strip_terminal_artifacts(&String::from_utf8_lossy(&output.stderr));

        if output.status.success() {
            if !stdout.is_empty() {
                Ok(stdout)
            } else if !stderr.is_empty() {
                Ok(stderr)
            } else {
                Err("kiro-cli 返回空响应".to_string())
            }
        } else {
            let mut parts = Vec::new();
            if !stdout.is_empty() {
                parts.push(format!("stdout: {}", stdout));
            }
            if !stderr.is_empty() {
                parts.push(format!("stderr: {}", stderr));
            }
            let detail = if parts.is_empty() {
                "无输出".to_string()
            } else {
                parts.join(" | ")
            };
            Err(format!(
                "kiro-cli 调用失败(code={}): {}",
                output.status.code().unwrap_or(-1),
                detail
            ))
        }
    })
    .await
    .map_err(|e| format!("等待 kiro-cli 结束失败: {}", e))?
}

async fn proxy_via_kiro_cli(
    request: &CompletionRequest,
    account: &KiroAccount,
) -> Result<ProxyResult, String> {
    let _guard = request_lock().lock().await;
    let account_clone = account.clone();
    let snapshot = tokio::task::spawn_blocking(move || prepare_kiro_cli_auth(&account_clone))
        .await
        .map_err(|e| format!("准备 Kiro CLI 凭据任务失败: {}", e))??;

    let result = invoke_kiro_cli(request.prompt.clone(), request.model.clone()).await;

    let restore_result = tokio::task::spawn_blocking(move || restore_kiro_cli_auth(snapshot))
        .await
        .map_err(|e| format!("恢复 Kiro CLI 凭据任务失败: {}", e))?;

    if let Err(restore_err) = restore_result {
        logger::log_error(&format!(
            "[KiroLocalAccess] 恢复 kiro-cli 登录态失败: {}",
            restore_err
        ));
        if result.is_ok() {
            return Err(format!("恢复 kiro-cli 登录态失败: {}", restore_err));
        }
    }

    let content = result?;
    Ok(build_completion_response(
        request.model.as_str(),
        content.as_str(),
        request.stream,
    ))
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
    if let Err(err) = save_stats_to_disk(&rt.stats) {
        logger::log_warn(&format!("[KiroLocalAccess] 保存统计失败: {}", err));
    }
}

async fn set_last_error(message: Option<String>) {
    let mut rt = gateway_runtime().lock().await;
    rt.last_error = message;
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    let raw = read_http_request(&mut stream).await?;
    let parsed = parse_http_request(&raw)?;

    if parsed.method.eq_ignore_ascii_case("OPTIONS") {
        let response = b"HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Authorization, Content-Type, X-API-Key\r\nAccess-Control-Max-Age: 86400\r\n\r\n";
        let _ = stream.write_all(response).await;
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

    let Some(api_key) = extract_api_key(&parsed.headers) else {
        write_error(&mut stream, 401, "缺少 Authorization Bearer 或 X-API-Key").await;
        return Ok(());
    };

    let collection = {
        let rt = gateway_runtime().lock().await;
        rt.collection.clone()
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

    if parsed.target == "/v1/models" || parsed.target.starts_with("/v1/models?") {
        let response = build_models_response();
        write_json_response(&mut stream, 200, &response).await;
        return Ok(());
    }

    if !parsed.target.starts_with("/v1/chat/completions") {
        write_error(&mut stream, 404, "仅支持 /v1/models 和 /v1/chat/completions").await;
        return Ok(());
    }

    let completion_request = match parse_completion_request(&parsed.body) {
        Ok(request) => request,
        Err(err) => {
            write_error(&mut stream, 400, err.as_str()).await;
            return Ok(());
        }
    };

    if collection.account_ids.is_empty() {
        write_error(&mut stream, 503, "账号池为空，请先同步 Kiro 账号").await;
        return Ok(());
    }

    let start_time = Instant::now();
    let max_tries = collection.account_ids.len();
    let mut tried = Vec::new();
    let mut failure_messages = Vec::new();

    for _ in 0..max_tries {
        let candidates = select_account_ids(&collection, &tried);
        let Some(account_id) = candidates.first() else {
            break;
        };
        tried.push(account_id.clone());

        let Some(account) = kiro_account::load_account(account_id) else {
            failure_messages.push(format!("{}: 账号不存在", account_id));
            continue;
        };
        if account.access_token.trim().is_empty() {
            failure_messages.push(format!("{}: 缺少 access_token", account.email));
            continue;
        }

        match proxy_via_kiro_cli(&completion_request, &account).await {
            Ok(result) => {
                write_upstream_response(&mut stream, &result).await;
                record_usage(true, start_time.elapsed().as_millis() as u64).await;
                set_last_error(None).await;
                logger::log_info(&format!(
                    "[KiroLocalAccess] 请求成功: account_id={}, email={}, elapsed={}ms",
                    account.id,
                    account.email,
                    start_time.elapsed().as_millis()
                ));
                return Ok(());
            }
            Err(err) => {
                logger::log_warn(&format!(
                    "[KiroLocalAccess] 账号请求失败: account_id={}, email={}, error={}",
                    account.id, account.email, err
                ));
                failure_messages.push(format!("{}: {}", account.email, err));
            }
        }
    }

    record_usage(false, start_time.elapsed().as_millis() as u64).await;
    let summary = if failure_messages.is_empty() {
        "所有账号均不可用".to_string()
    } else {
        format!("所有账号均不可用: {}", failure_messages.join(" | "))
    };
    set_last_error(Some(summary.clone())).await;
    write_error(&mut stream, 502, summary.as_str()).await;
    Ok(())
}

async fn start_gateway(port: u16) -> Result<(), String> {
    let listener = TcpListener::bind((LOCALHOST_BIND, port))
        .await
        .map_err(|e| format!("绑定 {}:{} 失败: {}", LOCALHOST_BIND, port, e))?;
    let actual_port = listener
        .local_addr()
        .map_err(|e| format!("读取监听端口失败: {}", e))?
        .port();

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    {
        let mut rt = gateway_runtime().lock().await;
        rt.running = true;
        rt.actual_port = Some(actual_port);
        rt.shutdown_sender = Some(shutdown_tx);
        rt.last_error = None;
    }

    logger::log_info(&format!(
        "[KiroLocalAccess] 本地接入服务已启动: bind={}:{} base=http://{}:{}/v1",
        LOCALHOST_BIND, actual_port, LOCALHOST_BIND, actual_port
    ));

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            tokio::spawn(async move {
                                if let Err(err) = handle_connection(stream).await {
                                    logger::log_warn(&format!("[KiroLocalAccess] 连接处理失败: {}", err));
                                }
                            });
                        }
                        Err(err) => {
                            logger::log_warn(&format!("[KiroLocalAccess] accept 失败: {}", err));
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
        logger::log_info("[KiroLocalAccess] 网关已停止");
    });

    let mut rt = gateway_runtime().lock().await;
    rt.task = Some(task);
    Ok(())
}

async fn stop_gateway() {
    let (sender, task) = {
        let mut rt = gateway_runtime().lock().await;
        rt.running = false;
        rt.actual_port = None;
        (rt.shutdown_sender.take(), rt.task.take())
    };

    if let Some(tx) = sender {
        let _ = tx.send(true);
    }
    if let Some(task) = task {
        let _ = timeout(GATEWAY_SHUTDOWN_TIMEOUT, task).await;
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
    let desired_port = collection.port;
    let already_running = rt.running;
    let current_port = rt.actual_port;
    drop(rt);

    if already_running && current_port == Some(desired_port) {
        return Ok(());
    }

    if already_running {
        stop_gateway().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    start_gateway(desired_port).await
}

pub async fn get_local_access_state() -> Result<KiroLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let rt = gateway_runtime().lock().await;
    Ok(build_state_snapshot(&rt))
}

pub async fn set_local_access_enabled(enabled: bool) -> Result<KiroLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;

    if let Some(collection) = rt.collection.as_mut() {
        collection.enabled = enabled;
        collection.updated_at = now_ms();
        save_collection_to_disk(collection)?;
    } else if enabled {
        let now = now_ms();
        let collection = KiroLocalAccessCollection {
            enabled: true,
            port: allocate_random_port()?,
            api_key: generate_api_key(),
            account_ids: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        save_collection_to_disk(&collection)?;
        rt.collection = Some(collection);
    }
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
) -> Result<KiroLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(collection) = rt.collection.as_mut() else {
        return Err("服务未配置".to_string());
    };
    collection.account_ids = dedupe_account_ids(account_ids);
    collection.updated_at = now_ms();
    save_collection_to_disk(collection)?;
    drop(rt);

    ensure_gateway_running().await?;
    get_local_access_state().await
}

pub async fn remove_local_access_account(account_id: &str) -> Result<KiroLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(collection) = rt.collection.as_mut() else {
        return Err("服务未配置".to_string());
    };
    collection.account_ids.retain(|item| item != account_id);
    collection.updated_at = now_ms();
    save_collection_to_disk(collection)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn rotate_local_access_api_key() -> Result<KiroLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    let Some(collection) = rt.collection.as_mut() else {
        return Err("服务未配置".to_string());
    };
    collection.api_key = generate_api_key();
    collection.updated_at = now_ms();
    save_collection_to_disk(collection)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn update_local_access_port(port: u16) -> Result<KiroLocalAccessState, String> {
    if port == 0 {
        return Err("端口必须大于 0".to_string());
    }

    ensure_runtime_loaded().await;
    {
        let mut rt = gateway_runtime().lock().await;
        let Some(collection) = rt.collection.as_mut() else {
            return Err("服务未配置".to_string());
        };
        collection.port = port;
        collection.updated_at = now_ms();
        save_collection_to_disk(collection)?;
    }

    ensure_gateway_running().await?;
    get_local_access_state().await
}

pub async fn clear_local_access_stats() -> Result<KiroLocalAccessState, String> {
    ensure_runtime_loaded().await;
    let mut rt = gateway_runtime().lock().await;
    rt.stats = KiroLocalAccessStats {
        since: now_ms(),
        ..Default::default()
    };
    save_stats_to_disk(&rt.stats)?;
    Ok(build_state_snapshot(&rt))
}

pub async fn test_local_access() -> Result<KiroLocalAccessTestResult, String> {
    ensure_runtime_loaded().await;
    let mut state = get_local_access_state().await?;

    let Some(collection) = state.collection.clone() else {
        return Ok(build_failure_result(build_test_failure(
            "服务尚未配置",
            "本地服务配置",
            "当前还没有初始化 Kiro API 服务。",
            "先点击“启用”，再同步账号池。",
            None,
            None,
            None,
        )));
    };

    if !collection.enabled {
        return Ok(build_failure_result(build_test_failure(
            "服务未启用",
            "本地服务配置",
            "Kiro API 服务当前处于停用状态。",
            "先启用服务，再执行测试。",
            None,
            None,
            None,
        )));
    }

    if collection.account_ids.is_empty() {
        return Ok(build_failure_result(build_test_failure(
            "账号池为空",
            "账号池检查",
            "当前 API 服务没有可用账号。",
            "点击“同步账号池”，把当前 Kiro 账号加入服务后再测试。",
            None,
            None,
            None,
        )));
    }

    if !state.running {
        if let Err(err) = ensure_gateway_running().await {
            return Ok(build_failure_result(build_test_failure(
                "服务启动失败",
                "本地网关启动",
                "Kiro API 服务未能启动监听端口。",
                "检查端口是否被占用，或修改端口后重试。",
                None,
                None,
                Some(err),
            )));
        }
        state = get_local_access_state().await?;
    }

    let Some(base_url) = state.base_url.clone() else {
        return Ok(build_failure_result(build_test_failure(
            "缺少服务地址",
            "本地网关启动",
            "服务已启用，但当前没有可用的 Base URL。",
            "停用后重新启用服务，再重试。",
            None,
            None,
            None,
        )));
    };

    let model_id = state
        .model_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "auto".to_string());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .map_err(|e| format!("创建测试客户端失败: {}", e))?;
    let request_body = json!({
        "model": model_id.clone(),
        "stream": false,
        "messages": [
            {
                "role": "user",
                "content": "Reply with exactly KIRO_LOCAL_ACCESS_OK and nothing else."
            }
        ]
    });

    let started_at = Instant::now();
    let response = match client
        .post(format!("{}/chat/completions", base_url))
        .bearer_auth(collection.api_key.clone())
        .json(&request_body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            return Ok(build_failure_result(build_test_failure(
                "请求本地服务失败",
                "本地网关请求",
                "无法向 Kiro API 服务发起测试请求。",
                "检查服务是否已启动、端口是否正确，以及本机防火墙是否拦截。",
                None,
                Some(model_id.clone()),
                Some(err.to_string()),
            )));
        }
    };
    let latency_ms = started_at.elapsed().as_millis() as u64;
    let status = response.status().as_u16();
    let response_text = response
        .text()
        .await
        .unwrap_or_else(|_| String::new());

    if status != 200 {
        return Ok(build_failure_result(build_test_failure(
            "服务返回异常状态",
            "本地网关响应",
            "本地 API 服务已收到请求，但返回了非 200 状态。",
            "优先查看最近错误；如果提示账号不可用，请同步账号池或检查 Kiro 登录态。",
            Some(status),
            Some(model_id.clone()),
            truncate_detail(&response_text, 2400),
        )));
    }

    let response_json: Value = match serde_json::from_str(&response_text) {
        Ok(value) => value,
        Err(err) => {
            return Ok(build_failure_result(build_test_failure(
                "响应格式无效",
                "本地网关响应",
                "本地服务返回了无法解析的响应格式。",
                "这通常表示上游输出异常，请查看响应详情并检查最近错误。",
                Some(status),
                Some(model_id.clone()),
                Some(format!(
                    "JSON 解析失败: {}\n\n响应正文:\n{}",
                    err,
                    truncate_detail(&response_text, 2000).unwrap_or_else(|| "<empty>".to_string())
                )),
            )));
        }
    };

    let Some(output) = extract_completion_text(&response_json) else {
        return Ok(build_failure_result(build_test_failure(
            "未提取到回复内容",
            "上游响应解析",
            "服务响应成功，但没有拿到 assistant 文本内容。",
            "说明本地网关已通，但 Kiro CLI 返回格式不符合预期，请查看响应详情。",
            Some(status),
            Some(model_id.clone()),
            truncate_detail(&response_text, 2400),
        )));
    };

    if output.trim() != "KIRO_LOCAL_ACCESS_OK" {
        return Ok(build_failure_result(build_test_failure(
            "返回内容不符合预期",
            "上游响应校验",
            "本地服务和上游链路已打通，但返回内容不是预期的固定结果。",
            "优先检查当前账号登录态是否稳定；必要时轮换账号后重新测试。",
            Some(status),
            Some(model_id.clone()),
            truncate_detail(&output, 1200),
        )));
    }

    Ok(KiroLocalAccessTestResult {
        model_id: Some(model_id),
        latency_ms: Some(latency_ms),
        output: Some(output),
        failure: None,
    })
}

pub async fn restore_local_access_gateway() {
    ensure_runtime_loaded().await;
    let enabled = {
        let rt = gateway_runtime().lock().await;
        rt.collection.as_ref().map(|item| item.enabled).unwrap_or(false)
    };
    if enabled {
        if let Err(err) = ensure_gateway_running().await {
            logger::log_warn(&format!("[KiroLocalAccess] 恢复网关失败: {}", err));
            set_last_error(Some(err)).await;
        }
    }
}
