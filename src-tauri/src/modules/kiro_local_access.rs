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
use std::collections::{HashMap, HashSet};
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
const MAX_KIRO_PAYLOAD_SIZE: usize = 615 * 1024;
const DEFAULT_KIRO_REGION: &str = "us-east-1";
const DEFAULT_AGENT_MODE: &str = "q-developer-converse";
const KNOWN_AUTH_KEYS: [&str; 3] = [
    "kirocli:social:token",
    "kirocli:external-idp:token",
    "kirocli:odic:token",
];
const PROFILE_STATE_KEY: &str = "api.codewhisperer.profile";
const SOCIAL_AUTH_KEY: &str = "kirocli:social:token";
const EXTERNAL_IDP_AUTH_KEY: &str = "kirocli:external-idp:token";
const OIDC_AUTH_KEY: &str = "kirocli:odic:token";
const DEFAULT_KIRO_MODELS: &[&str] = &[
    "claude-opus-4-7",
    "claude-opus-4-7-thinking",
    "claude-sonnet-4-7",
    "claude-sonnet-4-7-thinking",
    "claude-haiku-4-7",
    "claude-haiku-4-7-thinking",
    "claude-opus-4-6",
    "claude-opus-4-6-thinking",
    "claude-sonnet-4-6",
    "claude-sonnet-4-6-thinking",
    "claude-haiku-4-6",
    "claude-haiku-4-6-thinking",
    "claude-opus-4-5-20251101",
    "claude-opus-4-5-20251101-thinking",
    "claude-sonnet-4-5-20250929",
    "claude-sonnet-4-5-20250929-thinking",
    "claude-haiku-4-5-20251001",
    "claude-haiku-4-5-20251001-thinking",
    "claude-sonnet-4",
    "claude-sonnet-4-20250514",
    "claude-3-7-sonnet-20250219",
];
const SUPPORTED_KIRO_REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-north-1",
    "eu-south-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-south-1",
    "ap-east-1",
    "ca-central-1",
    "sa-east-1",
    "me-south-1",
    "af-south-1",
    "us-gov-west-1",
];

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

#[derive(Debug, Clone)]
struct GatewayProxyRequest {
    model: String,
    stream: bool,
    messages: Vec<GatewayMessage>,
    tools: Vec<GatewayTool>,
    previous_response_id: Option<String>,
}

#[derive(Debug, Clone)]
struct GatewayMessage {
    role: String,
    content: Value,
    tool_calls: Vec<GatewayToolCall>,
    tool_call_id: Option<String>,
}

#[derive(Debug, Clone)]
struct GatewayTool {
    name: String,
    description: String,
    input_schema: Value,
}

#[derive(Debug, Clone)]
struct GatewayToolCall {
    id: String,
    name: String,
    arguments: String,
}

#[derive(Debug, Clone, Default)]
struct AggregatedKiroResponse {
    text: String,
    thinking: String,
    tool_calls: Vec<GatewayToolCall>,
    input_tokens: i32,
    output_tokens: i32,
    cache_read_input_tokens: Option<i32>,
    cache_creation_input_tokens: Option<i32>,
}

#[derive(Debug)]
enum KiroEvent {
    Text(String),
    Thinking(String),
    ToolUseStart {
        id: String,
        name: String,
    },
    ToolUseInputDelta {
        id: String,
        input_delta: String,
    },
    ToolUseStop {
        id: String,
    },
    Usage {
        input_tokens: i32,
        output_tokens: i32,
        cache_read_input_tokens: Option<i32>,
        cache_creation_input_tokens: Option<i32>,
    },
}

#[derive(Debug, Clone)]
struct KiroUpstreamCredentials {
    access_token: String,
    profile_arn: Option<String>,
    region: String,
    user_agent: String,
    provider: Option<String>,
    account_email: String,
}

#[derive(Debug)]
struct DirectProxyError {
    status: u16,
    message: String,
}

#[derive(Debug, Copy, Clone)]
enum ApiProtocol {
    OpenAi,
    Anthropic,
    Responses,
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

enum KiroCliAuthMode {
    ReuseCurrent,
    Injected(KiroCliDbSnapshot),
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
        Regex::new(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)").expect("osc escape regex should compile")
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
    let json =
        serde_json::to_string_pretty(collection).map_err(|e| format!("序列化配置失败: {}", e))?;
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
    let json = serde_json::to_string_pretty(stats).map_err(|e| format!("序列化统计失败: {}", e))?;
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
        model_ids: gateway_model_ids(),
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

fn gateway_model_ids() -> Vec<String> {
    DEFAULT_KIRO_MODELS
        .iter()
        .map(|item| item.to_string())
        .collect()
}

fn dedupe_model_ids(model_ids: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    for model_id in model_ids {
        if seen.insert(model_id.clone()) {
            ordered.push(model_id);
        }
    }
    if seen.insert("auto".to_string()) {
        ordered.insert(0, "auto".to_string());
    }
    ordered
}

fn parse_kiro_cli_model_ids(output: &[u8]) -> Vec<String> {
    let mut model_ids = Vec::new();
    if let Ok(value) = serde_json::from_slice::<Value>(output) {
        if let Some(items) = value.get("models").and_then(|value| value.as_array()) {
            for item in items {
                let model_id = item
                    .get("model_id")
                    .or_else(|| item.get("id"))
                    .and_then(|value| value.as_str())
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                if let Some(model_id) = model_id {
                    model_ids.push(model_id.to_string());
                }
            }
        }
    }
    dedupe_model_ids(model_ids)
}

async fn invoke_kiro_cli_list_models() -> Result<Vec<String>, String> {
    tokio::task::spawn_blocking(move || {
        let output = Command::new(resolve_kiro_cli_path())
            .arg("chat")
            .arg("--list-models")
            .arg("--format")
            .arg("json")
            .output()
            .map_err(|e| format!("启动 kiro-cli 失败: {}", e))?;
        if !output.status.success() {
            let stderr = strip_terminal_artifacts(&String::from_utf8_lossy(&output.stderr));
            return Err(format!(
                "kiro-cli 列出模型失败(code={}): {}",
                output.status.code().unwrap_or(-1),
                if stderr.is_empty() {
                    "无输出".to_string()
                } else {
                    stderr
                }
            ));
        }
        Ok(parse_kiro_cli_model_ids(&output.stdout))
    })
    .await
    .map_err(|e| format!("等待 kiro-cli 结束失败: {}", e))?
}

async fn load_model_ids_for_account(account: &KiroAccount) -> Result<Vec<String>, String> {
    let _guard = request_lock().lock().await;
    let account_clone = account.clone();
    let auth_mode =
        tokio::task::spawn_blocking(move || prepare_kiro_cli_auth_for_request(&account_clone))
            .await
            .map_err(|e| format!("准备 Kiro CLI 凭据任务失败: {}", e))??;

    let result = invoke_kiro_cli_list_models().await;

    if let KiroCliAuthMode::Injected(snapshot) = auth_mode {
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
    }

    result
}

async fn resolve_gateway_model_ids(collection: &KiroLocalAccessCollection) -> Vec<String> {
    let _ = collection;
    gateway_model_ids()
}

async fn build_models_response(collection: &KiroLocalAccessCollection) -> Value {
    let model_ids = resolve_gateway_model_ids(collection).await;
    json!({
        "object": "list",
        "data": model_ids.iter().map(|id| json!({
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
    let mut response = format!(
        "HTTP/1.1 {} {}\r\n",
        result.status,
        status_text(result.status)
    );
    response.push_str("Access-Control-Allow-Origin: *\r\n");
    response.push_str("Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n");
    response.push_str("Access-Control-Allow-Headers: Authorization, Content-Type, X-API-Key\r\n");

    if result.is_stream {
        response.push_str("Content-Type: text/event-stream\r\n");
        response.push_str("Cache-Control: no-cache\r\n");
        response.push_str(&format!("Content-Length: {}\r\n", result.body.len()));
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
    if result.is_stream {
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    }
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

fn parse_anthropic_messages_to_text(messages: &Value) -> Result<String, String> {
    let items = messages
        .as_array()
        .ok_or_else(|| "messages 必须是数组".to_string())?;

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
        transcript.push(format!("{}:\n{}", role_label(role), content));
    }

    if transcript.is_empty() {
        return Err("messages 中没有可用的文本内容".to_string());
    }

    Ok(transcript.join("\n\n"))
}

fn parse_anthropic_system_to_text(system: Option<&Value>) -> String {
    match system {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(extract_text_part)
            .collect::<Vec<String>>()
            .join("\n"),
        Some(Value::Object(obj)) => obj
            .get("text")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .trim()
            .to_string(),
        _ => String::new(),
    }
}

fn parse_anthropic_request(body: &[u8]) -> Result<CompletionRequest, String> {
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
    let transcript = parse_anthropic_messages_to_text(
        value
            .get("messages")
            .ok_or_else(|| "缺少 messages 字段".to_string())?,
    )?;
    let system_text = parse_anthropic_system_to_text(value.get("system"));

    let mut prompt = String::new();
    if !system_text.is_empty() {
        prompt.push_str("System instructions:\n");
        prompt.push_str(system_text.as_str());
        prompt.push_str("\n\n");
    }
    prompt.push_str("Conversation:\n");
    prompt.push_str(transcript.as_str());
    prompt.push_str("\n\nRespond as the assistant to the final user message. Keep prior context when it matters.");

    Ok(CompletionRequest {
        model,
        stream,
        prompt,
    })
}

fn build_anthropic_count_tokens_response(request: &CompletionRequest) -> Value {
    let token_count = request.prompt.chars().count().div_ceil(4).max(1) as u64;
    json!({
        "input_tokens": token_count
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

fn build_anthropic_response(model: &str, content: &str, stream: bool) -> ProxyResult {
    let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());

    if stream {
        let mut body = String::new();
        let message_start = json!({
            "type":"message_start",
            "message":{
                "id": message_id,
                "type":"message",
                "role":"assistant",
                "model": model,
                "content": [],
                "stop_reason": Value::Null,
                "stop_sequence": Value::Null,
                "usage":{"input_tokens":0,"output_tokens":0}
            }
        });
        body.push_str("event: message_start\n");
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&message_start).unwrap_or_default());
        body.push_str("\n\n");

        let block_start = json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        });
        body.push_str("event: content_block_start\n");
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&block_start).unwrap_or_default());
        body.push_str("\n\n");

        for chunk in chunk_text_for_stream(content, 160) {
            let block_delta = json!({
                "type":"content_block_delta",
                "index":0,
                "delta":{"type":"text_delta","text":chunk}
            });
            body.push_str("event: content_block_delta\n");
            body.push_str("data: ");
            body.push_str(&serde_json::to_string(&block_delta).unwrap_or_default());
            body.push_str("\n\n");
        }

        let block_stop = json!({"type":"content_block_stop","index":0});
        body.push_str("event: content_block_stop\n");
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&block_stop).unwrap_or_default());
        body.push_str("\n\n");

        let message_delta = json!({
            "type":"message_delta",
            "delta":{"stop_reason":"end_turn","stop_sequence": Value::Null},
            "usage":{"output_tokens":0}
        });
        body.push_str("event: message_delta\n");
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&message_delta).unwrap_or_default());
        body.push_str("\n\n");

        let message_stop = json!({"type":"message_stop"});
        body.push_str("event: message_stop\n");
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&message_stop).unwrap_or_default());
        body.push_str("\n\n");

        ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "text/event-stream".to_string())],
            body: body.into_bytes(),
            is_stream: true,
        }
    } else {
        let body = json!({
            "id": message_id,
            "type":"message",
            "role":"assistant",
            "model": model,
            "content":[{"type":"text","text":content}],
            "stop_reason":"end_turn",
            "stop_sequence": Value::Null,
            "usage":{"input_tokens":0,"output_tokens":0}
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

fn normalize_kiro_region(region: Option<&str>) -> Option<String> {
    let region = region?.trim();
    if region.is_empty() || !SUPPORTED_KIRO_REGIONS.contains(&region) {
        return None;
    }
    Some(region.to_string())
}

fn parse_region_from_profile_arn(profile_arn: Option<&str>) -> Option<String> {
    let profile_arn = profile_arn?.trim();
    if profile_arn.is_empty() {
        return None;
    }

    let mut segments = profile_arn.split(':');
    let arn = segments.next()?;
    let partition = segments.next()?;
    let service = segments.next()?;
    let region = segments.next()?;
    if arn != "arn" || partition.is_empty() || service != "codewhisperer" {
        return None;
    }
    normalize_kiro_region(Some(region))
}

fn resolve_kiro_upstream_region(account: &KiroAccount, profile_arn: Option<&str>) -> String {
    parse_region_from_profile_arn(profile_arn)
        .or_else(|| normalize_kiro_region(account.idc_region.as_deref()))
        .unwrap_or_else(|| DEFAULT_KIRO_REGION.to_string())
}

fn get_kiro_data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        return dirs::home_dir().map(|home| {
            home.join("Library")
                .join("Application Support")
                .join("Kiro")
        });
    }
    #[cfg(target_os = "windows")]
    {
        return std::env::var("APPDATA")
            .ok()
            .map(|appdata| PathBuf::from(appdata).join("Kiro"));
    }
    #[cfg(target_os = "linux")]
    {
        return dirs::home_dir().map(|home| home.join(".config").join("Kiro"));
    }
    #[allow(unreachable_code)]
    None
}

fn read_kiro_machine_id() -> String {
    if let Some(path) = get_kiro_data_dir().map(|dir| dir.join("machineid")) {
        if let Ok(content) = std::fs::read_to_string(path) {
            let value = content.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    crate::modules::device::get_service_machine_id()
}

fn get_kiro_settings_path() -> Option<PathBuf> {
    get_kiro_data_dir().map(|dir| dir.join("User").join("settings.json"))
}

fn read_kiro_settings_json() -> Option<Value> {
    let path = get_kiro_settings_path()?;
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn get_setting_bool(json: &Value, key: &str) -> Option<bool> {
    if let Some(value) = json.get(key).and_then(Value::as_bool) {
        return Some(value);
    }
    let mut current = json;
    for segment in key.split('.') {
        current = current.get(segment)?;
    }
    current.as_bool()
}

fn get_setting_string(json: &Value, key: &str) -> Option<String> {
    if let Some(value) = json.get(key).and_then(Value::as_str) {
        return Some(value.to_string());
    }
    let mut current = json;
    for segment in key.split('.') {
        current = current.get(segment)?;
    }
    current.as_str().map(str::to_string)
}

fn should_send_codewhisperer_optout() -> bool {
    let Some(json) = read_kiro_settings_json() else {
        return true;
    };
    let content_collection_enabled = get_setting_bool(
        &json,
        "telemetry.dataSharingAndPromptLogging.contentCollectionForServiceImprovement",
    )
    .or_else(|| {
        get_setting_bool(
            &json,
            "telemetry.dataSharing.contentCollectionForServiceImprovement",
        )
    })
    .unwrap_or(false);
    !content_collection_enabled
}

fn get_proxy_from_kiro_settings() -> Option<String> {
    read_kiro_settings_json()
        .and_then(|json| get_setting_string(&json, "http.proxy"))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn get_kiro_app_version() -> String {
    let mut paths = Vec::new();
    #[cfg(target_os = "macos")]
    {
        let root = PathBuf::from("/Applications")
            .join("Kiro.app")
            .join("Contents")
            .join("Resources")
            .join("app");
        paths.push(root.join("product.json"));
        paths.push(root.join("package.json"));
    }
    #[cfg(target_os = "windows")]
    {
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            let root = PathBuf::from(local_app_data)
                .join("Programs")
                .join("Kiro")
                .join("resources")
                .join("app");
            paths.push(root.join("product.json"));
            paths.push(root.join("package.json"));
        }
    }
    #[cfg(target_os = "linux")]
    {
        paths.push(PathBuf::from("/opt/Kiro/resources/app/product.json"));
        paths.push(PathBuf::from("/opt/Kiro/resources/app/package.json"));
    }

    paths
        .into_iter()
        .find_map(|path| {
            let content = std::fs::read_to_string(path).ok()?;
            let json: Value = serde_json::from_str(&content).ok()?;
            json.get("version")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "0.0.0".to_string())
}

fn build_kiro_custom_user_agent(machine_id: &str) -> String {
    format!("KiroIDE {} {}", get_kiro_app_version(), machine_id)
}

fn build_kiro_http_client(streaming: bool) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(20)
        .tcp_keepalive(Duration::from_secs(60))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(20))
        .http2_keep_alive_while_idle(true);

    if !streaming {
        builder = builder.timeout(Duration::from_secs(300));
    }

    if let Some(proxy_url) = get_proxy_from_kiro_settings() {
        if let Ok(proxy) = reqwest::Proxy::all(&proxy_url) {
            builder = builder.proxy(proxy);
        }
    }

    builder
        .build()
        .map_err(|e| format!("创建 Kiro HTTP 客户端失败: {e}"))
}

fn account_token_needs_refresh(account: &KiroAccount) -> bool {
    account
        .expires_at
        .map(|expires_at| expires_at <= chrono::Utc::now().timestamp() + 60)
        .unwrap_or(false)
}

async fn prepare_kiro_upstream_credentials(
    account: &KiroAccount,
) -> Result<KiroUpstreamCredentials, String> {
    let account = if account_token_needs_refresh(account) {
        kiro_account::refresh_account_token(&account.id).await?
    } else {
        account.clone()
    };
    if account.access_token.trim().is_empty() {
        return Err(format!("账号 {} 缺少 access_token", account.email));
    }

    let profile_arn = extract_profile_arn(&account);
    let region = resolve_kiro_upstream_region(&account, profile_arn.as_deref());
    let machine_id = read_kiro_machine_id();
    Ok(KiroUpstreamCredentials {
        access_token: account.access_token.trim().to_string(),
        profile_arn,
        region,
        user_agent: build_kiro_custom_user_agent(&machine_id),
        provider: account.login_provider.clone(),
        account_email: account.email.clone(),
    })
}

fn normalize_external_model_alias(external_model: &str) -> String {
    external_model.trim().to_ascii_lowercase()
}

fn get_internal_model_id(external_model: &str) -> Result<String, String> {
    let normalized_model = normalize_external_model_alias(external_model);
    let model_id = match normalized_model.as_str() {
        "gpt-5.5" | "gpt-5.5-turbo" | "gpt-5.5-preview" => "claude-opus-4.7",
        "gpt-4o" | "gpt-4o-2024-11-20" | "gpt-4o-2024-08-06" => "claude-opus-4.7",
        "gpt-4o-mini" | "gpt-4o-mini-2024-07-18" => "claude-sonnet-4.7",
        "o1" | "o1-preview" | "o1-2024-12-17" => "claude-opus-4.7",
        "o1-mini" | "o1-mini-2024-09-12" => "claude-sonnet-4.7",
        "o3" | "o3-mini" | "o3-mini-2025-01-31" => "claude-sonnet-4.7",
        "gpt-4-turbo" | "gpt-4-turbo-preview" | "gpt-4-1106-preview" | "gpt-4-0125-preview" => {
            "claude-opus-4.7"
        }
        "gpt-4" | "gpt-4-0613" | "gpt-4-32k" => "claude-opus-4.7",
        "gpt-3.5-turbo" | "gpt-3.5-turbo-16k" | "gpt-3.5-turbo-0125" => "claude-haiku-4.7",
        "claude-opus-4-7" | "claude-opus-4.7" | "opus-4-7" | "opus" => "claude-opus-4.7",
        "claude-opus-4-7-thinking" | "claude-opus-4.7-thinking" => "claude-opus-4.7",
        "claude-sonnet-4-7" | "claude-sonnet-4.7" | "sonnet-4-7" | "sonnet" => "claude-sonnet-4.7",
        "claude-sonnet-4-7-thinking" | "claude-sonnet-4.7-thinking" => "claude-sonnet-4.7",
        "claude-haiku-4-7" | "claude-haiku-4.7" | "haiku-4-7" | "haiku" => "claude-haiku-4.7",
        "claude-haiku-4-7-thinking" | "claude-haiku-4.7-thinking" => "claude-haiku-4.7",
        "claude-opus-4-6-20260205" => "claude-opus-4.6",
        "claude-opus-4-6" | "claude-opus-4.6" | "opus-4-6" => "claude-opus-4.6",
        "claude-opus-4-6-thinking" | "claude-opus-4.6-thinking" => "claude-opus-4.6",
        "claude-sonnet-4-6-20260217" => "claude-sonnet-4.6",
        "claude-sonnet-4-6" | "claude-sonnet-4.6" | "sonnet-4-6" => "claude-sonnet-4.6",
        "claude-sonnet-4-6-thinking" | "claude-sonnet-4.6-thinking" => "claude-sonnet-4.6",
        "claude-haiku-4-6" | "claude-haiku-4.6" | "haiku-4-6" => "claude-haiku-4.6",
        "claude-haiku-4-6-thinking" | "claude-haiku-4.6-thinking" => "claude-haiku-4.6",
        "claude-opus-4-5" | "claude-opus-4.5" => "claude-opus-4-5-20251101",
        "claude-opus-4-5-20251101" | "claude-opus-4-5-20251101-thinking" => {
            "claude-opus-4-5-20251101"
        }
        "claude-sonnet-4-5" | "claude-sonnet-4.5" | "claude-sonnet-latest" => {
            "claude-sonnet-4-5-20250929"
        }
        "claude-sonnet-4-5-20250929" | "claude-sonnet-4-5-20250929-thinking" => {
            "claude-sonnet-4-5-20250929"
        }
        "claude-haiku-4-5" | "claude-haiku-4.5" => "claude-haiku-4-5-20251001",
        "claude-haiku-4-5-20251001" | "claude-haiku-4-5-20251001-thinking" => {
            "claude-haiku-4-5-20251001"
        }
        "claude-sonnet-4" | "claude-sonnet-4-20250514" => "claude-sonnet-4",
        "claude-3-7-sonnet-20250219" | "claude-3.7-sonnet" => "claude-3-7-sonnet-20250219",
        "claude-3-5-sonnet-20241022" | "claude-3-5-sonnet-latest" | "claude-3.5-sonnet" => {
            "claude-3-5-sonnet-20241022"
        }
        "auto" | "default" => "auto",
        other if other.starts_with("gpt-5.5-") => "claude-opus-4.7",
        other if other.starts_with("claude-opus-4-7-") => "claude-opus-4.7",
        other if other.starts_with("claude-sonnet-4-7-") => "claude-sonnet-4.7",
        other if other.starts_with("claude-haiku-4-7-") => "claude-haiku-4.7",
        other if other.starts_with("claude-opus-4-6-") => "claude-opus-4.6",
        other if other.starts_with("claude-sonnet-4-6-") => "claude-sonnet-4.6",
        other if other.starts_with("claude-haiku-4-6-") => "claude-haiku-4.6",
        other => other,
    };
    Ok(model_id.to_string())
}

fn get_internal_model_id_with_fallback(
    external_model: &str,
    available_models: &[String],
) -> Result<String, String> {
    let mapped_model = get_internal_model_id(external_model)?;
    if available_models.is_empty() || available_models.contains(&mapped_model) {
        return Ok(mapped_model);
    }

    let fallback = if mapped_model.contains("opus-4.7") || mapped_model.contains("opus-4.6") {
        "claude-opus-4.5"
    } else if mapped_model.contains("sonnet-4.7") || mapped_model.contains("sonnet-4.6") {
        "claude-sonnet-4.5"
    } else if mapped_model.contains("haiku-4.7") || mapped_model.contains("haiku-4.6") {
        "claude-haiku-4.5"
    } else {
        return Ok(mapped_model);
    };

    logger::log_warn(&format!(
        "[KiroLocalAccess] 模型 {} 不在账号模型列表中，按 KiroAccountManager 逻辑降级到 {}",
        mapped_model, fallback
    ));
    Ok(fallback.to_string())
}

fn value_to_plain_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(text) => Some(text.clone()),
                Value::Object(obj) => {
                    let item_type = obj.get("type").and_then(Value::as_str).unwrap_or_default();
                    match item_type {
                        "text" | "input_text" | "output_text" => {
                            obj.get("text").and_then(Value::as_str).map(str::to_string)
                        }
                        "image" | "image_url" | "input_image" => Some("[Image]".to_string()),
                        "tool_result" | "web_search_tool_result" => {
                            obj.get("content").map(value_to_plain_text)
                        }
                        "reasoning" | "thinking" => obj
                            .get("summary")
                            .or_else(|| obj.get("thinking"))
                            .or_else(|| obj.get("text"))
                            .map(value_to_plain_text),
                        _ => obj.get("text").and_then(Value::as_str).map(str::to_string),
                    }
                }
                other => Some(other.to_string()),
            })
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(obj) => obj
            .get("text")
            .or_else(|| obj.get("content"))
            .map(value_to_plain_text)
            .unwrap_or_else(|| value.to_string()),
        other => other.to_string(),
    }
}

fn json_string_or_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(value) => serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string()),
        None => "{}".to_string(),
    }
}

fn parse_tool_arguments(arguments: &str) -> Value {
    serde_json::from_str(arguments).unwrap_or_else(|_| json!({}))
}

fn extract_anthropic_tool_calls(content: &Value) -> Vec<GatewayToolCall> {
    let Value::Array(items) = content else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| {
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
            if item_type != "tool_use" && item_type != "server_tool_use" {
                return None;
            }
            Some(GatewayToolCall {
                id: item
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                name: item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                arguments: json_string_or_value(item.get("input")),
            })
        })
        .filter(|call| !call.name.trim().is_empty())
        .collect()
}

fn extract_anthropic_tool_result_id(content: &Value) -> Option<String> {
    let Value::Array(items) = content else {
        return None;
    };
    items.iter().find_map(|item| {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        if item_type == "tool_result" || item_type == "web_search_tool_result" {
            item.get("tool_use_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        } else {
            None
        }
    })
}

fn convert_anthropic_tools(value: Option<&Value>) -> Vec<GatewayTool> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| {
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            let description = item
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .chars()
                .take(1024)
                .collect::<String>();
            let input_schema = item
                .get("input_schema")
                .or_else(|| item.get("inputSchema"))
                .or_else(|| item.get("parameters"))
                .cloned()
                .unwrap_or_else(|| json!({ "type": "object" }));
            Some(GatewayTool {
                name: name.to_string(),
                description,
                input_schema,
            })
        })
        .collect()
}

fn convert_openai_tools(value: Option<&Value>) -> Vec<GatewayTool> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| {
            let function = item.get("function").unwrap_or(item);
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            let description = function
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .chars()
                .take(1024)
                .collect::<String>();
            let input_schema = function
                .get("parameters")
                .cloned()
                .unwrap_or_else(|| json!({ "type": "object" }));
            Some(GatewayTool {
                name: name.to_string(),
                description,
                input_schema,
            })
        })
        .collect()
}

fn normalize_anthropic_gateway_request(body: &[u8]) -> Result<GatewayProxyRequest, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("解析请求体失败: {}", e))?;
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .unwrap_or("claude-sonnet-4-5-20250929")
        .to_string();
    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut messages = Vec::new();

    if let Some(system) = value.get("system") {
        let system_text = value_to_plain_text(system);
        if !system_text.trim().is_empty() {
            messages.push(GatewayMessage {
                role: "system".to_string(),
                content: Value::String(system_text),
                tool_calls: Vec::new(),
                tool_call_id: None,
            });
        }
    }

    let raw_messages = value
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "缺少 messages 字段".to_string())?;
    for message in raw_messages {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = message.get("content").cloned().unwrap_or(Value::Null);
        let tool_calls = extract_anthropic_tool_calls(&content);
        let tool_call_id = extract_anthropic_tool_result_id(&content);
        messages.push(GatewayMessage {
            role,
            content,
            tool_calls,
            tool_call_id,
        });
    }

    if messages.iter().all(|message| message.role == "system") {
        return Err("messages 中没有可发送给 Kiro 的用户消息".to_string());
    }

    Ok(GatewayProxyRequest {
        model,
        stream,
        messages,
        tools: convert_anthropic_tools(value.get("tools")),
        previous_response_id: value
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn normalize_openai_gateway_request(body: &[u8]) -> Result<GatewayProxyRequest, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("解析请求体失败: {}", e))?;
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .unwrap_or("claude-sonnet-4-5-20250929")
        .to_string();
    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let raw_messages = value
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "缺少 messages 字段".to_string())?;
    let mut messages = Vec::new();

    for message in raw_messages {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = message.get("content").cloned().unwrap_or(Value::Null);
        let tool_call_id = message
            .get("tool_call_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let tool_calls = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let function = item.get("function")?;
                        let name = function.get("name").and_then(Value::as_str)?;
                        Some(GatewayToolCall {
                            id: item
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            name: name.to_string(),
                            arguments: json_string_or_value(function.get("arguments")),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        messages.push(GatewayMessage {
            role,
            content,
            tool_calls,
            tool_call_id,
        });
    }

    if messages.is_empty() {
        return Err("messages 中没有可发送给 Kiro 的用户消息".to_string());
    }

    Ok(GatewayProxyRequest {
        model,
        stream,
        messages,
        tools: convert_openai_tools(value.get("tools")),
        previous_response_id: value
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn normalize_responses_gateway_request(body: &[u8]) -> Result<GatewayProxyRequest, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("解析请求体失败: {}", e))?;
    if value.get("messages").is_some() && value.get("input").is_none() {
        return normalize_openai_gateway_request(body);
    }

    let model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .unwrap_or("claude-sonnet-4-5-20250929")
        .to_string();
    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut messages = Vec::new();
    if let Some(instructions) = value.get("instructions") {
        let text = value_to_plain_text(instructions);
        if !text.trim().is_empty() {
            messages.push(GatewayMessage {
                role: "system".to_string(),
                content: Value::String(text),
                tool_calls: Vec::new(),
                tool_call_id: None,
            });
        }
    }

    match value.get("input") {
        Some(Value::String(text)) => messages.push(GatewayMessage {
            role: "user".to_string(),
            content: Value::String(text.clone()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }),
        Some(Value::Array(items)) => {
            let text = value_to_plain_text(&Value::Array(items.clone()));
            if !text.trim().is_empty() {
                messages.push(GatewayMessage {
                    role: "user".to_string(),
                    content: Value::String(text),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                });
            }
        }
        _ => {}
    }

    if messages.iter().all(|message| message.role == "system") {
        return Err("Responses 请求缺少可转换的 input".to_string());
    }

    Ok(GatewayProxyRequest {
        model,
        stream,
        messages,
        tools: convert_openai_tools(value.get("tools")),
        previous_response_id: value
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_string),
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
        Some("enterprise") | Some("builderid") | Some("internal") | Some("awsidc") => OIDC_AUTH_KEY,
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

    if let Some(value) = account
        .idc_region
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
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
    if let Some(value) = account
        .client_id
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        obj.insert("client_id".to_string(), Value::String(value.clone()));
    }
    if let Some(value) = account
        .scopes
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
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

    let profile_arn = extract_profile_arn(account)
        .ok_or_else(|| format!("账号 {} 缺少 profileArn", account.email))?;
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

fn load_optional_text(conn: &Connection, sql: &str, key: &str) -> Result<Option<String>, String> {
    conn.query_row(sql, params![key], |row| row.get::<_, String>(0))
        .optional()
        .map_err(|e| format!("读取 Kiro CLI 数据库失败: {}", e))
}

fn read_kiro_cli_auth_snapshot() -> Result<KiroCliDbSnapshot, String> {
    let db_path = kiro_cli_db_path()?;
    let conn =
        Connection::open(&db_path).map_err(|e| format!("打开 Kiro CLI 数据库失败: {}", e))?;

    let mut snapshot = KiroCliDbSnapshot::default();
    for key in KNOWN_AUTH_KEYS {
        let value = load_optional_text(&conn, "SELECT value FROM auth_kv WHERE key = ?1", key)?;
        snapshot.auth_values.insert(key.to_string(), value);
    }
    snapshot.profile_value = load_optional_text(
        &conn,
        "SELECT CAST(value AS TEXT) FROM state WHERE key = ?1",
        PROFILE_STATE_KEY,
    )?;
    Ok(snapshot)
}

fn extract_profile_arn_from_text(value: &str) -> Option<String> {
    let parsed = serde_json::from_str::<Value>(value).ok()?;
    pick_string(
        Some(&parsed),
        &[
            &["arn"],
            &["profileArn"],
            &["profile_arn"],
            &["profile", "arn"],
        ],
    )
}

fn current_kiro_cli_profile_matches(account: &KiroAccount) -> bool {
    let Some(account_profile_arn) = extract_profile_arn(account) else {
        return false;
    };
    let Ok(snapshot) = read_kiro_cli_auth_snapshot() else {
        return false;
    };
    snapshot
        .profile_value
        .as_deref()
        .and_then(extract_profile_arn_from_text)
        .map(|current_profile_arn| current_profile_arn == account_profile_arn)
        .unwrap_or(false)
}

fn prepare_kiro_cli_auth(account: &KiroAccount) -> Result<KiroCliDbSnapshot, String> {
    let db_path = kiro_cli_db_path()?;
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("创建 Kiro CLI 数据目录失败: {}", e))?;
    }

    let mut conn =
        Connection::open(&db_path).map_err(|e| format!("打开 Kiro CLI 数据库失败: {}", e))?;
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
        let value = load_optional_text(&tx, "SELECT value FROM auth_kv WHERE key = ?1", key)?;
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

fn prepare_kiro_cli_auth_for_request(account: &KiroAccount) -> Result<KiroCliAuthMode, String> {
    if current_kiro_cli_profile_matches(account) {
        return Ok(KiroCliAuthMode::ReuseCurrent);
    }
    prepare_kiro_cli_auth(account).map(KiroCliAuthMode::Injected)
}

fn restore_kiro_cli_auth(snapshot: KiroCliDbSnapshot) -> Result<(), String> {
    let db_path = kiro_cli_db_path()?;
    let mut conn =
        Connection::open(&db_path).map_err(|e| format!("重新打开 Kiro CLI 数据库失败: {}", e))?;
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
            tx.execute(
                "DELETE FROM state WHERE key = ?1",
                params![PROFILE_STATE_KEY],
            )
            .map_err(|e| format!("清理注入 profile 失败: {}", e))?;
        }
    }

    tx.commit().map_err(|e| format!("提交恢复事务失败: {}", e))
}

fn join_with_double_newline(left: &str, right: &str) -> String {
    match (left.trim().is_empty(), right.trim().is_empty()) {
        (true, true) => String::new(),
        (true, false) => right.to_string(),
        (false, true) => left.to_string(),
        (false, false) => format!("{}\n\n{}", left, right),
    }
}

fn merge_adjacent_gateway_messages(messages: &[GatewayMessage]) -> Vec<GatewayMessage> {
    let mut merged: Vec<GatewayMessage> = Vec::new();
    for message in messages {
        if let Some(last) = merged.last_mut() {
            if last.role == message.role && message.tool_call_id.is_none() {
                let existing = value_to_plain_text(&last.content);
                let incoming = value_to_plain_text(&message.content);
                last.content = Value::String(join_with_double_newline(&existing, &incoming));
                last.tool_calls.extend(message.tool_calls.clone());
                continue;
            }
        }
        merged.push(message.clone());
    }
    merged
}

fn gateway_tools_to_kiro(tools: &[GatewayTool]) -> Option<Vec<Value>> {
    if tools.is_empty() {
        return None;
    }
    Some(
        tools
            .iter()
            .map(|tool| {
                json!({
                    "toolSpecification": {
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": {
                            "json": tool.input_schema
                        }
                    }
                })
            })
            .collect(),
    )
}

fn tool_result_content_to_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .map(value_to_plain_text)
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Some(value) => value_to_plain_text(value),
        None => String::new(),
    }
}

fn extract_kiro_tool_results(message: &GatewayMessage) -> Vec<Value> {
    let mut results = Vec::new();

    if message.role == "tool" {
        if let Some(tool_use_id) = message.tool_call_id.as_deref() {
            results.push(json!({
                "content": [{ "text": value_to_plain_text(&message.content) }],
                "status": "success",
                "toolUseId": tool_use_id
            }));
        }
        return results;
    }

    let Value::Array(items) = &message.content else {
        return results;
    };

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        if item_type != "tool_result" && item_type != "web_search_tool_result" {
            continue;
        }
        let Some(tool_use_id) = item.get("tool_use_id").and_then(Value::as_str) else {
            continue;
        };
        let status = if item.get("is_error").and_then(Value::as_bool) == Some(true) {
            "error"
        } else {
            "success"
        };
        results.push(json!({
            "content": [{ "text": tool_result_content_to_text(item.get("content")) }],
            "status": status,
            "toolUseId": tool_use_id
        }));
    }
    results
}

fn gateway_tool_uses(tool_calls: &[GatewayToolCall]) -> Option<Vec<Value>> {
    if tool_calls.is_empty() {
        return None;
    }
    Some(
        tool_calls
            .iter()
            .map(|call| {
                json!({
                    "name": call.name,
                    "input": parse_tool_arguments(&call.arguments),
                    "toolUseId": call.id
                })
            })
            .collect(),
    )
}

fn build_kiro_user_context(tools: Option<Vec<Value>>, tool_results: Vec<Value>) -> Option<Value> {
    if tools.is_none() && tool_results.is_empty() {
        return None;
    }
    Some(json!({
        "toolResults": if tool_results.is_empty() { Value::Null } else { Value::Array(tool_results) },
        "tools": tools.unwrap_or_default()
    }))
    .map(|mut context| {
        if let Some(obj) = context.as_object_mut() {
            obj.retain(|_, value| !value.is_null() && !matches!(value, Value::Array(items) if items.is_empty()));
        }
        context
    })
}

async fn build_kiro_payload(
    request: &GatewayProxyRequest,
    profile_arn: Option<String>,
    available_models: &[String],
) -> Result<Value, String> {
    let model_id = get_internal_model_id_with_fallback(&request.model, available_models)?;
    let conversation_id = request
        .previous_response_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut system_prompt = String::new();
    let mut non_system_messages = Vec::new();

    for message in &request.messages {
        if message.role == "system" {
            let text = value_to_plain_text(&message.content);
            if !text.trim().is_empty() {
                system_prompt = join_with_double_newline(&system_prompt, &text);
            }
        } else {
            non_system_messages.push(message.clone());
        }
    }

    if non_system_messages.is_empty() {
        return Err("没有可发送给 Kiro 的消息".to_string());
    }

    let merged_messages = merge_adjacent_gateway_messages(&non_system_messages);
    let first_user_index = merged_messages
        .iter()
        .position(|message| matches!(message.role.as_str(), "user" | "tool"));
    let history = if merged_messages.len() > 1 {
        let mut items = Vec::new();
        for (index, message) in merged_messages[..merged_messages.len() - 1]
            .iter()
            .enumerate()
        {
            match message.role.as_str() {
                "assistant" => {
                    items.push(json!({
                        "assistantResponseMessage": {
                            "content": value_to_plain_text(&message.content),
                            "toolUses": gateway_tool_uses(&message.tool_calls)
                        }
                    }));
                }
                "user" | "tool" => {
                    let mut content = if message.role == "tool" {
                        String::new()
                    } else {
                        value_to_plain_text(&message.content)
                    };
                    if Some(index) == first_user_index && !system_prompt.trim().is_empty() {
                        content = join_with_double_newline(&system_prompt, &content);
                    }
                    items.push(json!({
                        "userInputMessage": {
                            "content": content,
                            "modelId": model_id,
                            "origin": "AI_EDITOR",
                            "userInputMessageContext": build_kiro_user_context(
                                None,
                                extract_kiro_tool_results(message)
                            )
                        }
                    }));
                }
                _ => {}
            }
        }
        if items.is_empty() {
            None
        } else {
            Some(items)
        }
    } else {
        None
    };

    let current_message = merged_messages
        .last()
        .ok_or_else(|| "没有当前消息".to_string())?;
    let mut current_content = if current_message.role == "tool" {
        String::new()
    } else {
        value_to_plain_text(&current_message.content)
    };
    if history.is_none() && !system_prompt.trim().is_empty() {
        current_content = join_with_double_newline(&system_prompt, &current_content);
    }
    if current_message.role == "assistant" || current_content.trim().is_empty() {
        current_content = if current_content.trim().is_empty() {
            "Continue".to_string()
        } else {
            current_content
        };
    }

    let mut payload = json!({
        "conversationState": {
            "chatTriggerType": "MANUAL",
            "conversationId": conversation_id,
            "agentContinuationId": conversation_id,
            "agentTaskType": "vibe",
            "currentMessage": {
                "userInputMessage": {
                    "content": current_content,
                    "modelId": model_id,
                    "origin": "AI_EDITOR",
                    "userInputMessageContext": build_kiro_user_context(
                        gateway_tools_to_kiro(&request.tools),
                        extract_kiro_tool_results(current_message)
                    )
                }
            },
            "history": history,
            "customizationArn": Value::Null,
            "workspaceId": Value::Null
        },
        "profileArn": profile_arn
    });
    prune_nulls(&mut payload);
    Ok(payload)
}

fn prune_nulls(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for item in map.values_mut() {
                prune_nulls(item);
            }
            map.retain(|_, value| {
                !value.is_null() && !matches!(value, Value::Array(items) if items.is_empty())
            });
        }
        Value::Array(items) => {
            for item in items {
                prune_nulls(item);
            }
        }
        _ => {}
    }
}

fn trim_payload_history_if_needed(payload: &mut Value) {
    while serde_json::to_vec(payload)
        .map(|bytes| bytes.len() > MAX_KIRO_PAYLOAD_SIZE)
        .unwrap_or(false)
    {
        let Some(history) = payload
            .pointer_mut("/conversationState/history")
            .and_then(Value::as_array_mut)
        else {
            break;
        };
        if history.is_empty() {
            break;
        }
        history.remove(0);
    }
}

fn map_direct_upstream_error(status: u16, body: &str) -> DirectProxyError {
    let message = sanitize_upstream_error(&extract_upstream_error_message(body));
    let mapped_status = if status == 401 {
        401
    } else if status == 403 {
        403
    } else if status == 429
        || body.to_ascii_lowercase().contains("throttlingexception")
        || body
            .to_ascii_lowercase()
            .contains("servicequotaexceededexception")
    {
        429
    } else if status == 400 || body.to_ascii_lowercase().contains("validationexception") {
        400
    } else if status >= 500 {
        502
    } else {
        status
    };
    DirectProxyError {
        status: mapped_status,
        message,
    }
}

fn extract_upstream_error_message(body: &str) -> String {
    if body.trim().is_empty() {
        return "上游返回空错误响应".to_string();
    }
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        for pointer in [
            "/message",
            "/Message",
            "/error/message",
            "/reason",
            "/__type",
            "/errorCode",
        ] {
            if let Some(text) = value.pointer(pointer).and_then(Value::as_str) {
                return text.to_string();
            }
        }
    }
    body.to_string()
}

fn sanitize_upstream_error(message: &str) -> String {
    let mut sanitized = message.to_string();
    for pattern in [
        r"Bearer\s+[A-Za-z0-9._\-]+",
        r#""accessToken"\s*:\s*"[^"]+""#,
        r#""refreshToken"\s*:\s*"[^"]+""#,
        r#""clientSecret"\s*:\s*"[^"]+""#,
    ] {
        if let Ok(regex) = Regex::new(pattern) {
            sanitized = regex.replace_all(&sanitized, "[REDACTED]").to_string();
        }
    }
    sanitized
}

fn with_kiro_upstream_headers(
    builder: reqwest::RequestBuilder,
    upstream: &KiroUpstreamCredentials,
    accept: &str,
    include_agent_mode: bool,
) -> reqwest::RequestBuilder {
    let mut builder = builder
        .header("Authorization", format!("Bearer {}", upstream.access_token))
        .header("Content-Type", "application/json")
        .header("Accept", accept)
        .header("host", format!("q.{}.amazonaws.com", upstream.region))
        .header("user-agent", upstream.user_agent.clone())
        .header("x-amz-user-agent", upstream.user_agent.clone())
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=3");

    if should_send_codewhisperer_optout() {
        builder = builder.header("x-amzn-codewhisperer-optout", "true");
    }
    if include_agent_mode {
        builder = builder.header("x-amzn-kiro-agent-mode", DEFAULT_AGENT_MODE);
    }
    if let Some(profile_arn) = upstream
        .profile_arn
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        builder = builder.header("x-amzn-kiro-profile-arn", profile_arn);
    }
    if upstream
        .provider
        .as_deref()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("Internal"))
    {
        builder = builder.header("redirect-for-internal", "true");
    }
    builder
}

async fn list_available_models_direct(
    http: &reqwest::Client,
    upstream: &KiroUpstreamCredentials,
) -> Result<Vec<String>, DirectProxyError> {
    let mut url = format!(
        "https://q.{}.amazonaws.com/ListAvailableModels?origin=AI_EDITOR&maxResults=50",
        upstream.region
    );
    if let Some(profile_arn) = upstream
        .profile_arn
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        url.push_str("&profileArn=");
        url.push_str(&urlencoding::encode(profile_arn));
    }

    let response = with_kiro_upstream_headers(http.get(&url), upstream, "application/json", false)
        .send()
        .await
        .map_err(|e| DirectProxyError {
            status: 502,
            message: format!("ListAvailableModels 请求失败: {e}"),
        })?;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        return Err(map_direct_upstream_error(status, &body));
    }

    let value: Value = serde_json::from_str(&body).map_err(|e| DirectProxyError {
        status: 502,
        message: format!("解析 ListAvailableModels 响应失败: {e}"),
    })?;
    Ok(value
        .get("models")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("modelId")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default())
}

async fn send_generate_request_direct(
    http: &reqwest::Client,
    upstream: &KiroUpstreamCredentials,
    payload: &Value,
) -> Result<Vec<u8>, DirectProxyError> {
    let url = format!(
        "https://q.{}.amazonaws.com/generateAssistantResponse",
        upstream.region
    );
    let response = with_kiro_upstream_headers(
        http.post(&url),
        upstream,
        "application/vnd.amazon.eventstream",
        true,
    )
    .json(payload)
    .send()
    .await
    .map_err(|e| DirectProxyError {
        status: 502,
        message: format!("上游请求失败: {e}"),
    })?;

    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        let body = response.text().await.unwrap_or_default();
        return Err(map_direct_upstream_error(status, &body));
    }

    response
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|e| DirectProxyError {
            status: 502,
            message: format!("读取上游响应失败: {e}"),
        })
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = if crc & 1 == 1 { 0xEDB8_8320 } else { 0 };
            crc = (crc >> 1) ^ mask;
        }
    }
    crc ^ 0xFFFF_FFFF
}

fn decode_eventstream_message(buffer: &[u8]) -> Result<Option<(Vec<u8>, usize)>, String> {
    const MINIMUM_MESSAGE_LENGTH: usize = 16;
    if buffer.len() < MINIMUM_MESSAGE_LENGTH {
        return Ok(None);
    }
    let total_len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
    if total_len < MINIMUM_MESSAGE_LENGTH {
        return Err(format!("EventStream 消息长度无效: {total_len}"));
    }
    if buffer.len() < total_len {
        return Ok(None);
    }

    let msg = &buffer[..total_len];
    let headers_len = u32::from_be_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
    let prelude_crc = u32::from_be_bytes([msg[8], msg[9], msg[10], msg[11]]);
    if crc32(&msg[0..8]) != prelude_crc {
        return Err("EventStream prelude CRC 校验失败".to_string());
    }
    let msg_crc = u32::from_be_bytes([
        msg[total_len - 4],
        msg[total_len - 3],
        msg[total_len - 2],
        msg[total_len - 1],
    ]);
    if crc32(&msg[0..total_len - 4]) != msg_crc {
        return Err("EventStream message CRC 校验失败".to_string());
    }

    let payload_start = 12 + headers_len;
    let payload_end = total_len - 4;
    if payload_start > payload_end || payload_end > msg.len() {
        return Err("EventStream payload 边界无效".to_string());
    }
    Ok(Some((msg[payload_start..payload_end].to_vec(), total_len)))
}

fn decode_eventstream_payload_text(bytes: &[u8]) -> String {
    let mut offset = 0usize;
    let mut payloads = Vec::new();
    while offset < bytes.len() {
        match decode_eventstream_message(&bytes[offset..]) {
            Ok(Some((payload, consumed))) => {
                if let Ok(text) = String::from_utf8(payload) {
                    if !text.trim().is_empty() {
                        payloads.push(text);
                    }
                }
                offset += consumed;
            }
            Ok(None) => break,
            Err(err) => {
                logger::log_warn(&format!("[KiroLocalAccess] EventStream 解码失败: {err}"));
                return String::from_utf8_lossy(bytes).to_string();
            }
        }
    }
    if payloads.is_empty() {
        String::from_utf8_lossy(bytes).to_string()
    } else {
        payloads.join("\n")
    }
}

fn extract_json_object(source: &str) -> Option<String> {
    if !source.starts_with('{') {
        return None;
    }
    let mut brace_count = 0i32;
    let mut in_string = false;
    let mut escape_next = false;
    for (index, ch) in source.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape_next = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if !in_string {
            if ch == '{' {
                brace_count += 1;
            } else if ch == '}' {
                brace_count -= 1;
                if brace_count == 0 {
                    return Some(source[..=index].to_string());
                }
            }
        }
    }
    None
}

fn parse_kiro_event(json_str: &str) -> Option<KiroEvent> {
    let value: Value = serde_json::from_str(json_str).ok()?;
    if let Some(usage) = value.get("usage").and_then(Value::as_object) {
        let input_tokens = usage
            .get("inputTokens")
            .or_else(|| usage.get("input_tokens"))
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32;
        let output_tokens = usage
            .get("outputTokens")
            .or_else(|| usage.get("output_tokens"))
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32;
        let cache_read_input_tokens = usage
            .get("cacheReadInputTokens")
            .or_else(|| usage.get("cache_read_input_tokens"))
            .and_then(Value::as_i64)
            .map(|value| value as i32);
        let cache_creation_input_tokens = usage
            .get("cacheCreationInputTokens")
            .or_else(|| usage.get("cache_creation_input_tokens"))
            .and_then(Value::as_i64)
            .map(|value| value as i32);
        if input_tokens > 0
            || output_tokens > 0
            || cache_read_input_tokens.is_some()
            || cache_creation_input_tokens.is_some()
        {
            return Some(KiroEvent::Usage {
                input_tokens,
                output_tokens,
                cache_read_input_tokens,
                cache_creation_input_tokens,
            });
        }
    }

    if let Some(text) = value
        .get("reasoningContentEvent")
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("delta")
                .and_then(|item| item.get("thinking"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .get("contentBlockDelta")
                .and_then(|item| item.get("delta"))
                .and_then(|item| item.get("thinking"))
                .and_then(Value::as_str)
        })
    {
        if !text.is_empty() {
            return Some(KiroEvent::Thinking(text.to_string()));
        }
    }

    if let Some(tool_use_id) = value.get("toolUseId").and_then(Value::as_str) {
        if value.get("stop").and_then(Value::as_bool) == Some(true) {
            return Some(KiroEvent::ToolUseStop {
                id: tool_use_id.to_string(),
            });
        }
        if let Some(input) = value.get("input") {
            let input_delta = json_string_or_value(Some(input));
            if !input_delta.is_empty() && input_delta != "{}" {
                return Some(KiroEvent::ToolUseInputDelta {
                    id: tool_use_id.to_string(),
                    input_delta,
                });
            }
        }
        if let Some(name) = value.get("name").and_then(Value::as_str) {
            if !name.is_empty() {
                return Some(KiroEvent::ToolUseStart {
                    id: tool_use_id.to_string(),
                    name: name.to_string(),
                });
            }
        }
    }

    if let Some(tool) = value
        .get("assistantResponseEvent")
        .and_then(|item| item.get("toolUses"))
        .and_then(Value::as_array)
        .and_then(|items| items.first())
    {
        let id = tool
            .get("toolUseId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = tool
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !name.is_empty() {
            return Some(KiroEvent::ToolUseStart { id, name });
        }
    }

    if let Some(text) = value
        .get("content")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("delta")
                .and_then(|item| item.get("text"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .get("contentBlockDelta")
                .and_then(|item| item.get("delta"))
                .and_then(|item| item.get("text"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .get("assistantResponseEvent")
                .and_then(|item| item.get("content"))
                .and_then(Value::as_str)
        })
    {
        if !text.is_empty() {
            return Some(KiroEvent::Text(text.to_string()));
        }
    }
    None
}

fn aggregate_kiro_response(raw: &str) -> AggregatedKiroResponse {
    let mut aggregated = AggregatedKiroResponse::default();
    let mut remaining = raw;
    let mut tool_accumulators: HashMap<String, (String, String)> = HashMap::new();

    while let Some(start) = remaining.find('{') {
        remaining = &remaining[start..];
        let Some(json_str) = extract_json_object(remaining) else {
            break;
        };
        let json_len = json_str.len();
        if let Some(event) = parse_kiro_event(&json_str) {
            match event {
                KiroEvent::Text(text) => aggregated.text.push_str(&text),
                KiroEvent::Thinking(text) => aggregated.thinking.push_str(&text),
                KiroEvent::ToolUseStart { id, name } => {
                    tool_accumulators.entry(id).or_insert((name, String::new()));
                }
                KiroEvent::ToolUseInputDelta { id, input_delta } => {
                    if let Some((_, current_input)) = tool_accumulators.get_mut(&id) {
                        current_input.push_str(&input_delta);
                    } else {
                        tool_accumulators.insert(id, (String::new(), input_delta));
                    }
                }
                KiroEvent::ToolUseStop { id } => {
                    if let Some((name, input)) = tool_accumulators.remove(&id) {
                        aggregated.tool_calls.push(GatewayToolCall {
                            id,
                            name,
                            arguments: input,
                        });
                    }
                }
                KiroEvent::Usage {
                    input_tokens,
                    output_tokens,
                    cache_read_input_tokens,
                    cache_creation_input_tokens,
                } => {
                    aggregated.input_tokens = input_tokens;
                    aggregated.output_tokens = output_tokens;
                    aggregated.cache_read_input_tokens = cache_read_input_tokens;
                    aggregated.cache_creation_input_tokens = cache_creation_input_tokens;
                }
            }
        }
        remaining = &remaining[json_len..];
    }

    for (id, (name, input)) in tool_accumulators {
        if !name.is_empty() || !input.is_empty() {
            aggregated.tool_calls.push(GatewayToolCall {
                id,
                name,
                arguments: input,
            });
        }
    }
    dedupe_gateway_tool_calls(&mut aggregated.tool_calls);
    aggregated
}

fn dedupe_gateway_tool_calls(tool_calls: &mut Vec<GatewayToolCall>) {
    let mut seen = HashSet::new();
    tool_calls.retain(|call| seen.insert(call.id.clone()));
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

async fn generate_via_kiro_cli(
    request: &CompletionRequest,
    account: &KiroAccount,
) -> Result<String, String> {
    let _guard = request_lock().lock().await;
    let account_clone = account.clone();
    let auth_mode =
        tokio::task::spawn_blocking(move || prepare_kiro_cli_auth_for_request(&account_clone))
            .await
            .map_err(|e| format!("准备 Kiro CLI 凭据任务失败: {}", e))??;

    let result = invoke_kiro_cli(request.prompt.clone(), request.model.clone()).await;

    if let KiroCliAuthMode::Injected(snapshot) = auth_mode {
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
    }

    result
}

fn anthropic_content_blocks(aggregated: &AggregatedKiroResponse) -> Vec<Value> {
    let mut content = Vec::new();
    if !aggregated.thinking.is_empty() {
        content.push(json!({
            "type": "thinking",
            "thinking": aggregated.thinking
        }));
    }
    if !aggregated.text.is_empty() {
        content.push(json!({
            "type": "text",
            "text": aggregated.text
        }));
    }
    for call in &aggregated.tool_calls {
        content.push(json!({
            "type": "tool_use",
            "id": call.id,
            "name": call.name,
            "input": parse_tool_arguments(&call.arguments)
        }));
    }
    if content.is_empty() {
        content.push(json!({ "type": "text", "text": "" }));
    }
    content
}

fn build_direct_anthropic_response(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    stream: bool,
) -> ProxyResult {
    let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
    let stop_reason = if aggregated.tool_calls.is_empty() {
        "end_turn"
    } else {
        "tool_use"
    };

    if !stream {
        let body = json!({
            "id": message_id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": anthropic_content_blocks(aggregated),
            "stop_reason": stop_reason,
            "stop_sequence": Value::Null,
            "usage": {
                "input_tokens": aggregated.input_tokens,
                "output_tokens": aggregated.output_tokens
            }
        });
        return ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: serde_json::to_vec(&body).unwrap_or_default(),
            is_stream: false,
        };
    }

    let mut body = String::new();
    push_sse_event(
        &mut body,
        Some("message_start"),
        &json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": Value::Null,
                "stop_sequence": Value::Null,
                "usage": {
                    "input_tokens": aggregated.input_tokens,
                    "output_tokens": 0
                }
            }
        }),
    );

    let mut index = 0usize;
    if !aggregated.thinking.is_empty() {
        push_sse_event(
            &mut body,
            Some("content_block_start"),
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": { "type": "thinking", "thinking": "" }
            }),
        );
        for chunk in chunk_text_for_stream(&aggregated.thinking, 160) {
            push_sse_event(
                &mut body,
                Some("content_block_delta"),
                &json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": { "type": "thinking_delta", "thinking": chunk }
                }),
            );
        }
        push_sse_event(
            &mut body,
            Some("content_block_stop"),
            &json!({ "type": "content_block_stop", "index": index }),
        );
        index += 1;
    }

    if !aggregated.text.is_empty() {
        push_sse_event(
            &mut body,
            Some("content_block_start"),
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": { "type": "text", "text": "" }
            }),
        );
        for chunk in chunk_text_for_stream(&aggregated.text, 160) {
            push_sse_event(
                &mut body,
                Some("content_block_delta"),
                &json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": { "type": "text_delta", "text": chunk }
                }),
            );
        }
        push_sse_event(
            &mut body,
            Some("content_block_stop"),
            &json!({ "type": "content_block_stop", "index": index }),
        );
        index += 1;
    }

    for call in &aggregated.tool_calls {
        push_sse_event(
            &mut body,
            Some("content_block_start"),
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {
                    "type": "tool_use",
                    "id": call.id,
                    "name": call.name,
                    "input": {}
                }
            }),
        );
        if !call.arguments.trim().is_empty() {
            push_sse_event(
                &mut body,
                Some("content_block_delta"),
                &json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": call.arguments
                    }
                }),
            );
        }
        push_sse_event(
            &mut body,
            Some("content_block_stop"),
            &json!({ "type": "content_block_stop", "index": index }),
        );
        index += 1;
    }

    push_sse_event(
        &mut body,
        Some("message_delta"),
        &json!({
            "type": "message_delta",
            "delta": { "stop_reason": stop_reason, "stop_sequence": Value::Null },
            "usage": { "output_tokens": aggregated.output_tokens }
        }),
    );
    push_sse_event(
        &mut body,
        Some("message_stop"),
        &json!({ "type": "message_stop" }),
    );

    ProxyResult {
        status: 200,
        headers: vec![("content-type".to_string(), "text/event-stream".to_string())],
        body: body.into_bytes(),
        is_stream: true,
    }
}

fn push_sse_event(body: &mut String, event: Option<&str>, payload: &Value) {
    if let Some(event) = event {
        body.push_str("event: ");
        body.push_str(event);
        body.push('\n');
    }
    body.push_str("data: ");
    body.push_str(&serde_json::to_string(payload).unwrap_or_default());
    body.push_str("\n\n");
}

fn build_direct_openai_response(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    stream: bool,
) -> ProxyResult {
    let request_id = format!("chatcmpl-{}", uuid::Uuid::new_v4().simple());
    let created = chrono::Utc::now().timestamp();
    let finish_reason = if aggregated.tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };

    if !stream {
        let tool_calls = if aggregated.tool_calls.is_empty() {
            Value::Null
        } else {
            Value::Array(
                aggregated
                    .tool_calls
                    .iter()
                    .map(|call| {
                        json!({
                            "id": call.id,
                            "type": "function",
                            "function": {
                                "name": call.name,
                                "arguments": call.arguments
                            }
                        })
                    })
                    .collect(),
            )
        };
        let mut message = json!({
            "role": "assistant",
            "content": if aggregated.text.is_empty() { Value::Null } else { Value::String(aggregated.text.clone()) },
            "tool_calls": tool_calls
        });
        prune_nulls(&mut message);
        let body = json!({
            "id": request_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": finish_reason
            }],
            "usage": {
                "prompt_tokens": aggregated.input_tokens,
                "completion_tokens": aggregated.output_tokens,
                "total_tokens": aggregated.input_tokens + aggregated.output_tokens
            }
        });
        return ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: serde_json::to_vec(&body).unwrap_or_default(),
            is_stream: false,
        };
    }

    let mut body = String::new();
    push_sse_data(
        &mut body,
        &json!({
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{ "index": 0, "delta": { "role": "assistant" }, "finish_reason": Value::Null }]
        }),
    );
    for chunk in chunk_text_for_stream(&aggregated.text, 160) {
        push_sse_data(
            &mut body,
            &json!({
                "id": request_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{ "index": 0, "delta": { "content": chunk }, "finish_reason": Value::Null }]
            }),
        );
    }
    push_sse_data(
        &mut body,
        &json!({
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{ "index": 0, "delta": {}, "finish_reason": finish_reason }]
        }),
    );
    body.push_str("data: [DONE]\n\n");
    ProxyResult {
        status: 200,
        headers: vec![("content-type".to_string(), "text/event-stream".to_string())],
        body: body.into_bytes(),
        is_stream: true,
    }
}

fn push_sse_data(body: &mut String, payload: &Value) {
    body.push_str("data: ");
    body.push_str(&serde_json::to_string(payload).unwrap_or_default());
    body.push_str("\n\n");
}

fn build_direct_responses_response(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    stream: bool,
) -> ProxyResult {
    let response_id = format!("resp_{}", uuid::Uuid::new_v4().simple());
    let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
    let created_at = chrono::Utc::now().timestamp();
    let response = json!({
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": "completed",
        "model": model,
        "output": [{
            "id": message_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": aggregated.text,
                "annotations": []
            }]
        }],
        "usage": {
            "input_tokens": aggregated.input_tokens,
            "output_tokens": aggregated.output_tokens,
            "total_tokens": aggregated.input_tokens + aggregated.output_tokens
        }
    });

    if stream {
        let mut body = String::new();
        push_sse_data(
            &mut body,
            &json!({
                "type": "response.completed",
                "response": response
            }),
        );
        body.push_str("data: [DONE]\n\n");
        ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "text/event-stream".to_string())],
            body: body.into_bytes(),
            is_stream: true,
        }
    } else {
        ProxyResult {
            status: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: serde_json::to_vec(&response).unwrap_or_default(),
            is_stream: false,
        }
    }
}

async fn proxy_via_kiro_direct(
    request: &GatewayProxyRequest,
    account: &KiroAccount,
    protocol: ApiProtocol,
) -> Result<ProxyResult, String> {
    let http = build_kiro_http_client(true)?;
    let mut upstream = prepare_kiro_upstream_credentials(account).await?;
    let available_models = match list_available_models_direct(&http, &upstream).await {
        Ok(models) => models,
        Err(err) => {
            logger::log_warn(&format!(
                "[KiroLocalAccess] 获取账号模型列表失败: account={}, status={}, error={}，继续按请求模型直连",
                upstream.account_email, err.status, err.message
            ));
            Vec::new()
        }
    };
    let mut payload =
        build_kiro_payload(request, upstream.profile_arn.clone(), &available_models).await?;
    trim_payload_history_if_needed(&mut payload);

    let response_bytes = match send_generate_request_direct(&http, &upstream, &payload).await {
        Ok(bytes) => bytes,
        Err(err) if err.status == 401 || err.status == 403 => {
            logger::log_warn(&format!(
                "[KiroLocalAccess] Kiro 上游认证失败，尝试刷新账号后重试: account={}, status={}, error={}",
                upstream.account_email, err.status, err.message
            ));
            let refreshed = kiro_account::refresh_account_token(&account.id).await?;
            upstream = prepare_kiro_upstream_credentials(&refreshed).await?;
            send_generate_request_direct(&http, &upstream, &payload)
                .await
                .map_err(|retry_err| {
                    format!(
                        "Kiro 上游请求失败(status={}): {}",
                        retry_err.status, retry_err.message
                    )
                })?
        }
        Err(err) => {
            return Err(format!(
                "Kiro 上游请求失败(status={}): {}",
                err.status, err.message
            ));
        }
    };
    let payload_text = decode_eventstream_payload_text(&response_bytes);
    let aggregated = aggregate_kiro_response(&payload_text);

    Ok(match protocol {
        ApiProtocol::OpenAi => {
            build_direct_openai_response(&request.model, &aggregated, request.stream)
        }
        ApiProtocol::Anthropic => {
            build_direct_anthropic_response(&request.model, &aggregated, request.stream)
        }
        ApiProtocol::Responses => {
            build_direct_responses_response(&request.model, &aggregated, request.stream)
        }
    })
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
        let response = build_models_response(&collection).await;
        write_json_response(&mut stream, 200, &response).await;
        return Ok(());
    }

    if parsed.target.starts_with("/v1/messages/count_tokens") {
        match parse_anthropic_request(&parsed.body) {
            Ok(request) => {
                let response = build_anthropic_count_tokens_response(&request);
                write_json_response(&mut stream, 200, &response).await;
            }
            Err(err) => {
                write_error(&mut stream, 400, err.as_str()).await;
            }
        }
        return Ok(());
    }

    let (gateway_request, protocol) = if parsed.target.starts_with("/v1/chat/completions") {
        match normalize_openai_gateway_request(&parsed.body) {
            Ok(request) => (request, ApiProtocol::OpenAi),
            Err(err) => {
                write_error(&mut stream, 400, err.as_str()).await;
                return Ok(());
            }
        }
    } else if parsed.target.starts_with("/v1/messages") {
        match normalize_anthropic_gateway_request(&parsed.body) {
            Ok(request) => (request, ApiProtocol::Anthropic),
            Err(err) => {
                write_error(&mut stream, 400, err.as_str()).await;
                return Ok(());
            }
        }
    } else if parsed.target.starts_with("/v1/responses") {
        match normalize_responses_gateway_request(&parsed.body) {
            Ok(request) => (request, ApiProtocol::Responses),
            Err(err) => {
                write_error(&mut stream, 400, err.as_str()).await;
                return Ok(());
            }
        }
    } else {
        write_error(
            &mut stream,
            404,
            "仅支持 /v1/models、/v1/chat/completions、/v1/messages、/v1/messages/count_tokens 和 /v1/responses",
        )
        .await;
        return Ok(());
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

        match proxy_via_kiro_direct(&gateway_request, &account, protocol).await {
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
    let response_text = response.text().await.unwrap_or_else(|_| String::new());

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
        rt.collection
            .as_ref()
            .map(|item| item.enabled)
            .unwrap_or(false)
    };
    if enabled {
        if let Err(err) = ensure_gateway_running().await {
            logger::log_warn(&format!("[KiroLocalAccess] 恢复网关失败: {}", err));
            set_last_error(Some(err)).await;
        }
    }
}
