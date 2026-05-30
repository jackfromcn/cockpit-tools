use crate::models::kiro::KiroAccount;
use crate::models::kiro_local_access::{
    KiroLocalAccessCollection, KiroLocalAccessState, KiroLocalAccessStats,
    KiroLocalAccessTestFailure, KiroLocalAccessTestResult,
};
use crate::modules::atomic_write::write_string_atomic;
use crate::modules::kiro_gateway::models::{
    AggregatedKiroResponse, ApiProtocol, CompletionRequest, ConversationState, CurrentMessage,
    DirectProxyError, GatewayMessage, GatewayProxyRequest, GatewayTool, GatewayToolCall,
    HistoryAssistantMessage, HistoryItem, HistoryUserMessage, ImageBlock, ImageSource,
    KiroCliAuthMode, KiroCliDbSnapshot, KiroEvent, KiroInputSchema, KiroPayload, KiroTool,
    KiroToolResult, KiroToolResultContent, KiroToolSpec, KiroToolUse, KiroUpstreamCredentials,
    ProxyResult, ResponsesSessionEntry, ServerToolCall, UserInputMessage, UserInputMessageContext,
};
use crate::modules::{kiro_account, logger};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{SecondsFormat, TimeZone};
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::lookup_host;
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
const TOOL_DESCRIPTION_MAX_LENGTH: usize = 1024;
const WEB_SEARCH_TOOL_NAME: &str = "web_search";
const WEB_SEARCH_TOOL_DESCRIPTION: &str =
    "Search the web for current information and return relevant results.";
const MAX_SERVER_WEB_SEARCH_ITERATIONS: usize = 8;
const MAX_IMAGE_SOURCE_BYTES: usize = 5 * 1024 * 1024;
const MAX_IMAGE_REDIRECTS: usize = 3;
const IMAGE_FETCH_TIMEOUT_SECONDS: u64 = 15;
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
    responses_sessions: HashMap<String, ResponsesSessionEntry>,
}

struct ProxyExecutionOutcome {
    aggregated: AggregatedKiroResponse,
    server_tool_calls: Vec<ServerToolCall>,
}

#[derive(Debug, Clone)]
struct WebSearchSource {
    title: String,
    url: String,
}

#[derive(Debug)]
struct ParsedRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
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
    let machine_id = account
        .machine_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(read_kiro_machine_id);
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

    // 优先用传入的 available_models，为空时用 DEFAULT_KIRO_MODELS 作为 fallback
    let model_list: Vec<String> = if available_models.is_empty() {
        DEFAULT_KIRO_MODELS.iter().map(|s| s.to_string()).collect()
    } else {
        available_models.to_vec()
    };

    // 精确匹配
    if model_list.contains(&mapped_model) {
        return Ok(mapped_model);
    }

    // 同家族模糊匹配
    let family = if mapped_model.contains("opus") {
        "opus"
    } else if mapped_model.contains("sonnet") {
        "sonnet"
    } else if mapped_model.contains("haiku") {
        "haiku"
    } else {
        return Ok(mapped_model);
    };

    // 从列表末尾找（通常越新越靠后）
    let fallback = model_list
        .iter()
        .rev()
        .find(|m| m.contains(family) && !m.contains("thinking"))
        .cloned();

    if let Some(fallback_model) = fallback {
        if fallback_model != mapped_model {
            logger::log_warn(&format!(
                "[KiroLocalAccess] 模型 {} 不在列表中，降级到 {}",
                mapped_model, fallback_model
            ));
        }
        Ok(fallback_model)
    } else {
        Ok(mapped_model)
    }
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

fn extract_text_blocks(value: &Value, text_types: &[&str]) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
                if text_types.contains(&item_type) {
                    item.get("text").and_then(Value::as_str).map(str::to_string)
                } else if item_type == "image" {
                    Some("[Image]".to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) => map
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

fn extract_text_content(content: Option<&Value>) -> String {
    match content {
        None => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(value @ Value::Array(_)) => {
            extract_text_blocks(value, &["text", "input_text", "output_text"])
        }
        Some(other) => other.to_string(),
    }
}

fn meaningful_optional_value(value: Option<Value>) -> Option<Value> {
    match value {
        Some(Value::Null) => None,
        Some(Value::String(text)) if text.trim().is_empty() => None,
        Some(Value::Array(items)) if items.is_empty() => None,
        Some(Value::Object(map)) if map.is_empty() => None,
        other => other,
    }
}

fn extract_reasoning_content(content: Option<&Value>) -> Option<Value> {
    let content = content?;

    if let Some(existing) = content.get("reasoningContent") {
        return meaningful_optional_value(Some(existing.clone()));
    }

    let content_items = content.get("content").unwrap_or(content);
    let Value::Array(items) = content_items else {
        return None;
    };

    let mut texts = Vec::new();
    let mut signature: Option<Value> = None;
    let mut redacted_content: Option<Value> = None;

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        if item_type != "reasoning" && item_type != "thinking" {
            continue;
        }

        if let Some(text) = item
            .get("summary")
            .map(|value| extract_text_content(Some(value)))
        {
            if !text.is_empty() {
                texts.push(text);
            }
        } else if let Some(text) = item.get("thinking").and_then(Value::as_str) {
            if !text.is_empty() {
                texts.push(text.to_string());
            }
        } else if let Some(text) = item.get("text").and_then(Value::as_str) {
            if !text.is_empty() {
                texts.push(text.to_string());
            }
        }

        if signature.is_none() {
            signature = item.get("signature").cloned();
        }
        if redacted_content.is_none() {
            redacted_content = item.get("redactedContent").cloned();
        }
    }

    if texts.is_empty() && signature.is_none() && redacted_content.is_none() {
        return None;
    }

    let mut reasoning_text = Map::new();
    let merged_text = texts.join("\n");
    if !merged_text.is_empty() {
        reasoning_text.insert("text".to_string(), Value::String(merged_text));
    }
    if let Some(signature) = signature {
        reasoning_text.insert("signature".to_string(), signature);
    }

    let mut reasoning = Map::new();
    if !reasoning_text.is_empty() {
        reasoning.insert("reasoningText".to_string(), Value::Object(reasoning_text));
    }
    if let Some(redacted_content) = redacted_content {
        reasoning.insert("redactedContent".to_string(), redacted_content);
    }

    meaningful_optional_value(Some(Value::Object(reasoning)))
}

fn assistant_metadata_value(message: &GatewayMessage, key: &str) -> Option<Value> {
    message
        .metadata
        .as_ref()
        .and_then(|value| value.get(key).cloned())
        .or_else(|| message.content.get(key).cloned())
}

fn extract_responses_message_metadata(item: &Value, role: &str) -> Option<Value> {
    if role != "assistant" {
        return None;
    }

    let mut metadata = Map::new();
    for key in [
        "reasoningContent",
        "references",
        "supplementaryWebLinks",
        "followupPrompt",
        "cachePoint",
    ] {
        if let Some(value) = meaningful_optional_value(item.get(key).cloned()) {
            metadata.insert(key.to_string(), value);
        }
    }

    if let Some(message_id) = item
        .get("messageId")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        metadata.insert(
            "messageId".to_string(),
            Value::String(message_id.to_string()),
        );
    }

    if !metadata.contains_key("reasoningContent") {
        if let Some(reasoning) = extract_reasoning_content(item.get("content")) {
            metadata.insert("reasoningContent".to_string(), reasoning);
        }
    }

    if metadata.is_empty() {
        None
    } else {
        Some(Value::Object(metadata))
    }
}

fn extract_anthropic_message_metadata(message: &Value, role: &str) -> Option<Value> {
    if role != "assistant" {
        return None;
    }

    let mut metadata = Map::new();
    if let Some(reasoning) = extract_reasoning_content(message.get("content")) {
        metadata.insert("reasoningContent".to_string(), reasoning);
    }

    if metadata.is_empty() {
        None
    } else {
        Some(Value::Object(metadata))
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
            let tool_type = item
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function");
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
                .unwrap_or_else(|| json!({ "type": "object", "properties": {}, "required": [] }));
            Some(GatewayTool {
                tool_type: tool_type.to_string(),
                name: name.to_string(),
                description,
                input_schema,
                web_search_max_uses: item
                    .get("max_uses")
                    .and_then(Value::as_i64)
                    .map(|value| value as i32),
                allowed_domains: item
                    .get("allowed_domains")
                    .and_then(Value::as_array)
                    .map(|values| string_array_from_values(values)),
                blocked_domains: item
                    .get("blocked_domains")
                    .and_then(Value::as_array)
                    .map(|values| string_array_from_values(values)),
                user_location: item.get("user_location").cloned(),
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
            let item_type = item
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function");
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
                .unwrap_or_else(|| json!({ "type": "object", "properties": {}, "required": [] }));
            Some(GatewayTool {
                tool_type: item_type.to_string(),
                name: name.to_string(),
                description,
                input_schema,
                web_search_max_uses: item
                    .get("max_uses")
                    .and_then(Value::as_i64)
                    .map(|value| value as i32),
                allowed_domains: item
                    .get("allowed_domains")
                    .and_then(Value::as_array)
                    .map(|values| string_array_from_values(values)),
                blocked_domains: item
                    .get("blocked_domains")
                    .and_then(Value::as_array)
                    .map(|values| string_array_from_values(values)),
                user_location: item.get("user_location").cloned(),
            })
        })
        .collect()
}

fn string_array_from_values(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn is_web_search_tool_type(tool_type: &str) -> bool {
    tool_type.starts_with("web_search_") || tool_type == "remote_web_search"
}

fn normalize_tool_choice(
    tool_choice: &Option<Value>,
    tools: &[GatewayTool],
) -> Result<Option<Value>, String> {
    let Some(choice) = tool_choice.as_ref() else {
        return Ok(None);
    };

    let choice_type = match choice {
        Value::String(raw) => raw.trim(),
        Value::Object(_) => choice
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| "tool_choice.type 无效".to_string())?,
        _ => return Err("tool_choice 格式无效".to_string()),
    };

    match choice_type {
        "auto" => Ok(Some(json!({ "type": "auto" }))),
        "none" => Ok(Some(json!({ "type": "none" }))),
        "required" => {
            if tools.is_empty() {
                return Err("tool_choice=required 时必须同时提供 tools".to_string());
            }
            Ok(Some(json!({ "type": "required" })))
        }
        "function" => {
            let name = choice
                .get("name")
                .or_else(|| choice.pointer("/function/name"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "tool_choice.function.name 不能为空".to_string())?;

            if !tools.iter().any(|tool| tool.name == name) {
                return Err(format!("tool_choice 指定的工具不存在: {name}"));
            }

            Ok(Some(json!({
                "type": "function",
                "name": name
            })))
        }
        other => Err(format!("暂不支持的 tool_choice.type: {other}")),
    }
}

fn convert_anthropic_content(content: &Value) -> Value {
    match content {
        Value::String(text) => Value::String(text.clone()),
        Value::Array(items) => {
            let has_tool_result = items
                .iter()
                .any(|item| item.get("type").and_then(Value::as_str) == Some("tool_result"));
            if has_tool_result {
                return content.clone();
            }

            let text = extract_text_blocks(content, &["text"]);
            if text.is_empty() {
                content.clone()
            } else {
                Value::String(text)
            }
        }
        other => other.clone(),
    }
}

fn convert_openai_chat_content(content: &Value) -> Value {
    match content {
        Value::String(text) => Value::String(text.clone()),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| {
                    if item.get("type").and_then(Value::as_str) == Some("text") {
                        json!({
                            "type": "input_text",
                            "text": item.get("text").and_then(Value::as_str).unwrap_or_default()
                        })
                    } else {
                        item.clone()
                    }
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn create_tool_results_message(tool_results: &[(String, Value)]) -> GatewayMessage {
    let mut content_array = Vec::new();
    for (tool_call_id, content) in tool_results {
        content_array.push(json!({
            "type": "tool_result",
            "tool_use_id": tool_call_id,
            "content": content
        }));
    }

    GatewayMessage {
        role: "user".to_string(),
        content: Value::Array(content_array),
        tool_calls: Vec::new(),
        tool_call_id: None,
        metadata: None,
    }
}

fn convert_openai_chat_messages(messages: Option<&Value>) -> Vec<GatewayMessage> {
    let Some(Value::Array(items)) = messages else {
        return Vec::new();
    };

    let mut messages = Vec::new();
    let mut pending_tool_results: Vec<(String, Value)> = Vec::new();

    for item in items {
        let Some(role) = item.get("role").and_then(Value::as_str) else {
            continue;
        };

        match role {
            "system" => {
                let text = extract_text_content(item.get("content"));
                if !text.is_empty() {
                    messages.push(GatewayMessage {
                        role: "system".to_string(),
                        content: Value::String(text),
                        tool_calls: Vec::new(),
                        tool_call_id: None,
                        metadata: None,
                    });
                }
            }
            "tool" => {
                let tool_call_id = item
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let content = item
                    .get("content")
                    .cloned()
                    .unwrap_or_else(|| Value::String(String::new()));
                pending_tool_results.push((tool_call_id, content));
            }
            "user" | "assistant" => {
                if !pending_tool_results.is_empty() {
                    messages.push(create_tool_results_message(&pending_tool_results));
                    pending_tool_results.clear();
                }

                let tool_calls = if role == "assistant" {
                    item.get("tool_calls")
                        .and_then(Value::as_array)
                        .map(|calls| {
                            calls
                                .iter()
                                .filter_map(|call| {
                                    Some(GatewayToolCall {
                                        id: call.get("id").and_then(Value::as_str)?.to_string(),
                                        name: call
                                            .get("function")?
                                            .get("name")
                                            .and_then(Value::as_str)?
                                            .to_string(),
                                        arguments: call
                                            .get("function")?
                                            .get("arguments")
                                            .and_then(Value::as_str)
                                            .unwrap_or("{}")
                                            .to_string(),
                                    })
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };

                messages.push(GatewayMessage {
                    role: role.to_string(),
                    content: item
                        .get("content")
                        .map(convert_openai_chat_content)
                        .unwrap_or(Value::Null),
                    tool_calls,
                    tool_call_id: None,
                    metadata: extract_responses_message_metadata(item, role),
                });
            }
            _ => {}
        }
    }

    if !pending_tool_results.is_empty() {
        messages.push(create_tool_results_message(&pending_tool_results));
    }

    messages
}

fn responses_message_content(item: &Value) -> Option<Value> {
    item.get("content")
        .cloned()
        .or_else(|| item.get("text").cloned())
}

fn responses_tool_output_content(output: Option<&Value>) -> Option<Value> {
    match output {
        None => None,
        Some(Value::String(text)) => Some(Value::String(text.clone())),
        Some(other) => Some(Value::String(other.to_string())),
    }
}

fn flush_pending_responses_user_items(
    messages: &mut Vec<GatewayMessage>,
    pending_user_items: &mut Vec<Value>,
) {
    if pending_user_items.is_empty() {
        return;
    }

    messages.push(GatewayMessage {
        role: "user".to_string(),
        content: Value::Array(std::mem::take(pending_user_items)),
        tool_calls: Vec::new(),
        tool_call_id: None,
        metadata: None,
    });
}

fn convert_responses_input_items(items: &[Value]) -> Vec<GatewayMessage> {
    let mut messages = Vec::new();
    let mut pending_user_items = Vec::new();

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();

        if let Some(role) = item.get("role").and_then(Value::as_str) {
            flush_pending_responses_user_items(&mut messages, &mut pending_user_items);
            messages.push(GatewayMessage {
                role: role.to_string(),
                content: responses_message_content(item).unwrap_or(Value::Null),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: extract_responses_message_metadata(item, role),
            });
            continue;
        }

        match item_type {
            "message" => {
                flush_pending_responses_user_items(&mut messages, &mut pending_user_items);
                let role = item
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("user")
                    .to_string();
                messages.push(GatewayMessage {
                    role: role.clone(),
                    content: responses_message_content(item).unwrap_or(Value::Null),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    metadata: extract_responses_message_metadata(item, &role),
                });
            }
            "function_call" => {
                flush_pending_responses_user_items(&mut messages, &mut pending_user_items);
                messages.push(GatewayMessage {
                    role: "assistant".to_string(),
                    content: Value::Null,
                    tool_calls: vec![GatewayToolCall {
                        id: item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        name: item
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        arguments: item
                            .get("arguments")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| {
                                serde_json::to_string(
                                    &item.get("arguments").cloned().unwrap_or_else(|| json!({})),
                                )
                                .unwrap_or_else(|_| "{}".to_string())
                            }),
                    }],
                    tool_call_id: None,
                    metadata: None,
                });
            }
            "function_call_output" => {
                flush_pending_responses_user_items(&mut messages, &mut pending_user_items);
                messages.push(GatewayMessage {
                    role: "tool".to_string(),
                    content: responses_tool_output_content(item.get("output"))
                        .unwrap_or(Value::Null),
                    tool_calls: Vec::new(),
                    tool_call_id: item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    metadata: None,
                });
            }
            "input_text" | "output_text" | "input_image" | "image_url" | "image" => {
                pending_user_items.push(item.clone());
            }
            _ => {}
        }
    }

    flush_pending_responses_user_items(&mut messages, &mut pending_user_items);
    messages
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
        let system_text = extract_text_blocks(system, &["text"]);
        if !system_text.trim().is_empty() {
            messages.push(GatewayMessage {
                role: "system".to_string(),
                content: Value::String(system_text),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
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
        let content = message
            .get("content")
            .map(convert_anthropic_content)
            .unwrap_or(Value::Null);
        let tool_calls = extract_anthropic_tool_calls(&content);
        let tool_call_id = extract_anthropic_tool_result_id(&content);
        messages.push(GatewayMessage {
            role: role.clone(),
            content,
            tool_calls,
            tool_call_id,
            metadata: extract_anthropic_message_metadata(message, &role),
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
        tool_choice: value.get("tool_choice").cloned(),
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
    let messages = convert_openai_chat_messages(value.get("messages"));

    if messages.is_empty() {
        return Err("messages 中没有可发送给 Kiro 的用户消息".to_string());
    }

    Ok(GatewayProxyRequest {
        model,
        stream,
        messages,
        tools: convert_openai_tools(value.get("tools")),
        tool_choice: value.get("tool_choice").cloned(),
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
        let text = extract_text_blocks(instructions, &["text", "input_text", "output_text"]);
        if !text.trim().is_empty() {
            messages.push(GatewayMessage {
                role: "system".to_string(),
                content: Value::String(text),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            });
        }
    }

    match value.get("input") {
        Some(Value::String(text)) => messages.push(GatewayMessage {
            role: "user".to_string(),
            content: Value::String(text.clone()),
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: None,
        }),
        Some(Value::Array(items)) => messages.extend(convert_responses_input_items(items)),
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
        tool_choice: value.get("tool_choice").cloned(),
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

fn join_with_newline(left: &str, right: &str) -> String {
    match (left.trim().is_empty(), right.trim().is_empty()) {
        (true, true) => String::new(),
        (true, false) => right.to_string(),
        (false, true) => left.to_string(),
        (false, false) => format!("{}\n{}", left, right),
    }
}

fn process_tools_with_long_descriptions(
    tools: &[GatewayTool],
) -> (Vec<GatewayTool>, Option<String>) {
    if tools.is_empty() {
        return (Vec::new(), None);
    }

    let mut processed = Vec::with_capacity(tools.len());
    let mut long_docs = Vec::new();

    for tool in tools {
        if tool.description.len() > TOOL_DESCRIPTION_MAX_LENGTH {
            long_docs.push(format!("## Tool: {}\n\n{}", tool.name, tool.description));
            let mut downgraded = tool.clone();
            downgraded.description = format!(
                "[Full documentation in system prompt under '## Tool: {}']",
                tool.name
            );
            processed.push(downgraded);
        } else {
            processed.push(tool.clone());
        }
    }

    let docs = if long_docs.is_empty() {
        None
    } else {
        Some(format!(
            "# Tool Documentation\n\n{}",
            long_docs.join("\n\n")
        ))
    };

    (processed, docs)
}

fn merge_adjacent_gateway_messages(messages: &[GatewayMessage]) -> Vec<GatewayMessage> {
    let mut merged: Vec<GatewayMessage> = Vec::new();
    for message in messages {
        if let Some(last) = merged.last_mut() {
            if last.role == message.role {
                let existing = extract_text_content(Some(&last.content));
                let incoming = extract_text_content(Some(&message.content));
                last.content = Value::String(join_with_newline(&existing, &incoming));
                last.tool_calls.extend(message.tool_calls.clone());
                if last.tool_call_id.is_none() {
                    last.tool_call_id = message.tool_call_id.clone();
                }
                if last.metadata.is_none() {
                    last.metadata = message.metadata.clone();
                }
                continue;
            }
        }
        merged.push(message.clone());
    }
    merged
}

fn normalized_schema_type(value: Option<&Value>, has_properties: bool, has_items: bool) -> String {
    match value {
        Some(Value::String(raw)) if !raw.trim().is_empty() => raw.trim().to_string(),
        Some(Value::Array(items)) => {
            let normalized = items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .find(|kind| !kind.is_empty() && *kind != "null");
            if let Some(kind) = normalized {
                kind.to_string()
            } else if has_properties {
                "object".to_string()
            } else if has_items {
                "array".to_string()
            } else {
                "object".to_string()
            }
        }
        _ if has_properties => "object".to_string(),
        _ if has_items => "array".to_string(),
        _ => "object".to_string(),
    }
}

fn sanitize_json_schema(value: Value, is_root: bool) -> Value {
    let mut schema = match value {
        Value::Object(map) => map,
        _ => Map::new(),
    };

    let properties = match schema.remove("properties") {
        Some(Value::Object(map)) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, sanitize_json_schema(value, false)))
                .collect(),
        ),
        _ => Value::Object(Map::new()),
    };
    let items = schema
        .remove("items")
        .map(|value| sanitize_json_schema(value, false));
    let required = match schema.remove("required") {
        Some(Value::Array(items)) => Value::Array(
            items
                .into_iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .map(Value::String)
                .collect(),
        ),
        _ => Value::Array(Vec::new()),
    };

    let schema_type = normalized_schema_type(
        schema.get("type"),
        matches!(&properties, Value::Object(map) if !map.is_empty()),
        items.is_some(),
    );

    let mut normalized = Map::new();
    normalized.insert("type".to_string(), Value::String(schema_type.clone()));
    if let Some(description) = schema.get("description").cloned() {
        normalized.insert("description".to_string(), description);
    }
    if let Some(enum_values) = schema.get("enum").cloned() {
        normalized.insert("enum".to_string(), enum_values);
    }
    if let Some(format) = schema.get("format").cloned() {
        normalized.insert("format".to_string(), format);
    }
    if let Some(default) = schema.get("default").cloned() {
        normalized.insert("default".to_string(), default);
    }
    if let Some(examples) = schema.get("examples").cloned() {
        normalized.insert("examples".to_string(), examples);
    }
    if let Some(title) = schema.get("title").cloned() {
        normalized.insert("title".to_string(), title);
    }

    for key in [
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
    ] {
        if let Some(value) = schema.get(key).cloned() {
            normalized.insert(key.to_string(), value);
        }
    }

    for key in ["minLength", "maxLength", "pattern"] {
        if let Some(value) = schema.get(key).cloned() {
            normalized.insert(key.to_string(), value);
        }
    }

    for key in ["minItems", "maxItems", "uniqueItems"] {
        if let Some(value) = schema.get(key).cloned() {
            normalized.insert(key.to_string(), value);
        }
    }

    if matches!(schema_type.as_str(), "object") || is_root {
        normalized.insert("properties".to_string(), properties);
        normalized.insert("required".to_string(), required);
    }

    if schema_type == "array" {
        normalized.insert(
            "items".to_string(),
            items.unwrap_or_else(|| json!({ "type": "object", "properties": {}, "required": [] })),
        );
    }

    if let Some(Value::Array(items)) = schema.remove("oneOf") {
        let variants: Vec<Value> = items
            .into_iter()
            .map(|item| sanitize_json_schema(item, false))
            .collect();
        if !variants.is_empty() {
            normalized.insert("oneOf".to_string(), Value::Array(variants));
        }
    }
    if let Some(Value::Array(items)) = schema.remove("anyOf") {
        let variants: Vec<Value> = items
            .into_iter()
            .map(|item| sanitize_json_schema(item, false))
            .collect();
        if !variants.is_empty() {
            normalized.insert("anyOf".to_string(), Value::Array(variants));
        }
    }
    if let Some(Value::Array(items)) = schema.remove("allOf") {
        let variants: Vec<Value> = items
            .into_iter()
            .map(|item| sanitize_json_schema(item, false))
            .collect();
        if !variants.is_empty() {
            normalized.insert("allOf".to_string(), Value::Array(variants));
        }
    }

    Value::Object(normalized)
}

fn normalize_json_schema(value: Value) -> Value {
    sanitize_json_schema(value, true)
}

fn tool_description(tool: &GatewayTool) -> String {
    if is_web_search_tool_type(&tool.tool_type) {
        let mut parts = vec![if tool.description.is_empty() {
            WEB_SEARCH_TOOL_DESCRIPTION.to_string()
        } else {
            tool.description.clone()
        }];
        if let Some(domains) = tool
            .allowed_domains
            .as_ref()
            .filter(|items| !items.is_empty())
        {
            parts.push(format!("Only return results from: {}", domains.join(", ")));
        }
        if let Some(domains) = tool
            .blocked_domains
            .as_ref()
            .filter(|items| !items.is_empty())
        {
            parts.push(format!("Exclude results from: {}", domains.join(", ")));
        }
        return parts.join(" ");
    }

    tool.description.clone()
}

fn tool_input_schema(tool: &GatewayTool) -> Value {
    if is_web_search_tool_type(&tool.tool_type) {
        return json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query"
                }
            },
            "required": ["query"]
        });
    }

    normalize_json_schema(tool.input_schema.clone())
}

fn gateway_tools_to_kiro(tools: &[GatewayTool]) -> Option<Vec<KiroTool>> {
    if tools.is_empty() {
        return None;
    }
    Some(
        tools
            .iter()
            .map(|tool| KiroTool::ToolSpecification {
                tool_specification: KiroToolSpec {
                    name: tool.name.clone(),
                    description: tool_description(tool),
                    input_schema: KiroInputSchema {
                        json: tool_input_schema(tool),
                    },
                },
            })
            .collect(),
    )
}

fn tool_result_content_to_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item.get("type").and_then(Value::as_str) {
                Some("web_search_result") => item.to_string(),
                _ => value_to_plain_text(item),
            })
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Some(value) => value_to_plain_text(value),
        None => String::new(),
    }
}

fn extract_kiro_tool_results(message: &GatewayMessage) -> Vec<KiroToolResult> {
    let mut results = Vec::new();

    if message.role == "tool" {
        if let Some(tool_use_id) = message.tool_call_id.as_deref() {
            results.push(KiroToolResult {
                content: vec![KiroToolResultContent::Text {
                    text: value_to_plain_text(&message.content),
                }],
                status: "success".to_string(),
                tool_use_id: tool_use_id.to_string(),
            });
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
        results.push(KiroToolResult {
            content: vec![KiroToolResultContent::Text {
                text: tool_result_content_to_text(item.get("content")),
            }],
            status: status.to_string(),
            tool_use_id: tool_use_id.to_string(),
        });
    }
    results
}

fn extract_kiro_tool_results_from_tool_message(message: &GatewayMessage) -> Vec<KiroToolResult> {
    vec![KiroToolResult {
        content: vec![KiroToolResultContent::Text {
            text: extract_text_content(Some(&message.content)),
        }],
        status: "success".to_string(),
        tool_use_id: message.tool_call_id.clone().unwrap_or_default(),
    }]
}

fn extract_tool_uses(message: &GatewayMessage) -> Option<Vec<KiroToolUse>> {
    if message.tool_calls.is_empty() {
        return None;
    }
    Some(
        message
            .tool_calls
            .iter()
            .map(|call| KiroToolUse {
                name: call.name.clone(),
                input: parse_tool_arguments(&call.arguments),
                tool_use_id: call.id.clone(),
            })
            .collect(),
    )
}

fn build_history_assistant_message(message: &GatewayMessage) -> HistoryAssistantMessage {
    // P0 fix: Kiro 后端不接受 history 中的 toolUses 字段（会返回 400）
    // 将 tool_uses 转为纯文本追加到 content 中
    let base_content = extract_text_content(Some(&message.content));
    let tool_uses = extract_tool_uses(message);
    let content = if let Some(ref uses) = tool_uses {
        if uses.is_empty() {
            base_content
        } else {
            let tool_text = uses
                .iter()
                .map(|tu| {
                    format!(
                        "[Tool Call: {}({})]",
                        tu.name,
                        serde_json::to_string(&tu.input).unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            if base_content.trim().is_empty() {
                tool_text
            } else {
                format!("{}\n{}", base_content, tool_text)
            }
        }
    } else {
        base_content
    };

    HistoryAssistantMessage {
        content,
        tool_uses: None, // 不发送 toolUses，Kiro 后端拒绝
        reasoning_content: None, // P0 fix: Kiro 后端拒绝 history 中的 reasoningContent
        references: assistant_metadata_value(message, "references")
            .and_then(|value| meaningful_optional_value(Some(value))),
        supplementary_web_links: assistant_metadata_value(message, "supplementaryWebLinks")
            .and_then(|value| meaningful_optional_value(Some(value))),
        followup_prompt: assistant_metadata_value(message, "followupPrompt")
            .and_then(|value| meaningful_optional_value(Some(value))),
        message_id: assistant_metadata_value(message, "messageId")
            .and_then(|value| value.as_str().map(str::to_string))
            .filter(|value| !value.trim().is_empty()),
        cache_point: assistant_metadata_value(message, "cachePoint")
            .and_then(|value| meaningful_optional_value(Some(value))),
    }
}

fn images_option(images: Vec<ImageBlock>) -> Option<Vec<ImageBlock>> {
    if images.is_empty() {
        return None;
    }
    Some(images)
}

async fn extract_images(content: Option<&Value>) -> Vec<ImageBlock> {
    let Some(Value::Array(items)) = content else {
        return Vec::new();
    };

    let mut images = Vec::new();
    for item in items {
        if let Some(image) = extract_image_block(item).await {
            images.push(image);
        }
    }
    images
}

async fn extract_image_block(item: &Value) -> Option<ImageBlock> {
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
    match item_type {
        "image" => {
            let source = item.get("source")?;
            let bytes = source.get("data").and_then(Value::as_str)?.to_string();
            if encoded_image_exceeds_limit(&bytes) {
                return None;
            }
            let media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("image/png");
            Some(ImageBlock {
                format: media_type_to_format(media_type)?,
                source: ImageSource::Bytes { bytes },
            })
        }
        "image_url" => {
            let url = item
                .get("image_url")
                .and_then(|value| value.get("url").or(Some(value)))
                .and_then(Value::as_str)?;
            let (format, bytes) = resolve_image_source(url).await?;
            Some(ImageBlock {
                format,
                source: ImageSource::Bytes { bytes },
            })
        }
        "input_image" => {
            let url = item
                .get("image_url")
                .and_then(Value::as_str)
                .or_else(|| item.get("url").and_then(Value::as_str))?;
            let (format, bytes) = resolve_image_source(url).await?;
            Some(ImageBlock {
                format,
                source: ImageSource::Bytes { bytes },
            })
        }
        _ => None,
    }
}

fn media_type_to_format(media_type: &str) -> Option<String> {
    match media_type.trim().to_ascii_lowercase().as_str() {
        "image/png" | "png" => Some("png".to_string()),
        "image/jpeg" | "image/jpg" | "jpeg" | "jpg" => Some("jpeg".to_string()),
        "image/gif" | "gif" => Some("gif".to_string()),
        "image/webp" | "webp" => Some("webp".to_string()),
        _ => None,
    }
}

fn parse_data_url(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, bytes) = rest.split_once(',')?;
    let media_type = meta.split(';').next().unwrap_or_default();
    if encoded_image_exceeds_limit(bytes) {
        return None;
    }
    Some((media_type_to_format(media_type)?, bytes.to_string()))
}

async fn resolve_image_source(url: &str) -> Option<(String, String)> {
    if let Some(parsed) = parse_data_url(url) {
        return Some(parsed);
    }

    let image_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(IMAGE_FETCH_TIMEOUT_SECONDS))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .ok()?;
    let mut current_url = validate_remote_image_url(url).await?;

    for _ in 0..=MAX_IMAGE_REDIRECTS {
        let response = image_client.get(current_url.clone()).send().await.ok()?;
        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)?
                .to_str()
                .ok()?;
            let next_url = current_url.join(location).ok()?;
            current_url = validate_remote_image_url(next_url.as_str()).await?;
            continue;
        }
        if !response.status().is_success() {
            return None;
        }
        if response
            .content_length()
            .map(|length| length > MAX_IMAGE_SOURCE_BYTES as u64)
            .unwrap_or(false)
        {
            return None;
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let final_url = response.url().clone();
        let bytes = response.bytes().await.ok()?;
        if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
            return None;
        }
        let format = content_type
            .as_deref()
            .and_then(|value| value.split(';').next())
            .and_then(media_type_to_format)
            .or_else(|| infer_image_format_from_url(final_url.as_str()))?;

        return Some((format, STANDARD.encode(bytes)));
    }

    None
}

fn infer_image_format_from_url(url: &str) -> Option<String> {
    let path = reqwest::Url::parse(url).ok()?.path().to_ascii_lowercase();
    if path.ends_with(".png") {
        Some("png".to_string())
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        Some("jpeg".to_string())
    } else if path.ends_with(".gif") {
        Some("gif".to_string())
    } else if path.ends_with(".webp") {
        Some("webp".to_string())
    } else {
        None
    }
}

async fn validate_remote_image_url(url: &str) -> Option<reqwest::Url> {
    let parsed = reqwest::Url::parse(url).ok()?;
    match parsed.scheme() {
        "http" | "https" => {}
        _ => return None,
    }

    let host = parsed.host_str()?;
    if host.eq_ignore_ascii_case("localhost") {
        return None;
    }

    let port = parsed.port_or_known_default()?;
    let mut resolved_any = false;
    for address in lookup_host((host, port)).await.ok()? {
        resolved_any = true;
        if is_restricted_remote_ip(address.ip()) {
            return None;
        }
    }

    if !resolved_any {
        return None;
    }

    Some(parsed)
}

fn encoded_image_exceeds_limit(encoded: &str) -> bool {
    encoded.len() > max_base64_len_for_bytes(MAX_IMAGE_SOURCE_BYTES)
}

fn max_base64_len_for_bytes(max_bytes: usize) -> usize {
    max_bytes.div_ceil(3) * 4
}

fn is_restricted_remote_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(addr) => {
            addr.is_private()
                || addr.is_loopback()
                || addr.is_link_local()
                || addr.is_broadcast()
                || addr.is_documentation()
                || addr.is_unspecified()
                || addr.is_multicast()
                || is_ipv4_shared(addr)
                || is_ipv4_reserved(addr)
        }
        IpAddr::V6(addr) => {
            addr.is_loopback()
                || addr.is_unspecified()
                || addr.is_multicast()
                || addr.is_unique_local()
                || addr.is_unicast_link_local()
                || is_ipv6_documentation(addr)
        }
    }
}

fn is_ipv4_shared(addr: Ipv4Addr) -> bool {
    let octets = addr.octets();
    octets[0] == 100 && (64..=127).contains(&octets[1])
}

fn is_ipv4_reserved(addr: Ipv4Addr) -> bool {
    let octets = addr.octets();
    octets[0] >= 240
}

fn is_ipv6_documentation(addr: Ipv6Addr) -> bool {
    let segments = addr.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

fn build_kiro_user_context(
    tools: Option<Vec<KiroTool>>,
    tool_results: Vec<KiroToolResult>,
    tool_choice: Option<Value>,
    additional_context: Option<Value>,
) -> Option<UserInputMessageContext> {
    if tools.is_none()
        && tool_results.is_empty()
        && tool_choice.is_none()
        && additional_context.is_none()
    {
        return None;
    }
    Some(UserInputMessageContext {
        additional_context,
        app_studio_context: None,
        console_state: None,
        diagnostic: None,
        editor_state: None,
        env_state: None,
        git_state: None,
        shell_state: None,
        tool_results: if tool_results.is_empty() {
            None
        } else {
            Some(tool_results)
        },
        tools,
        tool_choice,
        user_settings: None,
    })
}

async fn build_kiro_payload(
    request: &GatewayProxyRequest,
    profile_arn: Option<String>,
    available_models: &[String],
) -> Result<Value, String> {
    let model_id = get_internal_model_id_with_fallback(&request.model, available_models)?;
    let normalized_tool_choice = normalize_tool_choice(&request.tool_choice, &request.tools)?;
    // P2 fix: conversationId 会话稳定化
    // 优先使用 previous_response_id；否则基于首条 user message 内容生成稳定 hash
    let conversation_id = request
        .previous_response_id
        .clone()
        .unwrap_or_else(|| {
            let first_user_content = request
                .messages
                .iter()
                .find(|m| m.role == "user")
                .map(|m| extract_text_content(Some(&m.content)))
                .unwrap_or_default();
            if first_user_content.is_empty() {
                uuid::Uuid::new_v4().to_string()
            } else {
                // 使用内容前 256 字节的简单 hash 生成确定性 UUID 格式 ID
                // 必须按字符边界截取，避免中文等多字节字符导致 panic
                let byte_limit = first_user_content
                    .char_indices()
                    .take_while(|(i, _)| *i < 256)
                    .last()
                    .map(|(i, c)| i + c.len_utf8())
                    .unwrap_or(0);
                let seed = &first_user_content[..byte_limit];
                let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a
                for byte in seed.as_bytes() {
                    hash ^= *byte as u64;
                    hash = hash.wrapping_mul(0x100000001b3);
                }
                format!("{:016x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
                    hash,
                    (hash >> 16) as u16,
                    (hash >> 32) as u16 & 0x0fff,
                    (hash >> 48) as u16 & 0x0fff,
                    hash.wrapping_mul(0x517cc1b727220a95)
                )
            }
        });
    let agent_continuation_id = conversation_id.clone();
    let _ = agent_continuation_id; // 不再使用，Kiro 后端不接受此字段
    let (processed_tools, tool_docs) = process_tools_with_long_descriptions(&request.tools);
    let tool_docs_for_current = tool_docs.clone();
    let mut system_prompt = String::new();
    let mut non_system_messages = Vec::new();

    for message in &request.messages {
        if message.role == "system" {
            let text = extract_text_content(Some(&message.content));
            if !text.trim().is_empty() {
                system_prompt = join_with_double_newline(&system_prompt, &text);
            }
        } else {
            non_system_messages.push(message.clone());
        }
    }

    if let Some(tool_docs) = tool_docs {
        system_prompt = join_with_double_newline(&system_prompt, &tool_docs);
    }

    if non_system_messages.is_empty() {
        return Err("没有可发送给 Kiro 的消息".to_string());
    }

    // system prompt 直接拼接到 currentMessage content 中（Kiro 官方 IDE 做法）
    let merged_messages = merge_adjacent_gateway_messages(&non_system_messages);
    // P1 fix: 收集当前请求中定义的 tool 名称集合
    let current_tool_names: HashSet<&str> = processed_tools
        .iter()
        .map(|t| t.name.as_str())
        .collect();
    let history = if merged_messages.len() > 1 {
        let mut items = Vec::new();
        let history_len = merged_messages.len() - 1;
        for (index, message) in merged_messages[..merged_messages.len() - 1]
            .iter()
            .enumerate()
        {
            match message.role.as_str() {
                "assistant" => {
                    let mut assistant_message = build_history_assistant_message(message);
                    if history_len > 10 && index == history_len - 10 {
                        assistant_message.cache_point = Some(json!({ "type": "default" }));
                    }
                    // P1 fix: 如果 tool_uses 中的工具不在当前 tool 定义中，转为纯文本
                    if let Some(ref tool_uses) = assistant_message.tool_uses {
                        let has_orphan = tool_uses
                            .iter()
                            .any(|tu| !current_tool_names.contains(tu.name.as_str()));
                        if has_orphan {
                            let tool_text = tool_uses
                                .iter()
                                .map(|tu| {
                                    format!(
                                        "[Tool Call: {}({})]",
                                        tu.name,
                                        serde_json::to_string(&tu.input).unwrap_or_default()
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("\n");
                            assistant_message.content =
                                join_with_newline(&assistant_message.content, &tool_text);
                            assistant_message.tool_uses = None;
                        }
                    }
                    items.push(HistoryItem::Assistant {
                        assistant_response_message: assistant_message,
                    });
                }
                "user" | "tool" => {
                    let content = if message.role == "tool" {
                        String::new()
                    } else {
                        extract_text_content(Some(&message.content))
                    };
                    let images = if message.role == "tool" {
                        None
                    } else {
                        images_option(extract_images(Some(&message.content)).await)
                    };
                    let tool_results = if message.role == "tool" {
                        extract_kiro_tool_results_from_tool_message(message)
                    } else {
                        extract_kiro_tool_results(message)
                    };
                    // P0 fix: Kiro 后端不接受 history 中的 toolResults（会返回 400）
                    // 永远将 tool_results 转为纯文本追加到 content 中
                    let (final_content, final_tool_results) =
                        if !tool_results.is_empty() {
                            let result_text = tool_results
                                .iter()
                                .map(|tr| {
                                    let text = tr
                                        .content
                                        .iter()
                                        .filter_map(|c| match c {
                                            KiroToolResultContent::Text { text } => {
                                                Some(text.clone())
                                            }
                                            KiroToolResultContent::Json { json } => {
                                                Some(json.to_string())
                                            }
                                            KiroToolResultContent::Other { data } => {
                                                Some(data.to_string())
                                            }
                                        })
                                        .collect::<Vec<_>>()
                                        .join("\n");
                                    format!("[Tool Result: {}]", text)
                                })
                                .collect::<Vec<_>>()
                                .join("\n");
                            (join_with_newline(&content, &result_text), Vec::new())
                        } else {
                            (content, tool_results)
                        };
                    items.push(HistoryItem::User {
                        user_input_message: HistoryUserMessage {
                            content: final_content,
                            model_id: model_id.clone(),
                            origin: "AI_EDITOR".to_string(),
                            images,
                            user_input_message_context: build_kiro_user_context(
                                None,
                                final_tool_results,
                                None,
                                None,
                            ),
                        },
                    });
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
    let mut current_content = extract_text_content(Some(&current_message.content));
    // system prompt 拼接到 currentMessage content 前面
    if !system_prompt.trim().is_empty() {
        current_content = join_with_double_newline(&system_prompt, &current_content);
    }
    if let Some(tool_docs) = tool_docs_for_current {
        current_content = join_with_double_newline(&tool_docs, &current_content);
    }
    if current_message.role == "assistant" || current_content.trim().is_empty() {
        current_content = if current_content.trim().is_empty() {
            "Continue".to_string()
        } else {
            current_content
        };
    }
    let current_images = images_option(extract_images(Some(&current_message.content)).await);
    let current_tool_results = if current_message.role == "tool" {
        extract_kiro_tool_results_from_tool_message(current_message)
    } else {
        extract_kiro_tool_results(current_message)
    };
    let payload = KiroPayload {
        conversation_state: ConversationState {
            chat_trigger_type: "MANUAL".to_string(),
            conversation_id,
            agent_continuation_id: None,
            agent_task_type: None,
            current_message: CurrentMessage {
                user_input_message: UserInputMessage {
                    content: current_content,
                    model_id,
                    origin: "AI_EDITOR".to_string(),
                    cache_point: None,
                    client_cache_config: None,
                    documents: None,
                    images: current_images,
                    user_input_message_context: build_kiro_user_context(
                        gateway_tools_to_kiro(&processed_tools),
                        current_tool_results,
                        normalized_tool_choice,
                        None,
                    ),
                    user_intent: None,
                },
            },
            history,
            customization_arn: None,
            workspace_id: None,
        },
        profile_arn,
    };
    serde_json::to_value(payload).map_err(|e| format!("序列化 Kiro payload 失败: {e}"))
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
    // P2 fix: 基于 token 估算的 history trimming
    // 先用 token 估算判断是否需要裁剪，再用字节大小作为兜底
    const TOKEN_BUDGET: usize = 180_000; // 200K 模型留 20K buffer
    const CHARS_PER_TOKEN: usize = 3; // 混合中英文平均 ~3 chars/token

    let payload_str = serde_json::to_string(payload).unwrap_or_default();
    let estimated_tokens = payload_str.chars().count() / CHARS_PER_TOKEN;

    if estimated_tokens <= TOKEN_BUDGET && payload_str.len() <= MAX_KIRO_PAYLOAD_SIZE {
        return;
    }

    loop {
        let current_str = serde_json::to_string(payload).unwrap_or_default();
        let current_tokens = current_str.chars().count() / CHARS_PER_TOKEN;
        if current_tokens <= TOKEN_BUDGET && current_str.len() <= MAX_KIRO_PAYLOAD_SIZE {
            break;
        }

        let Some(history) = payload
            .pointer_mut("/conversationState/history")
            .and_then(Value::as_array_mut)
        else {
            break;
        };

        if history.len() <= 2 {
            break;
        }

        let first_is_assistant_with_tools = history
            .first()
            .and_then(|msg| msg.get("assistantResponseMessage"))
            .and_then(|msg| msg.get("toolUses"))
            .and_then(Value::as_array)
            .map(|arr| !arr.is_empty())
            .unwrap_or(false);

        if first_is_assistant_with_tools && history.len() > 1 {
            let second_has_tool_results = history
                .get(1)
                .and_then(|msg| msg.get("userInputMessage"))
                .and_then(|msg| msg.get("userInputMessageContext"))
                .and_then(|ctx| ctx.get("toolResults"))
                .and_then(Value::as_array)
                .map(|arr| !arr.is_empty())
                .unwrap_or(false);

            if second_has_tool_results {
                if history.len() > 3 {
                    history.remove(0);
                    history.remove(0);
                    continue;
                }
                break;
            }
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
    const MAX_RETRIES: u32 = 3;
    let mut attempt = 0;

    loop {
        attempt += 1;
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
        if (200..300).contains(&status) {
            return response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| DirectProxyError {
                    status: 502,
                    message: format!("读取上游响应失败: {e}"),
                });
        }

        let body = response.text().await.unwrap_or_default();
        if status == 429 {
            return Err(map_direct_upstream_error(status, &body));
        }

        let should_retry = attempt < MAX_RETRIES && (status == 403 || status >= 500);
        if should_retry {
            let backoff_ms = 1000 * 2u64.pow(attempt - 1);
            logger::log_warn(&format!(
                "[KiroLocalAccess] 上游请求失败(status={})，{}ms 后重试({}/{})",
                status, backoff_ms, attempt, MAX_RETRIES
            ));
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            continue;
        }

        return Err(map_direct_upstream_error(status, &body));
    }
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

    if let Some(percentage) = value.get("contextUsagePercentage").and_then(Value::as_f64) {
        return Some(KiroEvent::ContextUsage {
            percentage: percentage as f32,
        });
    }

    if let Some(metering) = value.get("meteringEvent").and_then(Value::as_object) {
        let unit = metering
            .get("unit")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let unit_plural = metering
            .get("unitPlural")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let usage = metering.get("usage").and_then(Value::as_f64).unwrap_or(0.0);
        if !unit.is_empty() {
            return Some(KiroEvent::Metering {
                unit,
                unit_plural,
                usage,
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

    if let Some(citation) = parse_citation_event(&value) {
        return Some(KiroEvent::Citation {
            text: citation.text,
            link: citation.link,
            target: citation.target,
        });
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
                KiroEvent::ContextUsage { percentage } => {
                    aggregated.context_usage_percentage = Some(percentage);
                }
                KiroEvent::Metering { usage, .. } => {
                    aggregated.metering_usage = Some(usage);
                }
                KiroEvent::Citation { text, link, target } => {
                    aggregated.citations.push(
                        crate::modules::kiro_gateway::models::AggregatedCitation {
                            text,
                            link,
                            target,
                        },
                    );
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

fn parse_citation_event(
    value: &Value,
) -> Option<crate::modules::kiro_gateway::models::AggregatedCitation> {
    let target = value.get("target")?.clone();
    let link = value
        .get("citationLink")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|item| !item.is_empty())?
        .to_string();
    let text = value
        .get("citationText")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string);
    ensure_citation_target_supported(&target)?;

    Some(crate::modules::kiro_gateway::models::AggregatedCitation { text, link, target })
}

fn ensure_citation_target_supported(target: &Value) -> Option<()> {
    if let Some(range) = target.get("range") {
        let start_index = range.get("start").and_then(Value::as_u64)? as usize;
        let end_index = range.get("end").and_then(Value::as_u64)? as usize;
        if end_index < start_index {
            return None;
        }
        return Some(());
    }

    if target.get("location").and_then(Value::as_u64).is_some() {
        return Some(());
    }

    None
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

fn anthropic_content_blocks(
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
) -> Vec<Value> {
    let mut content = Vec::new();
    if !aggregated.thinking.is_empty() {
        content.push(json!({
            "type": "thinking",
            "thinking": aggregated.thinking
        }));
    }
    for call in server_tool_calls {
        content.push(json!({
            "type": "server_tool_use",
            "id": call.id,
            "name": call.name,
            "input": call.input
        }));
        content.push(json!({
            "type": "web_search_tool_result",
            "tool_use_id": call.id,
            "content": call.result_content
        }));
    }
    if !aggregated.text.is_empty() {
        let mut text_block = json!({
            "type": "text",
            "text": aggregated.text
        });
        if let Some(citations) =
            build_anthropic_text_citations(&aggregated.citations, &aggregated.text)
        {
            text_block["citations"] = citations;
        }
        content.push(text_block);
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

fn infer_citation_text(
    citation: &crate::modules::kiro_gateway::models::AggregatedCitation,
    message_text: &str,
) -> String {
    if let Some(text) = citation
        .text
        .as_ref()
        .filter(|text| !text.trim().is_empty())
    {
        return text.clone();
    }

    if let Some(range) = citation.target.get("range") {
        let start = range.get("start").and_then(Value::as_u64).unwrap_or(0) as usize;
        let end = range
            .get("end")
            .and_then(Value::as_u64)
            .unwrap_or(start as u64) as usize;
        let chars: Vec<char> = message_text.chars().collect();
        if start < chars.len() && end <= chars.len() && start < end {
            return chars[start..end].iter().collect();
        }
    }

    String::new()
}

fn extract_anthropic_citation_bounds(
    citation: &crate::modules::kiro_gateway::models::AggregatedCitation,
    message_text: &str,
) -> Option<(usize, usize)> {
    let range = citation.target.get("range")?;
    let start = range.get("start").and_then(Value::as_u64)? as usize;
    let end = range.get("end").and_then(Value::as_u64)? as usize;
    if start > end || end > message_text.chars().count() {
        return None;
    }
    Some((start, end))
}

fn build_anthropic_text_citation(
    citation: &crate::modules::kiro_gateway::models::AggregatedCitation,
    message_text: &str,
) -> Option<Value> {
    let (start_char_index, end_char_index) =
        extract_anthropic_citation_bounds(citation, message_text)?;
    let cited_text = infer_citation_text(citation, message_text);

    Some(json!({
        "type": "char_location",
        "cited_text": cited_text,
        "document_index": 0,
        "document_title": citation.link,
        "start_char_index": start_char_index,
        "end_char_index": end_char_index,
        "file_id": Value::Null
    }))
}

fn build_anthropic_text_citations(
    citations: &[crate::modules::kiro_gateway::models::AggregatedCitation],
    message_text: &str,
) -> Option<Value> {
    let mapped: Vec<Value> = citations
        .iter()
        .filter_map(|citation| build_anthropic_text_citation(citation, message_text))
        .collect();

    if mapped.is_empty() {
        None
    } else {
        Some(Value::Array(mapped))
    }
}

fn build_anthropic_citation_delta_event(
    index: usize,
    citation: &crate::modules::kiro_gateway::models::AggregatedCitation,
    message_text: &str,
) -> Option<Value> {
    Some(json!({
        "type": "content_block_delta",
        "index": index,
        "delta": {
            "type": "citations_delta",
            "citation": build_anthropic_text_citation(citation, message_text)?
        }
    }))
}

fn build_direct_anthropic_response(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
    stream: bool,
    response_id: Option<String>,
) -> ProxyResult {
    let message_id =
        response_id.unwrap_or_else(|| format!("msg_{}", uuid::Uuid::new_v4().simple()));
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
            "content": anthropic_content_blocks(aggregated, server_tool_calls),
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

    for call in server_tool_calls {
        push_sse_event(
            &mut body,
            Some("content_block_start"),
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {
                    "type": "server_tool_use",
                    "id": call.id,
                    "name": call.name,
                    "input": call.input
                }
            }),
        );
        push_sse_event(
            &mut body,
            Some("content_block_stop"),
            &json!({ "type": "content_block_stop", "index": index }),
        );
        index += 1;

        push_sse_event(
            &mut body,
            Some("content_block_start"),
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {
                    "type": "web_search_tool_result",
                    "tool_use_id": call.id,
                    "content": call.result_content
                }
            }),
        );
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
        for citation in &aggregated.citations {
            if let Some(delta) =
                build_anthropic_citation_delta_event(index, citation, &aggregated.text)
            {
                push_sse_event(&mut body, Some("content_block_delta"), &delta);
            }
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

    if let Some(percentage) = aggregated.context_usage_percentage {
        push_sse_event(
            &mut body,
            Some("context_usage"),
            &json!({
                "type": "context_usage",
                "percentage": percentage
            }),
        );
    }
    if let Some(usage) = aggregated.metering_usage {
        push_sse_event(
            &mut body,
            Some("metering"),
            &json!({
                "type": "metering",
                "usage": usage
            }),
        );
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
    response_id: Option<String>,
) -> ProxyResult {
    let request_id =
        response_id.unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
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

fn build_responses_citation_annotations(
    citations: &[crate::modules::kiro_gateway::models::AggregatedCitation],
) -> Vec<Value> {
    citations
        .iter()
        .map(|citation| {
            let mut value = json!({
                "type": "url_citation",
                "url": citation.link.clone(),
                "target": citation.target.clone(),
                "citationLink": citation.link.clone()
            });
            if let Some(range) = citation.target.get("range") {
                if let Some(start_index) = range.get("start").and_then(Value::as_u64) {
                    value["start_index"] = Value::from(start_index);
                }
                if let Some(end_index) = range.get("end").and_then(Value::as_u64) {
                    value["end_index"] = Value::from(end_index);
                }
            }
            if let Some(text) = citation.text.as_ref() {
                value["citationText"] = Value::String(text.clone());
            }
            value
        })
        .collect()
}

fn build_responses_annotation_added_event(
    response_id: &str,
    message_id: &str,
    annotation: Value,
    annotation_index: usize,
    sequence_number: usize,
) -> Value {
    json!({
        "type": "response.output_text.annotation.added",
        "response_id": response_id,
        "item_id": message_id,
        "output_index": 0,
        "content_index": 0,
        "annotation_index": annotation_index,
        "annotation": annotation,
        "sequence_number": sequence_number
    })
}

fn extract_web_search_sources(server_tool_calls: &[ServerToolCall]) -> Vec<WebSearchSource> {
    let mut seen = HashSet::new();
    let mut sources = Vec::new();

    for call in server_tool_calls
        .iter()
        .filter(|call| call.name == WEB_SEARCH_TOOL_NAME)
    {
        let Some(results) = call.result_content.as_array() else {
            continue;
        };

        for item in results {
            let Some(url) = item
                .get("url")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };

            if !seen.insert(url.to_string()) {
                continue;
            }

            let title = item
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(url)
                .to_string();

            sources.push(WebSearchSource {
                title,
                url: url.to_string(),
            });
        }
    }

    sources
}

fn build_responses_output_text(
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
) -> Value {
    let mut text = aggregated.text.clone();
    let mut annotations = build_responses_citation_annotations(&aggregated.citations);
    let sources = extract_web_search_sources(server_tool_calls);

    if !sources.is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str("Sources:\n");

        for (index, source) in sources.iter().enumerate() {
            let prefix = format!("[{}] ", index + 1);
            let start_index = text.chars().count() + prefix.chars().count();
            text.push_str(&prefix);
            text.push_str(&source.title);
            let end_index = start_index + source.title.chars().count();
            annotations.push(json!({
                "type": "url_citation",
                "start_index": start_index,
                "end_index": end_index,
                "url": source.url,
                "title": source.title
            }));
            text.push('\n');
        }

        let _ = text.pop();
    }

    json!({
        "text": text,
        "annotations": annotations
    })
}

fn build_responses_web_search_call(call: &ServerToolCall) -> Value {
    let mut action = json!({
        "type": "search"
    });
    if let Some(query) = call
        .input
        .get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        action["query"] = Value::String(query.to_string());
    }

    let sources = extract_web_search_sources(std::slice::from_ref(call));
    if !sources.is_empty() {
        action["sources"] = Value::Array(
            sources
                .into_iter()
                .map(|source| {
                    json!({
                        "type": "source",
                        "url": source.url,
                        "title": source.title
                    })
                })
                .collect(),
        );
    }

    json!({
        "id": call.id,
        "type": "web_search_call",
        "status": "completed",
        "action": action
    })
}

fn build_responses_message_content(
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
) -> Vec<Value> {
    let output_text = build_responses_output_text(aggregated, server_tool_calls);
    let mut content = Vec::new();
    if output_text
        .get("text")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.is_empty())
    {
        content.push(json!({
            "type": "output_text",
            "text": output_text.get("text").cloned().unwrap_or(Value::String(String::new())),
            "annotations": output_text.get("annotations").cloned().unwrap_or(Value::Array(Vec::new()))
        }));
    }
    if !aggregated.thinking.is_empty() {
        content.push(json!({
            "type": "reasoning",
            "summary": aggregated.thinking
        }));
    }
    for call in &aggregated.tool_calls {
        content.push(json!({
            "type": "function_call",
            "call_id": call.id,
            "name": call.name,
            "arguments": call.arguments
        }));
    }
    content
}

fn build_responses_response_with_ids(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
    response_id: &str,
    message_id: &str,
    created_at: i64,
    previous_response_id: Option<&str>,
) -> Value {
    let output_text = build_responses_output_text(aggregated, server_tool_calls);
    let mut output: Vec<Value> = server_tool_calls
        .iter()
        .filter(|call| call.name == WEB_SEARCH_TOOL_NAME)
        .map(build_responses_web_search_call)
        .collect();
    output.push(json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": build_responses_message_content(aggregated, server_tool_calls)
    }));
    json!({
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": "completed",
        "model": model,
        "previous_response_id": previous_response_id,
        "output": output,
        "output_text": output_text.get("text").cloned().unwrap_or(Value::String(String::new())),
        "usage": {
            "input_tokens": aggregated.input_tokens,
            "output_tokens": aggregated.output_tokens,
            "total_tokens": aggregated.input_tokens + aggregated.output_tokens
        }
    })
}

fn build_stream_responses_completed_event(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
    response_id: &str,
    message_id: &str,
    created_at: i64,
    previous_response_id: Option<&str>,
) -> Value {
    json!({
        "type": "response.completed",
        "response": build_responses_response_with_ids(
            model,
            aggregated,
            server_tool_calls,
            response_id,
            message_id,
            created_at,
            previous_response_id,
        )
    })
}

fn build_stream_responses_function_call_arguments_done_event(
    response_id: &str,
    call_id: &str,
    arguments: &str,
) -> Value {
    json!({
        "type": "response.function_call_arguments.done",
        "response_id": response_id,
        "call_id": call_id,
        "arguments": arguments
    })
}

fn build_stream_responses_output_text_done_event(response_id: &str, text: &str) -> Value {
    json!({
        "type": "response.output_text.done",
        "response_id": response_id,
        "text": text
    })
}

fn build_stream_responses_reasoning_done_event(response_id: &str, text: &str) -> Value {
    json!({
        "type": "response.reasoning.done",
        "response_id": response_id,
        "text": text
    })
}

fn build_direct_responses_response(
    model: &str,
    aggregated: &AggregatedKiroResponse,
    server_tool_calls: &[ServerToolCall],
    stream: bool,
    response_id_override: Option<String>,
    previous_response_id: Option<&str>,
) -> ProxyResult {
    let response_id =
        response_id_override.unwrap_or_else(|| format!("resp_{}", uuid::Uuid::new_v4().simple()));
    let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
    let created_at = chrono::Utc::now().timestamp();
    let response = build_responses_response_with_ids(
        model,
        aggregated,
        server_tool_calls,
        &response_id,
        &message_id,
        created_at,
        previous_response_id,
    );

    if stream {
        let mut body = String::new();
        push_sse_data(
            &mut body,
            &json!({
                "type": "response.created",
                "response": {
                    "id": response_id,
                    "object": "response",
                    "created_at": created_at,
                    "status": "in_progress",
                    "model": model,
                    "output": []
                }
            }),
        );
        push_sse_data(
            &mut body,
            &json!({
                "type": "response.output_item.added",
                "response_id": response_id,
                "output_index": 0,
                "item": {
                    "id": message_id,
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": []
                }
            }),
        );
        for (index, call) in server_tool_calls.iter().enumerate() {
            push_sse_data(
                &mut body,
                &json!({
                    "type": "response.output_item.added",
                    "response_id": response_id,
                    "output_index": index + 1,
                    "item": build_responses_web_search_call(call)
                }),
            );
        }
        for (index, call) in aggregated.tool_calls.iter().enumerate() {
            let output_index = index + 1 + server_tool_calls.len();
            push_sse_data(
                &mut body,
                &json!({
                    "type": "response.output_item.added",
                    "response_id": response_id,
                    "output_index": output_index,
                    "item": {
                        "id": call.id,
                        "type": "function_call",
                        "status": "in_progress",
                        "call_id": call.id,
                        "name": call.name,
                        "arguments": ""
                    }
                }),
            );
            push_sse_data(
                &mut body,
                &json!({
                    "type": "response.function_call_arguments.delta",
                    "response_id": response_id,
                    "call_id": call.id,
                    "delta": call.arguments
                }),
            );
            push_sse_data(
                &mut body,
                &build_stream_responses_function_call_arguments_done_event(
                    &response_id,
                    &call.id,
                    &call.arguments,
                ),
            );
            push_sse_data(
                &mut body,
                &json!({
                    "type": "response.output_item.done",
                    "response_id": response_id,
                    "output_index": output_index,
                    "item": {
                        "id": call.id,
                        "type": "function_call",
                        "status": "completed",
                        "call_id": call.id,
                        "name": call.name,
                        "arguments": call.arguments
                    }
                }),
            );
        }
        for (index, annotation) in build_responses_citation_annotations(&aggregated.citations)
            .into_iter()
            .enumerate()
        {
            push_sse_data(
                &mut body,
                &build_responses_annotation_added_event(
                    &response_id,
                    &message_id,
                    annotation,
                    index,
                    index,
                ),
            );
        }
        if !aggregated.text.is_empty() {
            push_sse_data(
                &mut body,
                &json!({
                    "type": "response.output_text.delta",
                    "response_id": response_id,
                    "delta": aggregated.text
                }),
            );
        }
        if !aggregated.thinking.is_empty() {
            push_sse_data(
                &mut body,
                &json!({
                    "type": "response.reasoning.delta",
                    "response_id": response_id,
                    "delta": aggregated.thinking
                }),
            );
        }
        if let Some(text) = build_responses_output_text(aggregated, server_tool_calls)
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            push_sse_data(
                &mut body,
                &build_stream_responses_output_text_done_event(&response_id, text),
            );
        }
        if !aggregated.thinking.is_empty() {
            push_sse_data(
                &mut body,
                &build_stream_responses_reasoning_done_event(&response_id, &aggregated.thinking),
            );
        }
        push_sse_data(
            &mut body,
            &json!({
                "type": "response.output_item.done",
                "response_id": response_id,
                "output_index": 0,
                "item": {
                    "id": message_id,
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": build_responses_message_content(aggregated, server_tool_calls)
                }
            }),
        );
        push_sse_data(
            &mut body,
            &build_stream_responses_completed_event(
                model,
                aggregated,
                server_tool_calls,
                &response_id,
                &message_id,
                created_at,
                previous_response_id,
            ),
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

fn has_server_web_search_tool(request: &GatewayProxyRequest) -> bool {
    request
        .tools
        .iter()
        .any(|tool| is_web_search_tool_type(&tool.tool_type))
}

fn server_web_search_iteration_limit(max_uses: Option<i32>) -> usize {
    max_uses
        .unwrap_or(MAX_SERVER_WEB_SEARCH_ITERATIONS as i32)
        .max(0)
        .min(MAX_SERVER_WEB_SEARCH_ITERATIONS as i32) as usize
}

fn normalized_assistant_message_from_aggregated(
    aggregated: &AggregatedKiroResponse,
) -> GatewayMessage {
    GatewayMessage {
        role: "assistant".to_string(),
        content: if aggregated.text.is_empty() {
            Value::String(String::new())
        } else {
            Value::String(aggregated.text.clone())
        },
        tool_calls: aggregated.tool_calls.clone(),
        tool_call_id: None,
        metadata: if aggregated.thinking.is_empty() {
            None
        } else {
            Some(json!({
                "reasoningContent": {
                    "reasoningText": {
                        "text": aggregated.thinking
                    }
                }
            }))
        },
    }
}

fn build_web_search_mcp_arguments(input: &Value) -> Value {
    let query = input
        .get("query")
        .and_then(Value::as_str)
        .or_else(|| input.get("search_query").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    json!({ "query": query })
}

fn extract_domain_from_url(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()?
        .host_str()
        .map(|host| host.trim_start_matches("www.").to_ascii_lowercase())
}

fn domain_matches_rule(domain: &str, rule: &str) -> bool {
    let normalized = rule.trim().trim_start_matches("www.").to_ascii_lowercase();
    domain == normalized || domain.ends_with(&format!(".{normalized}"))
}

fn domain_matches_filters(item: &Value, tool: Option<&GatewayTool>) -> bool {
    let Some(tool) = tool else {
        return true;
    };
    let domain = item
        .get("url")
        .and_then(Value::as_str)
        .and_then(extract_domain_from_url);
    let Some(domain) = domain else {
        return true;
    };

    if let Some(allowed) = tool
        .allowed_domains
        .as_ref()
        .filter(|items| !items.is_empty())
    {
        if !allowed
            .iter()
            .any(|entry| domain_matches_rule(&domain, entry))
        {
            return false;
        }
    }
    if let Some(blocked) = tool.blocked_domains.as_ref() {
        if blocked
            .iter()
            .any(|entry| domain_matches_rule(&domain, entry))
        {
            return false;
        }
    }
    true
}

fn normalize_anthropic_web_search_result(item: Value) -> Value {
    match item {
        Value::Object(mut map) => {
            map.insert(
                "type".to_string(),
                Value::String("web_search_result".to_string()),
            );
            Value::Object(map)
        }
        other => other,
    }
}

fn parse_web_search_mcp_result(result: &Value, tool: Option<&GatewayTool>) -> (Value, String) {
    let text = result
        .get("content")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find_map(|item| {
                if item.get("type").and_then(Value::as_str) == Some("text") {
                    item.get("text").and_then(Value::as_str).map(str::to_string)
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| result.to_string());

    let filtered_results = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| value.get("results").and_then(Value::as_array).cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|item| domain_matches_filters(item, tool))
        .map(normalize_anthropic_web_search_result)
        .collect::<Vec<_>>();

    let tool_result_text = if filtered_results.is_empty() {
        text
    } else {
        json!({ "results": filtered_results.clone() }).to_string()
    };

    (Value::Array(filtered_results), tool_result_text)
}

async fn call_mcp_tool(
    http: &reqwest::Client,
    upstream: &KiroUpstreamCredentials,
    tool_name: &str,
    arguments: Value,
) -> Result<Value, String> {
    let url = format!("https://q.{}.amazonaws.com/mcp", upstream.region);
    let payload = json!({
        "jsonrpc": "2.0",
        "id": uuid::Uuid::new_v4().simple().to_string(),
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    });

    let response = with_kiro_upstream_headers(http.post(&url), upstream, "application/json", false)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("MCP 上游请求失败: {e}"))?;
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .map_err(|e| format!("读取 MCP 上游响应失败: {e}"))?;
    if !(200..300).contains(&status) {
        let err = map_direct_upstream_error(status, &body);
        return Err(format!(
            "MCP 上游请求失败(status={}): {}",
            err.status, err.message
        ));
    }

    let value: Value = serde_json::from_str(&body)
        .unwrap_or_else(|_| json!({ "result": { "content": [{ "type": "text", "text": body }] } }));
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("MCP 工具调用失败");
        return Err(sanitize_upstream_error(message));
    }

    Ok(value.get("result").cloned().unwrap_or(value))
}

async fn send_generate_request_with_refresh(
    http: &reqwest::Client,
    upstream: &mut KiroUpstreamCredentials,
    account: &KiroAccount,
    payload: &Value,
) -> Result<Vec<u8>, String> {
    match send_generate_request_direct(http, upstream, payload).await {
        Ok(bytes) => Ok(bytes),
        Err(err) if err.status == 401 || err.status == 403 => {
            logger::log_warn(&format!(
                "[KiroLocalAccess] Kiro 上游认证失败，尝试刷新账号后重试: account={}, status={}, error={}",
                upstream.account_email, err.status, err.message
            ));
            let refreshed = kiro_account::refresh_account_token(&account.id).await?;
            *upstream = prepare_kiro_upstream_credentials(&refreshed).await?;
            send_generate_request_direct(http, upstream, payload)
                .await
                .map_err(|retry_err| {
                    format!(
                        "Kiro 上游请求失败(status={}): {}",
                        retry_err.status, retry_err.message
                    )
                })
        }
        Err(err) => Err(format!(
            "Kiro 上游请求失败(status={}): {}",
            err.status, err.message
        )),
    }
}

async fn execute_request_with_server_tools(
    http: &reqwest::Client,
    request: &GatewayProxyRequest,
    account: &KiroAccount,
    upstream: &mut KiroUpstreamCredentials,
    available_models: &[String],
) -> Result<ProxyExecutionOutcome, String> {
    let mut working_request = request.clone();
    let web_search_tool = request
        .tools
        .iter()
        .find(|tool| is_web_search_tool_type(&tool.tool_type))
        .cloned();
    let max_uses = server_web_search_iteration_limit(
        web_search_tool
            .as_ref()
            .and_then(|tool| tool.web_search_max_uses),
    );
    let mut server_tool_calls = Vec::new();

    for _ in 0..max_uses {
        let mut payload = build_kiro_payload(
            &working_request,
            upstream.profile_arn.clone(),
            available_models,
        )
        .await?;
        trim_payload_history_if_needed(&mut payload);
        if let Ok(serialized) = serde_json::to_string_pretty(&payload) {
            logger::log_info(&format!(
                "[KiroLocalAccess] 上游 payload: account={}, bytes={}, body={}",
                upstream.account_email,
                serialized.len(),
                serialized
            ));
        }

        let response_bytes =
            send_generate_request_with_refresh(http, upstream, account, &payload).await?;
        let payload_text = decode_eventstream_payload_text(&response_bytes);
        let aggregated = aggregate_kiro_response(&payload_text);
        let web_search_calls: Vec<GatewayToolCall> = aggregated
            .tool_calls
            .iter()
            .filter(|call| call.name == WEB_SEARCH_TOOL_NAME)
            .cloned()
            .collect();

        if web_search_calls.is_empty() {
            return Ok(ProxyExecutionOutcome {
                aggregated,
                server_tool_calls,
            });
        }

        working_request
            .messages
            .push(normalized_assistant_message_from_aggregated(&aggregated));

        let mut tool_result_blocks = Vec::new();
        for call in web_search_calls {
            let input = serde_json::from_str(&call.arguments)
                .unwrap_or_else(|_| json!({ "query": call.arguments }));
            let mcp_arguments = build_web_search_mcp_arguments(&input);
            let mcp_result = call_mcp_tool(http, upstream, &call.name, mcp_arguments).await?;
            let (result_content, tool_result_text) =
                parse_web_search_mcp_result(&mcp_result, web_search_tool.as_ref());

            server_tool_calls.push(ServerToolCall {
                id: call.id.clone(),
                name: call.name.clone(),
                input: input.clone(),
                result_content: result_content.clone(),
                tool_result_text,
            });

            tool_result_blocks.push(json!({
                "type": "web_search_tool_result",
                "tool_use_id": call.id,
                "content": result_content
            }));
        }

        working_request.messages.push(GatewayMessage {
            role: "user".to_string(),
            content: Value::Array(tool_result_blocks),
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: None,
        });
    }

    Err("web_search 代理循环超过最大轮数".to_string())
}

async fn proxy_via_kiro_direct(
    request: &GatewayProxyRequest,
    account: &KiroAccount,
    protocol: ApiProtocol,
) -> Result<ProxyResult, String> {
    let http = build_kiro_http_client(true)?;
    let mut upstream = prepare_kiro_upstream_credentials(account).await?;
    let available_models = match list_available_models_direct(&http, &upstream).await {
        Ok(models) => {
            logger::log_info(&format!(
                "[KiroLocalAccess] 账号可用模型: account={}, models={:?}",
                upstream.account_email, models
            ));
            models
        }
        Err(err) => {
            logger::log_warn(&format!(
                "[KiroLocalAccess] 获取账号模型列表失败: account={}, status={}, error={}，使用默认模型列表",
                upstream.account_email, err.status, err.message
            ));
            Vec::new()
        }
    };
    let mut effective_request = request.clone();
    if request.previous_response_id.is_some() {
        effective_request.messages = restore_responses_session_messages(request).await;
    }

    let outcome = if has_server_web_search_tool(&effective_request) {
        execute_request_with_server_tools(
            &http,
            &effective_request,
            account,
            &mut upstream,
            &available_models,
        )
        .await?
    } else {
        let mut payload = build_kiro_payload(
            &effective_request,
            upstream.profile_arn.clone(),
            &available_models,
        )
        .await?;
        trim_payload_history_if_needed(&mut payload);
        if let Ok(serialized) = serde_json::to_string_pretty(&payload) {
            logger::log_info(&format!(
                "[KiroLocalAccess] 上游 payload: account={}, protocol={:?}, bytes={}, body={}",
                upstream.account_email,
                protocol,
                serialized.len(),
                serialized
            ));
        }
        let response_bytes =
            send_generate_request_with_refresh(&http, &mut upstream, account, &payload).await?;
        ProxyExecutionOutcome {
            aggregated: aggregate_kiro_response(&decode_eventstream_payload_text(&response_bytes)),
            server_tool_calls: Vec::new(),
        }
    };

    let assistant_message = normalized_assistant_message_from_aggregated(&outcome.aggregated);
    Ok(match protocol {
        ApiProtocol::Responses => {
            let response_id = format!("resp_{}", uuid::Uuid::new_v4().simple());
            persist_responses_session_entry(
                &response_id,
                effective_request.messages.clone(),
                request.previous_response_id.clone(),
                assistant_message.clone(),
            )
            .await;
            build_direct_responses_response(
                &request.model,
                &outcome.aggregated,
                &outcome.server_tool_calls,
                request.stream,
                Some(response_id),
                request.previous_response_id.as_deref(),
            )
        }
        ApiProtocol::OpenAi => {
            let response_id = format!("chatcmpl-{}", uuid::Uuid::new_v4().simple());
            persist_responses_session_entry(
                &response_id,
                effective_request.messages.clone(),
                request.previous_response_id.clone(),
                assistant_message.clone(),
            )
            .await;
            build_direct_openai_response(
                &request.model,
                &outcome.aggregated,
                request.stream,
                Some(response_id),
            )
        }
        ApiProtocol::Anthropic => {
            let response_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
            persist_responses_session_entry(
                &response_id,
                effective_request.messages.clone(),
                request.previous_response_id.clone(),
                assistant_message,
            )
            .await;
            build_direct_anthropic_response(
                &request.model,
                &outcome.aggregated,
                &outcome.server_tool_calls,
                request.stream,
                Some(response_id),
            )
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

async fn restore_responses_session_messages(request: &GatewayProxyRequest) -> Vec<GatewayMessage> {
    let Some(mut current_response_id) = request.previous_response_id.clone() else {
        return request.messages.clone();
    };

    let rt = gateway_runtime().lock().await;
    let mut chain = Vec::new();
    while let Some(entry) = rt.responses_sessions.get(&current_response_id) {
        chain.push(entry.clone());
        let Some(previous) = entry.previous_response_id.clone() else {
            break;
        };
        current_response_id = previous;
    }
    drop(rt);

    if chain.is_empty() {
        return request.messages.clone();
    }

    chain.reverse();
    let mut merged = Vec::new();
    for entry in chain {
        merged.extend(entry.request_messages.clone());
        merged.push(entry.assistant_message.clone());
    }
    merged.extend(request.messages.clone());
    merged
}

async fn persist_responses_session_entry(
    response_id: &str,
    request_messages: Vec<GatewayMessage>,
    previous_response_id: Option<String>,
    assistant_message: GatewayMessage,
) {
    let mut rt = gateway_runtime().lock().await;
    rt.responses_sessions
        .retain(|_, entry| entry.updated_at.elapsed() < Duration::from_secs(60 * 60));
    rt.responses_sessions.insert(
        response_id.to_string(),
        ResponsesSessionEntry {
            previous_response_id,
            request_messages,
            assistant_message,
            updated_at: Instant::now(),
        },
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_responses_gateway_request_preserves_message_content_items() {
        let payload = json!({
            "model": "claude-3-7-sonnet-20250219",
            "stream": true,
            "input": [
                {
                    "role": "user",
                    "content": [
                        { "type": "input_text", "text": "第一段" },
                        { "type": "input_text", "text": "第二段" },
                        { "type": "input_image", "image_url": "data:image/png;base64,aGVsbG8=" }
                    ]
                }
            ]
        });

        let converted = normalize_responses_gateway_request(
            serde_json::to_string(&payload).unwrap().as_bytes(),
        )
        .expect("responses payload should convert");

        assert!(converted.stream);
        assert_eq!(converted.messages.len(), 1);
        assert_eq!(converted.messages[0].role, "user");
        assert_eq!(
            converted.messages[0].content,
            json!([
                { "type": "input_text", "text": "第一段" },
                { "type": "input_text", "text": "第二段" },
                { "type": "input_image", "image_url": "data:image/png;base64,aGVsbG8=" }
            ])
        );
    }

    #[tokio::test]
    async fn build_kiro_payload_moves_long_tool_docs_into_prompt() {
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: true,
            messages: vec![
                GatewayMessage {
                    role: "system".to_string(),
                    content: Value::String("系统要求".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    metadata: None,
                },
                GatewayMessage {
                    role: "assistant".to_string(),
                    content: Value::String("我先调用工具".to_string()),
                    tool_calls: vec![GatewayToolCall {
                        id: "call_1".to_string(),
                        name: "search_docs".to_string(),
                        arguments: "{\"q\":\"gateway\"}".to_string(),
                    }],
                    tool_call_id: None,
                    metadata: None,
                },
                GatewayMessage {
                    role: "tool".to_string(),
                    content: Value::String("命中结果".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: Some("call_1".to_string()),
                    metadata: None,
                },
                GatewayMessage {
                    role: "user".to_string(),
                    content: Value::String("继续总结".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    metadata: None,
                },
            ],
            tools: vec![GatewayTool {
                tool_type: "function".to_string(),
                name: "search_docs".to_string(),
                description: "A".repeat(TOOL_DESCRIPTION_MAX_LENGTH + 32),
                input_schema: json!({
                    "type": "object",
                    "properties": { "q": { "type": "string" } },
                    "required": ["q"]
                }),
                web_search_max_uses: None,
                allowed_domains: None,
                blocked_domains: None,
                user_location: None,
            }],
            tool_choice: None,
            previous_response_id: None,
        };

        let payload = build_kiro_payload(&request, Some("arn:test".to_string()), &[])
            .await
            .expect("payload should build");

        let current_content = payload
            .pointer("/conversationState/currentMessage/userInputMessage/content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(current_content.contains("Tool Documentation"));

        let history = payload
            .pointer("/conversationState/history")
            .and_then(Value::as_array)
            .expect("history should exist");
        assert_eq!(history.len(), 2);
        assert_eq!(
            history[0]
                .pointer("/assistantResponseMessage/toolUses")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            history[1]
                .pointer("/userInputMessage/userInputMessageContext/toolResults")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn trim_payload_history_preserves_tool_call_result_pair() {
        let mut payload = json!({
            "conversationState": {
                "history": [
                    {
                        "assistantResponseMessage": {
                            "content": "old assistant",
                            "toolUses": [{ "name": "search_docs", "toolUseId": "call_1", "input": { "q": "gateway" } }]
                        }
                    },
                    {
                        "userInputMessage": {
                            "content": "",
                            "userInputMessageContext": {
                                "toolResults": [{ "toolUseId": "call_1", "status": "success", "content": [{ "text": "命中结果" }] }]
                            }
                        }
                    },
                    {
                        "assistantResponseMessage": {
                            "content": "recent assistant"
                        }
                    },
                    {
                        "userInputMessage": {
                            "content": "recent user"
                        }
                    }
                ]
            }
        });

        let filler = "X".repeat(MAX_KIRO_PAYLOAD_SIZE);
        payload["conversationState"]["history"][0]["assistantResponseMessage"]["content"] =
            Value::String(filler);

        trim_payload_history_if_needed(&mut payload);

        let history = payload
            .pointer("/conversationState/history")
            .and_then(Value::as_array)
            .expect("history should remain");
        assert_eq!(history.len(), 2);
        assert!(
            history[0].get("assistantResponseMessage").is_some()
                || history[0].get("userInputMessage").is_some()
        );
        assert_eq!(
            history[0]
                .pointer("/assistantResponseMessage/content")
                .and_then(Value::as_str),
            Some("recent assistant")
        );
        assert_eq!(
            history[1]
                .pointer("/userInputMessage/content")
                .and_then(Value::as_str),
            Some("recent user")
        );
    }

    #[tokio::test]
    async fn build_kiro_payload_extracts_base64_images() {
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: json!([
                    { "type": "input_text", "text": "看这张图" },
                    { "type": "input_image", "image_url": "data:image/png;base64,aGVsbG8=" }
                ]),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: None,
        };

        let payload = build_kiro_payload(&request, None, &[])
            .await
            .expect("payload should build");
        assert_eq!(
            payload
                .pointer("/conversationState/currentMessage/userInputMessage/images")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            payload
                .pointer("/conversationState/currentMessage/userInputMessage/images/0/format")
                .and_then(Value::as_str),
            Some("png")
        );
    }

    #[tokio::test]
    async fn build_kiro_payload_reuses_previous_response_id_as_conversation_id() {
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: Value::String("继续".to_string()),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: Some("resp_prev_123".to_string()),
        };

        let payload = build_kiro_payload(&request, None, &[])
            .await
            .expect("payload should build");

        assert_eq!(
            payload
                .pointer("/conversationState/conversationId")
                .and_then(Value::as_str),
            Some("resp_prev_123")
        );
    }

    #[tokio::test]
    async fn build_kiro_payload_rejects_private_remote_images() {
        use std::io::{Read, Write};
        use std::thread;

        let expected_bytes = vec![137, 80, 78, 71, 13, 10, 26, 10];
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        listener
            .set_nonblocking(true)
            .expect("listener should set nonblocking");
        let address = format!(
            "http://{}",
            listener.local_addr().expect("local addr should resolve")
        );

        let handle = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0u8; 1024];
                        let _ = stream.read(&mut buffer);
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            expected_bytes.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("headers should write");
                        stream
                            .write_all(&expected_bytes)
                            .expect("body should write");
                        return true;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            return false;
                        }
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => return false,
                }
            }
        });

        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: json!([
                    { "type": "input_text", "text": "看图回答" },
                    { "type": "input_image", "image_url": format!("{address}/sample.png") }
                ]),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: None,
        };

        let payload = build_kiro_payload(&request, None, &[])
            .await
            .expect("payload should build");

        assert!(
            !handle.join().expect("server thread should finish"),
            "client should not fetch private image"
        );
        assert!(payload
            .pointer("/conversationState/currentMessage/userInputMessage/images")
            .and_then(Value::as_array)
            .map(Vec::is_empty)
            .unwrap_or(true));
    }

    #[tokio::test]
    async fn build_kiro_payload_rejects_oversized_data_url_images() {
        let oversized = STANDARD.encode(vec![0u8; 6 * 1024 * 1024]);
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: json!([
                    { "type": "input_text", "text": "看图回答" },
                    { "type": "input_image", "image_url": format!("data:image/png;base64,{oversized}") }
                ]),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: None,
        };

        let payload = build_kiro_payload(&request, None, &[])
            .await
            .expect("payload should build");

        assert!(payload
            .pointer("/conversationState/currentMessage/userInputMessage/images")
            .and_then(Value::as_array)
            .map(Vec::is_empty)
            .unwrap_or(true));
    }

    #[tokio::test]
    async fn build_kiro_payload_preserves_assistant_message_metadata() {
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![
                GatewayMessage {
                    role: "assistant".to_string(),
                    content: json!([
                        { "type": "output_text", "text": "历史回答" },
                        { "type": "reasoning", "summary": "内部推理" }
                    ]),
                    tool_calls: vec![GatewayToolCall {
                        id: "call_1".to_string(),
                        name: "search_docs".to_string(),
                        arguments: "{\"q\":\"gateway\"}".to_string(),
                    }],
                    tool_call_id: None,
                    metadata: Some(json!({
                        "reasoningContent": {
                            "reasoningText": {
                                "text": "内部推理",
                                "signature": "sig_1"
                            }
                        },
                        "references": [
                            {
                                "licenseName": "MIT",
                                "repository": "repo",
                                "url": "https://example.com/ref"
                            }
                        ],
                        "supplementaryWebLinks": [
                            {
                                "url": "https://example.com",
                                "title": "example",
                                "snippet": "snippet"
                            }
                        ],
                        "followupPrompt": {
                            "content": "继续",
                            "userIntent": "SHOW_EXAMPLES"
                        },
                        "messageId": "msg_123",
                        "cachePoint": {
                            "type": "default"
                        }
                    })),
                },
                GatewayMessage {
                    role: "user".to_string(),
                    content: Value::String("继续".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    metadata: None,
                },
            ],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: None,
        };

        let payload = build_kiro_payload(&request, None, &[])
            .await
            .expect("payload should build");
        let history = payload
            .pointer("/conversationState/history")
            .and_then(Value::as_array)
            .expect("history should exist");

        assert_eq!(
            history[0]
                .pointer("/assistantResponseMessage/content")
                .and_then(Value::as_str),
            Some("历史回答")
        );
        assert_eq!(
            history[0].pointer("/assistantResponseMessage/reasoningContent"),
            Some(&json!({
                "reasoningText": {
                    "text": "内部推理",
                    "signature": "sig_1"
                }
            }))
        );
        assert_eq!(
            history[0].pointer("/assistantResponseMessage/references"),
            Some(&json!([
                {
                    "licenseName": "MIT",
                    "repository": "repo",
                    "url": "https://example.com/ref"
                }
            ]))
        );
        assert_eq!(
            history[0].pointer("/assistantResponseMessage/supplementaryWebLinks"),
            Some(&json!([
                {
                    "url": "https://example.com",
                    "title": "example",
                    "snippet": "snippet"
                }
            ]))
        );
        assert_eq!(
            history[0].pointer("/assistantResponseMessage/followupPrompt"),
            Some(&json!({
                "content": "继续",
                "userIntent": "SHOW_EXAMPLES"
            }))
        );
        assert_eq!(
            history[0]
                .pointer("/assistantResponseMessage/messageId")
                .and_then(Value::as_str),
            Some("msg_123")
        );
        assert_eq!(
            history[0].pointer("/assistantResponseMessage/cachePoint"),
            Some(&json!({ "type": "default" }))
        );
        assert_eq!(
            history[0]
                .pointer("/assistantResponseMessage/toolUses/0/name")
                .and_then(Value::as_str),
            Some("search_docs")
        );
    }

    #[tokio::test]
    async fn build_kiro_payload_preserves_responses_tool_choice() {
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: Value::String("hello".to_string()),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: vec![GatewayTool {
                tool_type: "function".to_string(),
                name: "search_docs".to_string(),
                description: "搜索文档".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "q": { "type": "string" } }
                }),
                web_search_max_uses: None,
                allowed_domains: None,
                blocked_domains: None,
                user_location: None,
            }],
            tool_choice: Some(json!({ "type": "function", "name": "search_docs" })),
            previous_response_id: None,
        };

        let payload = build_kiro_payload(&request, None, &[])
            .await
            .expect("payload should build");

        assert_eq!(
            payload
                .pointer("/conversationState/currentMessage/userInputMessage/userInputMessageContext/toolChoice")
                .cloned(),
            Some(json!({ "type": "function", "name": "search_docs" }))
        );
    }

    #[tokio::test]
    async fn build_kiro_payload_rejects_unknown_tool_choice_function() {
        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: Value::String("hello".to_string()),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: vec![GatewayTool {
                tool_type: "function".to_string(),
                name: "search_docs".to_string(),
                description: "搜索文档".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "q": { "type": "string" } }
                }),
                web_search_max_uses: None,
                allowed_domains: None,
                blocked_domains: None,
                user_location: None,
            }],
            tool_choice: Some(json!({ "type": "function", "name": "missing_tool" })),
            previous_response_id: None,
        };

        let error = build_kiro_payload(&request, None, &[])
            .await
            .expect_err("unknown tool choice should fail");

        assert!(error.contains("tool_choice 指定的工具不存在"));
    }

    #[test]
    fn build_direct_responses_response_preserves_previous_response_id() {
        let aggregated = AggregatedKiroResponse {
            text: "hello".to_string(),
            ..Default::default()
        };

        let response = build_direct_responses_response(
            "claude-sonnet-4-5-20250929",
            &aggregated,
            &[],
            false,
            Some("resp_new_1".to_string()),
            Some("resp_prev_123"),
        );

        let value: Value = serde_json::from_slice(&response.body).expect("valid json");
        assert_eq!(value["id"], "resp_new_1");
        assert_eq!(value["previous_response_id"], "resp_prev_123");
    }

    #[test]
    fn build_direct_responses_response_stream_emits_kam_like_events() {
        let aggregated = AggregatedKiroResponse {
            text: "hello".to_string(),
            thinking: "reason".to_string(),
            tool_calls: vec![GatewayToolCall {
                id: "call_1".to_string(),
                name: "search_docs".to_string(),
                arguments: "{\"q\":\"gateway\"}".to_string(),
            }],
            citations: vec![crate::modules::kiro_gateway::models::AggregatedCitation {
                text: Some("hello".to_string()),
                link: "https://example.com".to_string(),
                target: json!({"range":{"start":0,"end":5}}),
            }],
            ..Default::default()
        };

        let response = build_direct_responses_response(
            "claude-sonnet-4-5-20250929",
            &aggregated,
            &[],
            true,
            Some("resp_new_1".to_string()),
            Some("resp_prev_123"),
        );
        let body = String::from_utf8(response.body).expect("stream should be utf8");

        assert!(body.contains("\"type\":\"response.created\""));
        assert!(body.contains("\"type\":\"response.output_item.added\""));
        assert!(body.contains("\"type\":\"response.function_call_arguments.done\""));
        assert!(body.contains("\"type\":\"response.output_text.annotation.added\""));
        assert!(body.contains("\"type\":\"response.output_text.done\""));
        assert!(body.contains("\"type\":\"response.reasoning.done\""));
        assert!(body.contains("\"type\":\"response.output_item.done\""));
        assert!(body.contains("\"type\":\"response.completed\""));
        assert!(body.contains("data: [DONE]"));
    }

    #[test]
    fn build_direct_anthropic_response_stream_emits_citation_and_usage_events() {
        let aggregated = AggregatedKiroResponse {
            text: "hello".to_string(),
            citations: vec![crate::modules::kiro_gateway::models::AggregatedCitation {
                text: Some("hello".to_string()),
                link: "https://example.com".to_string(),
                target: json!({"range":{"start":0,"end":5}}),
            }],
            context_usage_percentage: Some(42.0),
            metering_usage: Some(0.25),
            ..Default::default()
        };

        let response = build_direct_anthropic_response(
            "claude-sonnet-4-5-20250929",
            &aggregated,
            &[],
            true,
            None,
        );
        let body = String::from_utf8(response.body).expect("stream should be utf8");

        assert!(body.contains("event: message_start"));
        assert!(body.contains("\"type\":\"citations_delta\""));
        assert!(body.contains("event: context_usage"));
        assert!(body.contains("\"percentage\":42.0"));
        assert!(body.contains("event: metering"));
        assert!(body.contains("\"usage\":0.25"));
        assert!(body.contains("event: message_stop"));
    }

    #[test]
    fn build_direct_anthropic_response_emits_server_tool_blocks() {
        let aggregated = AggregatedKiroResponse {
            text: "hello".to_string(),
            ..Default::default()
        };
        let server_tool_calls = vec![ServerToolCall {
            id: "toolu_web_1".to_string(),
            name: "web_search".to_string(),
            input: json!({"query":"rust async"}),
            result_content: json!([{
                "type":"web_search_result",
                "title":"Rust Async",
                "url":"https://example.com/rust-async"
            }]),
            tool_result_text: "{\"results\":[]}".to_string(),
        }];

        let response = build_direct_anthropic_response(
            "claude-sonnet-4-5-20250929",
            &aggregated,
            &server_tool_calls,
            false,
            None,
        );
        let value: Value = serde_json::from_slice(&response.body).expect("valid json");

        assert_eq!(value["content"][0]["type"], "server_tool_use");
        assert_eq!(value["content"][1]["type"], "web_search_tool_result");
        assert_eq!(value["content"][1]["tool_use_id"], "toolu_web_1");
    }

    #[test]
    fn build_direct_responses_response_includes_web_search_sources() {
        let aggregated = AggregatedKiroResponse {
            text: "hello".to_string(),
            ..Default::default()
        };
        let server_tool_calls = vec![ServerToolCall {
            id: "toolu_web_1".to_string(),
            name: "web_search".to_string(),
            input: json!({"query":"rust async"}),
            result_content: json!([{
                "type":"web_search_result",
                "title":"Rust Async",
                "url":"https://example.com/rust-async"
            }]),
            tool_result_text: "{\"results\":[]}".to_string(),
        }];

        let response = build_direct_responses_response(
            "claude-sonnet-4-5-20250929",
            &aggregated,
            &server_tool_calls,
            false,
            Some("resp_new_1".to_string()),
            None,
        );
        let value: Value = serde_json::from_slice(&response.body).expect("valid json");

        assert_eq!(value["output"][0]["type"], "web_search_call");
        assert!(value["output_text"]
            .as_str()
            .is_some_and(|text| text.contains("Sources:\n[1] Rust Async")));
    }

    #[tokio::test]
    async fn restore_responses_session_messages_replays_previous_assistant_turn() {
        {
            let mut rt = gateway_runtime().lock().await;
            rt.responses_sessions.clear();
            rt.responses_sessions.insert(
                "resp_prev_123".to_string(),
                ResponsesSessionEntry {
                    previous_response_id: None,
                    request_messages: vec![GatewayMessage {
                        role: "user".to_string(),
                        content: Value::String("第一次提问".to_string()),
                        tool_calls: Vec::new(),
                        tool_call_id: None,
                        metadata: None,
                    }],
                    assistant_message: GatewayMessage {
                        role: "assistant".to_string(),
                        content: Value::String("第一次回答".to_string()),
                        tool_calls: vec![GatewayToolCall {
                            id: "call_1".to_string(),
                            name: "search_docs".to_string(),
                            arguments: "{\"q\":\"gateway\"}".to_string(),
                        }],
                        tool_call_id: None,
                        metadata: None,
                    },
                    updated_at: Instant::now(),
                },
            );
        }

        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: Value::String("第二次提问".to_string()),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: Some("resp_prev_123".to_string()),
        };

        let merged = restore_responses_session_messages(&request).await;
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].role, "user");
        assert_eq!(merged[1].role, "assistant");
        assert_eq!(merged[2].role, "user");
        assert_eq!(merged[1].tool_calls.len(), 1);
        assert_eq!(merged[1].tool_calls[0].name, "search_docs");
    }

    #[tokio::test]
    async fn restore_responses_session_messages_preserves_assistant_metadata() {
        {
            let mut rt = gateway_runtime().lock().await;
            rt.responses_sessions.clear();
            rt.responses_sessions.insert(
                "msg_prev_123".to_string(),
                ResponsesSessionEntry {
                    previous_response_id: None,
                    request_messages: vec![GatewayMessage {
                        role: "user".to_string(),
                        content: Value::String("第一次提问".to_string()),
                        tool_calls: Vec::new(),
                        tool_call_id: None,
                        metadata: None,
                    }],
                    assistant_message: GatewayMessage {
                        role: "assistant".to_string(),
                        content: Value::String("第一次回答".to_string()),
                        tool_calls: Vec::new(),
                        tool_call_id: None,
                        metadata: Some(json!({
                            "reasoningContent": {
                                "reasoningText": {
                                    "text": "内部推理"
                                }
                            }
                        })),
                    },
                    updated_at: Instant::now(),
                },
            );
        }

        let request = GatewayProxyRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            stream: false,
            messages: vec![GatewayMessage {
                role: "user".to_string(),
                content: Value::String("第二次提问".to_string()),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata: None,
            }],
            tools: Vec::new(),
            tool_choice: None,
            previous_response_id: Some("msg_prev_123".to_string()),
        };

        let merged = restore_responses_session_messages(&request).await;
        assert_eq!(merged.len(), 3);
        assert_eq!(
            merged[1]
                .metadata
                .as_ref()
                .and_then(|value| value.pointer("/reasoningContent/reasoningText/text"))
                .and_then(Value::as_str),
            Some("内部推理")
        );
    }

    #[test]
    fn gateway_tools_to_kiro_includes_web_search_domain_hints() {
        let tools = vec![GatewayTool {
            tool_type: "web_search_20260209".to_string(),
            name: "web_search".to_string(),
            description: "搜索网络".to_string(),
            input_schema: json!({}),
            web_search_max_uses: Some(3),
            allowed_domains: Some(vec!["blog.rust-lang.org".to_string()]),
            blocked_domains: Some(vec!["example.com".to_string()]),
            user_location: Some(json!({"type":"approximate","city":"Singapore"})),
        }];

        let converted = gateway_tools_to_kiro(&tools).expect("tool should convert");
        let converted_value = serde_json::to_value(&converted[0]).expect("tool should serialize");
        let description = converted_value
            .pointer("/toolSpecification/description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(description.contains("Only return results from: blog.rust-lang.org"));
        assert!(description.contains("Exclude results from: example.com"));
        assert_eq!(
            converted_value
                .pointer("/toolSpecification/inputSchema/json/required/0")
                .and_then(Value::as_str),
            Some("query")
        );
    }

    #[test]
    fn normalize_json_schema_prunes_unsupported_nested_keywords() {
        let normalized = normalize_json_schema(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": false,
            "properties": {
                "agent": {
                    "additionalProperties": false,
                    "properties": {
                        "mode": {
                            "enum": ["default", "plan"],
                            "type": "string"
                        },
                        "metadata": {
                            "additionalProperties": false,
                            "properties": {
                                "name": { "type": "string" }
                            },
                            "propertyNames": {
                                "pattern": "^[a-z]+$"
                            },
                            "required": ["name"],
                            "type": "object"
                        }
                    },
                    "required": ["mode"],
                    "type": "object"
                },
                "choices": {
                    "items": {
                        "additionalProperties": false,
                        "properties": {
                            "label": { "type": ["string", "null"] }
                        },
                        "type": "object"
                    },
                    "type": "array"
                }
            },
            "required": ["agent"],
            "type": "object"
        }));

        assert!(normalized.get("$schema").is_none());
        assert!(normalized.get("additionalProperties").is_none());
        assert!(normalized
            .pointer("/properties/agent/additionalProperties")
            .is_none());
        assert!(normalized
            .pointer("/properties/agent/properties/metadata/propertyNames")
            .is_none());
        assert_eq!(
            normalized
                .pointer("/properties/agent/type")
                .and_then(Value::as_str),
            Some("object")
        );
        assert_eq!(
            normalized
                .pointer("/properties/choices/type")
                .and_then(Value::as_str),
            Some("array")
        );
        assert_eq!(
            normalized
                .pointer("/properties/choices/items/properties/label/type")
                .and_then(Value::as_str),
            Some("string")
        );
    }
}
