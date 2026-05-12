use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use rusqlite::{params_from_iter, types::Value, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use url::Url;
use uuid::Uuid;

use crate::modules;

const DEFAULT_INSTANCE_ID: &str = "__default__";
const DEFAULT_INSTANCE_NAME: &str = "默认实例";
const STATE_DB_FILE: &str = "state_5.sqlite";
const SESSION_INDEX_FILE: &str = "session_index.jsonl";
const GLOBAL_STATE_FILE: &str = ".codex-global-state.json";
const SHARED_CHAT_CATALOG_FILE: &str = ".cockpit_codex_shared_chat_catalog.json";
const SHARED_CHAT_THREAD_SOURCE: &str = "cockpit_shared_foreign";
const SESSION_TRASH_ROOT_DIR: &str = "cockpit-tools-codex-session-trash";
const TOKEN_STATS_READ_CHUNK_BYTES: usize = 64 * 1024;

static TOKEN_STATS_CACHE: LazyLock<Mutex<HashMap<PathBuf, TokenStatsCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionLocation {
    pub instance_id: String,
    pub instance_name: String,
    pub running: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionRecord {
    pub session_id: String,
    pub title: String,
    pub cwd: String,
    pub updated_at: Option<i64>,
    pub location_count: usize,
    pub locations: Vec<CodexSessionLocation>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSharedChatCatalogRecord {
    pub source_session_id: String,
    pub title: String,
    pub cwd: String,
    pub updated_at: Option<i64>,
    pub archived: bool,
    pub foreign: bool,
    pub materialized: bool,
    pub local_session_id: Option<String>,
    pub source_instance_id: String,
    pub source_instance_name: String,
    pub source_account_id: Option<String>,
    pub source_codex_home: String,
    pub source_rollout_path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSharedChatVisibilitySummary {
    pub target_instance_id: String,
    pub target_instance_name: String,
    pub materialized_foreign_count: usize,
    pub copied_same_account_count: usize,
    pub removed_unsafe_same_id_count: usize,
    pub backup_dir: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionTokenStats {
    pub session_id: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionTrashSummary {
    pub requested_session_count: usize,
    pub trashed_session_count: usize,
    pub trashed_instance_count: usize,
    pub trash_dirs: Vec<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexTrashedSessionLocation {
    pub instance_id: String,
    pub instance_name: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexTrashedSessionRecord {
    pub session_id: String,
    pub title: String,
    pub cwd: String,
    pub deleted_at: Option<i64>,
    pub location_count: usize,
    pub locations: Vec<CodexTrashedSessionLocation>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionRestoreSummary {
    pub requested_session_count: usize,
    pub restored_session_count: usize,
    pub restored_instance_count: usize,
    pub message: String,
}

#[derive(Debug, Clone)]
struct CodexSyncInstance {
    id: String,
    name: String,
    data_dir: PathBuf,
    account_id: Option<String>,
    last_pid: Option<u32>,
}

#[derive(Debug, Clone)]
struct ThreadRowData {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl ThreadRowData {
    fn get_value(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|item| item == column)
            .and_then(|index| self.values.get(index))
    }

    fn get_text(&self, column: &str) -> Option<String> {
        match self.get_value(column)? {
            Value::Text(value) => Some(value.clone()),
            Value::Integer(value) => Some(value.to_string()),
            Value::Real(value) => Some(value.to_string()),
            _ => None,
        }
    }

    fn get_i64(&self, column: &str) -> Option<i64> {
        match self.get_value(column)? {
            Value::Integer(value) => Some(*value),
            Value::Text(value) => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn set_text(&mut self, column: &str, value: String) {
        if let Some(index) = self.columns.iter().position(|item| item == column) {
            if let Some(slot) = self.values.get_mut(index) {
                *slot = Value::Text(value);
            }
        }
    }

    fn set_null(&mut self, column: &str) {
        if let Some(index) = self.columns.iter().position(|item| item == column) {
            if let Some(slot) = self.values.get_mut(index) {
                *slot = Value::Null;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ThreadSnapshot {
    id: String,
    title: String,
    cwd: String,
    updated_at: Option<i64>,
    archived: bool,
    rollout_path: PathBuf,
    rollout_len: Option<u64>,
    rollout_modified_at_ms: Option<i64>,
    row_data: ThreadRowData,
    session_index_entry: JsonValue,
    source_root: PathBuf,
    materialized_foreign: bool,
}

#[derive(Debug, Clone)]
struct OwnedThreadSnapshot {
    instance: CodexSyncInstance,
    snapshot: ThreadSnapshot,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct SharedChatSourceIdentity {
    source_account_id: Option<String>,
    source_instance_id: String,
    source_session_id: String,
    archived: bool,
}

impl SharedChatSourceIdentity {
    fn key(&self) -> String {
        shared_chat_mapping_key_from_parts(
            self.source_account_id.as_deref(),
            &self.source_instance_id,
            &self.source_session_id,
            self.archived,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SharedChatCatalogFile {
    version: u32,
    updated_at: String,
    mappings: Vec<SharedChatMapping>,
}

impl Default for SharedChatCatalogFile {
    fn default() -> Self {
        Self {
            version: 1,
            updated_at: Utc::now().to_rfc3339(),
            mappings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SharedChatMapping {
    key: String,
    source_instance_id: String,
    source_account_id: Option<String>,
    source_codex_home: String,
    source_session_id: String,
    source_rollout_path: String,
    local_instance_id: String,
    local_account_id: Option<String>,
    local_session_id: String,
    local_rollout_path: String,
    archived: bool,
    last_source_updated_at: Option<i64>,
    last_source_rollout_len: Option<u64>,
    last_source_rollout_modified_at_ms: Option<i64>,
    local_materialized_updated_at: Option<i64>,
    local_materialized_rollout_len: Option<u64>,
    local_materialized_rollout_modified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrashedSessionManifest {
    session_id: String,
    title: String,
    cwd: String,
    instance_id: String,
    instance_name: String,
    instance_root: PathBuf,
    original_rollout_path: PathBuf,
    relative_rollout_path: String,
    session_index_entry: JsonValue,
    thread_row: JsonValue,
    deleted_at: Option<String>,
}

#[derive(Debug, Clone)]
struct TrashedSessionEntry {
    entry_dir: PathBuf,
    manifest: TrashedSessionManifest,
    trashed_rollout_path: PathBuf,
}

#[derive(Debug, Clone)]
struct TokenStatsCacheEntry {
    file_len: u64,
    modified_at: Option<SystemTime>,
    stats: Option<(u64, u64, u64)>,
}

/// 从 rollout JSONL 文件中读取 token 统计信息
/// 返回 (input_tokens, output_tokens, total_tokens)
fn read_token_stats_from_rollout(rollout_path: &Path) -> Option<(u64, u64, u64)> {
    let metadata = fs::metadata(rollout_path).ok()?;
    let cache_key = rollout_path.to_path_buf();
    let file_len = metadata.len();
    let modified_at = metadata.modified().ok();

    {
        let cache = TOKEN_STATS_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = cache.get(&cache_key) {
            if entry.file_len == file_len && entry.modified_at == modified_at {
                return entry.stats;
            }
        }
    }

    let stats = read_token_stats_from_rollout_uncached(rollout_path, file_len);
    let mut cache = TOKEN_STATS_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.insert(
        cache_key,
        TokenStatsCacheEntry {
            file_len,
            modified_at,
            stats,
        },
    );
    stats
}

fn rollout_file_fingerprint(rollout_path: &Path) -> (Option<u64>, Option<i64>) {
    let Ok(metadata) = fs::metadata(rollout_path) else {
        return (None, None);
    };
    (
        Some(metadata.len()),
        metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64),
    )
}

fn read_token_stats_from_rollout_uncached(
    rollout_path: &Path,
    file_len: u64,
) -> Option<(u64, u64, u64)> {
    let mut file = File::open(rollout_path).ok()?;
    let mut offset = file_len;
    let mut pending_prefix = Vec::new();

    while offset > 0 {
        let chunk_len = TOKEN_STATS_READ_CHUNK_BYTES.min(offset as usize);
        offset -= chunk_len as u64;

        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut chunk = vec![0u8; chunk_len];
        file.read_exact(&mut chunk).ok()?;

        let starts_on_line_boundary =
            offset == 0 || byte_before_is_newline(&mut file, offset).ok()?;
        chunk.extend_from_slice(&pending_prefix);

        let parse_from_index = if starts_on_line_boundary {
            pending_prefix.clear();
            0
        } else if let Some(newline_index) = chunk.iter().position(|byte| *byte == b'\n') {
            pending_prefix = chunk[..newline_index].to_vec();
            newline_index + 1
        } else {
            pending_prefix = chunk;
            continue;
        };

        if let Some(stats) = parse_token_stats_lines(&chunk[parse_from_index..]) {
            return Some(stats);
        }
    }

    if pending_prefix.is_empty() {
        None
    } else {
        parse_token_stats_lines(&pending_prefix)
    }
}

fn byte_before_is_newline(file: &mut File, offset: u64) -> std::io::Result<bool> {
    if offset == 0 {
        return Ok(true);
    }

    file.seek(SeekFrom::Start(offset - 1))?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)?;
    Ok(byte[0] == b'\n')
}

fn parse_token_stats_lines(content: &[u8]) -> Option<(u64, u64, u64)> {
    for line in content.split(|byte| *byte == b'\n').rev() {
        let raw = String::from_utf8_lossy(line);
        let trimmed = raw.trim();
        if trimmed.is_empty()
            || !trimmed.contains("\"token_count\"")
            || !trimmed.contains("\"total_token_usage\"")
        {
            continue;
        }

        let Ok(parsed) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        if parsed.get("type").and_then(|value| value.as_str()) != Some("event_msg") {
            continue;
        }
        let Some(payload) = parsed.get("payload") else {
            continue;
        };
        if payload.get("type").and_then(|value| value.as_str()) != Some("token_count") {
            continue;
        }
        let Some(usage) = payload
            .get("info")
            .and_then(|info| info.get("total_token_usage"))
        else {
            continue;
        };

        let input = usage
            .get("input_tokens")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        let output = usage
            .get("output_tokens")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        let total = usage
            .get("total_tokens")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        return Some((input, output, total));
    }

    None
}

pub fn list_sessions_across_instances() -> Result<Vec<CodexSessionRecord>, String> {
    let instances = collect_instances()?;
    let process_entries = modules::process::collect_codex_process_entries();
    let mut session_map = HashMap::<String, CodexSessionRecord>::new();

    for instance in &instances {
        let running = is_instance_running(instance, &process_entries);
        for snapshot in load_thread_snapshots(instance)? {
            let entry =
                session_map
                    .entry(snapshot.id.clone())
                    .or_insert_with(|| CodexSessionRecord {
                        session_id: snapshot.id.clone(),
                        title: snapshot.title.clone(),
                        cwd: snapshot.cwd.clone(),
                        updated_at: snapshot.updated_at,
                        location_count: 0,
                        locations: Vec::new(),
                    });

            if entry.updated_at.is_none() {
                entry.updated_at = snapshot.updated_at;
            }
            if entry.title.trim().is_empty() {
                entry.title = snapshot.title.clone();
            }
            if entry.cwd.trim().is_empty() {
                entry.cwd = snapshot.cwd.clone();
            }

            entry.locations.push(CodexSessionLocation {
                instance_id: instance.id.clone(),
                instance_name: instance.name.clone(),
                running,
            });
            entry.location_count = entry.locations.len();
        }
    }

    let mut sessions = session_map.into_values().collect::<Vec<_>>();
    sessions.sort_by(|left, right| {
        right
            .updated_at
            .unwrap_or_default()
            .cmp(&left.updated_at.unwrap_or_default())
            .then_with(|| left.cwd.cmp(&right.cwd))
            .then_with(|| left.title.cmp(&right.title))
    });
    Ok(sessions)
}

pub fn list_shared_chat_catalog(
    target_instance_id: &str,
) -> Result<Vec<CodexSharedChatCatalogRecord>, String> {
    let instances = collect_instances()?;
    list_shared_chat_catalog_for_instances(&instances, target_instance_id)
}

pub fn ensure_shared_chat_visibility_for_instance(
    target_instance_id: &str,
) -> Result<CodexSharedChatVisibilitySummary, String> {
    let instances = collect_instances()?;
    ensure_shared_chat_visibility_for_instances(&instances, target_instance_id)
}

pub fn ensure_shared_chat_visibility_across_instances(
) -> Result<Vec<CodexSharedChatVisibilitySummary>, String> {
    let instances = collect_instances()?;
    let ids = instances
        .iter()
        .map(|instance| instance.id.clone())
        .collect::<Vec<_>>();
    let mut summaries = Vec::with_capacity(ids.len());
    for id in ids {
        summaries.push(ensure_shared_chat_visibility_for_instances(
            &instances, &id,
        )?);
    }
    Ok(summaries)
}

pub fn get_session_token_stats_across_instances(
    session_ids: Vec<String>,
) -> Result<Vec<CodexSessionTokenStats>, String> {
    let requested_ids = session_ids
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect::<HashSet<_>>();
    if requested_ids.is_empty() {
        return Ok(Vec::new());
    }

    let instances = collect_instances()?;
    let mut pending_ids = requested_ids.clone();
    let mut stats_by_session_id = HashMap::<String, CodexSessionTokenStats>::new();

    for instance in &instances {
        if pending_ids.is_empty() {
            break;
        }

        for snapshot in load_thread_snapshots(instance)? {
            if !pending_ids.contains(&snapshot.id) {
                continue;
            }

            let Some((input_tokens, output_tokens, total_tokens)) =
                read_token_stats_from_rollout(&snapshot.rollout_path)
            else {
                continue;
            };

            stats_by_session_id.insert(
                snapshot.id.clone(),
                CodexSessionTokenStats {
                    session_id: snapshot.id.clone(),
                    input_tokens,
                    output_tokens,
                    total_tokens,
                },
            );
            pending_ids.remove(&snapshot.id);
        }
    }

    let mut stats = stats_by_session_id.into_values().collect::<Vec<_>>();
    stats.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    Ok(stats)
}

pub fn move_sessions_to_trash_across_instances(
    session_ids: Vec<String>,
) -> Result<CodexSessionTrashSummary, String> {
    let requested_ids = session_ids
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect::<HashSet<_>>();
    if requested_ids.is_empty() {
        return Err("请至少选择一条会话".to_string());
    }

    let instances = collect_instances()?;
    let process_entries = modules::process::collect_codex_process_entries();
    let trash_root = create_trash_root_dir()?;
    let mut trashed_session_ids = HashSet::new();
    let mut trashed_instance_count = 0usize;
    let mut mutated_running_instance_count = 0usize;

    for instance in &instances {
        let snapshots = load_thread_snapshots(instance)?
            .into_iter()
            .filter(|snapshot| requested_ids.contains(&snapshot.id))
            .collect::<Vec<_>>();
        if snapshots.is_empty() {
            continue;
        }

        if is_instance_running(instance, &process_entries) {
            mutated_running_instance_count += 1;
        }

        trash_snapshots_for_instance(instance, &trash_root, &snapshots)?;
        trashed_instance_count += 1;
        for snapshot in snapshots {
            trashed_session_ids.insert(snapshot.id);
        }
    }

    if trashed_instance_count == 0 {
        return Ok(CodexSessionTrashSummary {
            requested_session_count: requested_ids.len(),
            trashed_session_count: 0,
            trashed_instance_count: 0,
            trash_dirs: Vec::new(),
            message: "所选会话在当前实例集合中不存在，无需处理".to_string(),
        });
    }

    let message = if mutated_running_instance_count > 0 {
        format!(
            "已将 {} 条会话移到废纸篓，运行中的实例可能需要重启后显示",
            trashed_session_ids.len()
        )
    } else {
        format!("已将 {} 条会话移到废纸篓", trashed_session_ids.len())
    };

    Ok(CodexSessionTrashSummary {
        requested_session_count: requested_ids.len(),
        trashed_session_count: trashed_session_ids.len(),
        trashed_instance_count,
        trash_dirs: vec![trash_root.to_string_lossy().to_string()],
        message,
    })
}

pub fn list_trashed_sessions_across_instances() -> Result<Vec<CodexTrashedSessionRecord>, String> {
    let entries = load_trash_entries()?;
    let mut session_map = HashMap::<String, CodexTrashedSessionRecord>::new();

    for entry in entries {
        let deleted_at = parse_deleted_at(entry.manifest.deleted_at.as_deref());
        let record = session_map
            .entry(entry.manifest.session_id.clone())
            .or_insert_with(|| CodexTrashedSessionRecord {
                session_id: entry.manifest.session_id.clone(),
                title: entry.manifest.title.clone(),
                cwd: entry.manifest.cwd.clone(),
                deleted_at,
                location_count: 0,
                locations: Vec::new(),
            });

        if deleted_at.unwrap_or_default() > record.deleted_at.unwrap_or_default() {
            record.deleted_at = deleted_at;
        }
        if record.title.trim().is_empty() {
            record.title = entry.manifest.title.clone();
        }
        if record.cwd.trim().is_empty() {
            record.cwd = entry.manifest.cwd.clone();
        }

        record.locations.push(CodexTrashedSessionLocation {
            instance_id: entry.manifest.instance_id.clone(),
            instance_name: entry.manifest.instance_name.clone(),
        });
        record.location_count = record.locations.len();
    }

    let mut sessions = session_map.into_values().collect::<Vec<_>>();
    sessions.sort_by(|left, right| {
        right
            .deleted_at
            .unwrap_or_default()
            .cmp(&left.deleted_at.unwrap_or_default())
            .then_with(|| left.cwd.cmp(&right.cwd))
            .then_with(|| left.title.cmp(&right.title))
    });
    Ok(sessions)
}

pub fn restore_sessions_from_trash_across_instances(
    session_ids: Vec<String>,
) -> Result<CodexSessionRestoreSummary, String> {
    let requested_ids = session_ids
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect::<HashSet<_>>();
    if requested_ids.is_empty() {
        return Err("请至少选择一条会话".to_string());
    }

    let entries = load_trash_entries()?
        .into_iter()
        .filter(|entry| requested_ids.contains(&entry.manifest.session_id))
        .collect::<Vec<_>>();

    if entries.is_empty() {
        return Ok(CodexSessionRestoreSummary {
            requested_session_count: requested_ids.len(),
            restored_session_count: 0,
            restored_instance_count: 0,
            message: "所选会话在废纸篓中不存在，无需恢复".to_string(),
        });
    }

    let instances = collect_instances()?;
    let process_entries = modules::process::collect_codex_process_entries();
    let running_instance_ids = instances
        .iter()
        .filter(|instance| is_instance_running(instance, &process_entries))
        .map(|instance| instance.id.clone())
        .collect::<HashSet<_>>();

    let mut restored_session_ids = HashSet::new();
    let mut restored_instance_ids = HashSet::new();

    for entry in &entries {
        restore_trashed_session_entry(entry)?;
        restored_session_ids.insert(entry.manifest.session_id.clone());
        restored_instance_ids.insert(entry.manifest.instance_id.clone());
    }

    let restored_running_instance = restored_instance_ids
        .iter()
        .any(|instance_id| running_instance_ids.contains(instance_id));
    let message = if restored_running_instance {
        format!(
            "已恢复 {} 条会话，运行中的实例可能需要重启后显示",
            restored_session_ids.len()
        )
    } else {
        format!("已恢复 {} 条会话", restored_session_ids.len())
    };

    Ok(CodexSessionRestoreSummary {
        requested_session_count: requested_ids.len(),
        restored_session_count: restored_session_ids.len(),
        restored_instance_count: restored_instance_ids.len(),
        message,
    })
}

fn collect_instances() -> Result<Vec<CodexSyncInstance>, String> {
    let mut instances = Vec::new();
    let default_dir = modules::codex_instance::get_default_codex_home()?;
    let store = modules::codex_instance::load_instance_store()?;
    let default_account_id =
        resolve_profile_account_id(&default_dir, store.default_settings.bind_account_id.clone());
    instances.push(CodexSyncInstance {
        id: DEFAULT_INSTANCE_ID.to_string(),
        name: DEFAULT_INSTANCE_NAME.to_string(),
        data_dir: default_dir,
        account_id: default_account_id,
        last_pid: store.default_settings.last_pid,
    });

    for instance in store.instances {
        let user_data_dir = instance.user_data_dir.trim();
        if user_data_dir.is_empty() {
            continue;
        }
        instances.push(CodexSyncInstance {
            id: instance.id,
            name: instance.name,
            data_dir: PathBuf::from(user_data_dir),
            account_id: resolve_profile_account_id(
                Path::new(user_data_dir),
                instance.bind_account_id,
            ),
            last_pid: instance.last_pid,
        });
    }

    Ok(instances)
}

fn resolve_profile_account_id(
    profile_dir: &Path,
    bound_account_id: Option<String>,
) -> Option<String> {
    read_profile_account_id(profile_dir).or_else(|| {
        bound_account_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn read_profile_account_id(profile_dir: &Path) -> Option<String> {
    for relative_path in [
        ".cockpit_codex_auth.json",
        "electron-user-data/.cockpit_codex_electron_auth.json",
    ] {
        let path = profile_dir.join(relative_path);
        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<JsonValue>(&content) else {
            continue;
        };
        let Some(account_id) = parsed
            .get("account_id")
            .or_else(|| parsed.get("accountId"))
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        return Some(account_id.to_string());
    }
    None
}

fn is_instance_running(
    instance: &CodexSyncInstance,
    process_entries: &[(u32, Option<String>)],
) -> bool {
    let codex_home = if instance.id == DEFAULT_INSTANCE_ID {
        None
    } else {
        instance.data_dir.to_str()
    };
    modules::process::resolve_codex_pid_from_entries(instance.last_pid, codex_home, process_entries)
        .is_some()
}

fn list_shared_chat_catalog_for_instances(
    instances: &[CodexSyncInstance],
    target_instance_id: &str,
) -> Result<Vec<CodexSharedChatCatalogRecord>, String> {
    let target = find_instance(instances, target_instance_id)?;
    let mappings = load_shared_chat_catalog(&target.data_dir)?;
    let mapping_by_key = mappings
        .mappings
        .iter()
        .map(|mapping| (mapping.key.clone(), mapping))
        .collect::<HashMap<_, _>>();
    let owners = collect_owned_thread_snapshots(instances)?;
    let mut records = Vec::with_capacity(owners.len());

    for owner in owners {
        let key = shared_chat_mapping_key(&owner.instance, &owner.snapshot);
        let mapping = mapping_by_key.get(&key);
        records.push(CodexSharedChatCatalogRecord {
            source_session_id: owner.snapshot.id.clone(),
            title: owner.snapshot.title.clone(),
            cwd: owner.snapshot.cwd.clone(),
            updated_at: owner.snapshot.updated_at,
            archived: owner.snapshot.archived,
            foreign: accounts_are_foreign(&owner.instance, target),
            materialized: mapping.is_some(),
            local_session_id: mapping.map(|item| item.local_session_id.clone()),
            source_instance_id: owner.instance.id.clone(),
            source_instance_name: owner.instance.name.clone(),
            source_account_id: owner.instance.account_id.clone(),
            source_codex_home: owner.instance.data_dir.to_string_lossy().to_string(),
            source_rollout_path: owner.snapshot.rollout_path.to_string_lossy().to_string(),
        });
    }

    records.sort_by(|left, right| {
        right
            .updated_at
            .unwrap_or_default()
            .cmp(&left.updated_at.unwrap_or_default())
            .then_with(|| left.source_instance_name.cmp(&right.source_instance_name))
            .then_with(|| left.title.cmp(&right.title))
    });
    Ok(records)
}

fn ensure_shared_chat_visibility_for_instances(
    instances: &[CodexSyncInstance],
    target_instance_id: &str,
) -> Result<CodexSharedChatVisibilitySummary, String> {
    let target = find_instance(instances, target_instance_id)?.clone();
    if instance_uses_shared_canonical_history(instances, &target) {
        return Ok(CodexSharedChatVisibilitySummary {
            target_instance_id: target.id,
            target_instance_name: target.name,
            materialized_foreign_count: 0,
            copied_same_account_count: 0,
            removed_unsafe_same_id_count: 0,
            backup_dir: None,
            message: "Shared chat visibility skipped: instance uses canonical shared history"
                .to_string(),
        });
    }

    promote_materialized_shared_chat_updates_to_sources(instances)?;
    if !target.data_dir.join(STATE_DB_FILE).exists() {
        return Ok(CodexSharedChatVisibilitySummary {
            target_instance_id: target.id,
            target_instance_name: target.name,
            materialized_foreign_count: 0,
            copied_same_account_count: 0,
            removed_unsafe_same_id_count: 0,
            backup_dir: None,
            message: "Shared chat visibility skipped: target state database is not initialized"
                .to_string(),
        });
    }
    let owners = collect_owned_thread_snapshots(instances)?;
    let owner_by_id = owners
        .iter()
        .map(|owner| (owner.snapshot.id.clone(), owner))
        .collect::<HashMap<_, _>>();
    let target_snapshots = load_thread_snapshots(&target)?;
    let mut unsafe_snapshots = Vec::new();
    let mut unsafe_snapshot_ids = HashSet::new();

    for snapshot in &target_snapshots {
        let rollout_missing = !snapshot.rollout_path.exists();
        let shadows_other_owner = owner_by_id
            .get(&snapshot.id)
            .map(|owner| owner.instance.id != target.id)
            .unwrap_or(false);
        let unsafe_same_id = owner_by_id
            .get(&snapshot.id)
            .map(|owner| {
                owner.instance.id != target.id && accounts_are_foreign(&owner.instance, &target)
            })
            .unwrap_or(false);
        let unsafe_missing_materialized = rollout_missing && snapshot.materialized_foreign;
        let unsafe_missing_shadow = rollout_missing && shadows_other_owner;
        let unsafe_missing_local_history = rollout_missing
            && rollout_path_is_declared_local_history(&target.data_dir, &snapshot.rollout_path);

        if (unsafe_same_id
            || unsafe_missing_materialized
            || unsafe_missing_shadow
            || unsafe_missing_local_history)
            && unsafe_snapshot_ids.insert(snapshot.id.clone())
        {
            unsafe_snapshots.push(snapshot.clone());
        }
    }

    let mut catalog = load_shared_chat_catalog(&target.data_dir)?;
    let mut backup_dir = if unsafe_snapshots.is_empty() {
        None
    } else {
        Some(backup_shared_chat_target_files(
            &target.data_dir,
            &unsafe_snapshots,
        )?)
    };
    let mut removed_unsafe_same_id_count = 0usize;
    for snapshot in &unsafe_snapshots {
        remove_unsafe_same_id_snapshot(&target, snapshot, backup_dir.as_deref())?;
        removed_unsafe_same_id_count += 1;
    }

    let mut current_snapshots = load_thread_snapshots(&target)?;
    let legacy_thread_source_snapshots = current_snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.row_data.get_text("thread_source").as_deref()
                == Some(SHARED_CHAT_THREAD_SOURCE)
                && session_index_entry_is_materialized_foreign(
                    &snapshot.session_index_entry,
                    &target,
                    &snapshot.id,
                )
        })
        .cloned()
        .collect::<Vec<_>>();
    if !legacy_thread_source_snapshots.is_empty() {
        if backup_dir.is_none() {
            backup_dir = Some(backup_shared_chat_target_files(&target.data_dir, &[])?);
        }
        clear_legacy_shared_thread_sources(&target, &legacy_thread_source_snapshots)?;
        current_snapshots = load_thread_snapshots(&target)?;
    }
    let mut current_ids = current_snapshots
        .iter()
        .map(|snapshot| snapshot.id.clone())
        .collect::<HashSet<_>>();
    let mut materialized_foreign_count = 0usize;
    let mut copied_same_account_count = 0usize;
    let mut foreign_materialization_changed = false;

    for owner in owners {
        if owner.instance.id == target.id {
            continue;
        }

        if accounts_match(&owner.instance.account_id, &target.account_id) {
            if let Some(local_snapshot) = current_snapshots
                .iter()
                .find(|snapshot| snapshot.id == owner.snapshot.id)
            {
                if snapshot_content_is_newer(&owner.snapshot, local_snapshot) {
                    if backup_dir.is_none() {
                        backup_dir = Some(backup_shared_chat_target_files(&target.data_dir, &[])?);
                    }
                    copy_snapshot_to_target(&target, &owner, &owner.snapshot.id, None)?;
                    copied_same_account_count += 1;
                    current_snapshots = load_thread_snapshots(&target)?;
                }
                current_ids.insert(owner.snapshot.id.clone());
                continue;
            }
            if backup_dir.is_none() {
                backup_dir = Some(backup_shared_chat_target_files(&target.data_dir, &[])?);
            }
            copy_snapshot_to_target(&target, &owner, &owner.snapshot.id, None)?;
            current_ids.insert(owner.snapshot.id.clone());
            copied_same_account_count += 1;
            continue;
        }

        if !foreign_snapshot_needs_materialization(&owner, &catalog, &current_snapshots) {
            continue;
        }
        if backup_dir.is_none() {
            backup_dir = Some(backup_shared_chat_target_files(&target.data_dir, &[])?);
        }
        let (local_id, materialization_changed) = ensure_foreign_snapshot_materialized(
            &target,
            &owner,
            &mut catalog,
            &current_snapshots,
        )?;
        if materialization_changed {
            foreign_materialization_changed = true;
            current_snapshots = load_thread_snapshots(&target)?;
        }
        if current_ids.insert(local_id) {
            materialized_foreign_count += 1;
        }
    }

    if materialized_foreign_count > 0
        || copied_same_account_count > 0
        || removed_unsafe_same_id_count > 0
        || foreign_materialization_changed
    {
        save_shared_chat_catalog(&target.data_dir, &mut catalog)?;
    }

    let message = format!(
        "Shared chat visibility updated: {} foreign fork(s), {} same-account copy/copies, {} unsafe same-id record(s) removed",
        materialized_foreign_count, copied_same_account_count, removed_unsafe_same_id_count
    );

    Ok(CodexSharedChatVisibilitySummary {
        target_instance_id: target.id,
        target_instance_name: target.name,
        materialized_foreign_count,
        copied_same_account_count,
        removed_unsafe_same_id_count,
        backup_dir: backup_dir.map(|path| path.to_string_lossy().to_string()),
        message,
    })
}

fn paths_point_to_same_location(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(left), Ok(right)) => left == right,
        _ => a == b,
    }
}

fn instance_uses_shared_canonical_history(
    instances: &[CodexSyncInstance],
    target: &CodexSyncInstance,
) -> bool {
    let target_db = target.data_dir.join(STATE_DB_FILE);
    if !target_db.exists() {
        return false;
    }

    instances
        .iter()
        .filter(|instance| instance.id != target.id)
        .map(|instance| instance.data_dir.join(STATE_DB_FILE))
        .any(|other_db| other_db.exists() && paths_point_to_same_location(&target_db, &other_db))
}

fn find_instance<'a>(
    instances: &'a [CodexSyncInstance],
    target_instance_id: &str,
) -> Result<&'a CodexSyncInstance, String> {
    instances
        .iter()
        .find(|instance| instance.id == target_instance_id)
        .ok_or_else(|| format!("Codex instance not found: {}", target_instance_id))
}

fn accounts_match(left: &Option<String>, right: &Option<String>) -> bool {
    match (left.as_deref(), right.as_deref()) {
        (Some(left), Some(right)) => {
            !left.trim().is_empty() && left.trim().eq_ignore_ascii_case(right.trim())
        }
        _ => false,
    }
}

fn accounts_are_foreign(source: &CodexSyncInstance, target: &CodexSyncInstance) -> bool {
    source.id != target.id && !accounts_match(&source.account_id, &target.account_id)
}

fn promote_materialized_shared_chat_updates_to_sources(
    instances: &[CodexSyncInstance],
) -> Result<usize, String> {
    let mut snapshots_by_instance = HashMap::<String, Vec<ThreadSnapshot>>::new();
    for instance in instances {
        snapshots_by_instance.insert(instance.id.clone(), load_thread_snapshots(instance)?);
    }

    let instance_by_id = instances
        .iter()
        .map(|instance| (instance.id.as_str(), instance))
        .collect::<HashMap<_, _>>();
    let catalogs_by_instance = instances
        .iter()
        .map(|instance| {
            let catalog = load_shared_chat_catalog(&instance.data_dir).unwrap_or_default();
            (instance.id.clone(), catalog)
        })
        .collect::<HashMap<_, _>>();
    let mut source_by_key = HashMap::<String, OwnedThreadSnapshot>::new();
    for instance in instances {
        let Some(snapshots) = snapshots_by_instance.get(&instance.id) else {
            continue;
        };
        for snapshot in snapshots {
            if snapshot.materialized_foreign || !snapshot.rollout_path.exists() {
                continue;
            }
            source_by_key.insert(
                shared_chat_mapping_key(instance, snapshot),
                OwnedThreadSnapshot {
                    instance: instance.clone(),
                    snapshot: snapshot.clone(),
                },
            );
        }
    }

    let mut newest_materialized_by_key =
        HashMap::<String, (SharedChatSourceIdentity, OwnedThreadSnapshot)>::new();
    for instance in instances {
        let Some(snapshots) = snapshots_by_instance.get(&instance.id) else {
            continue;
        };
        for snapshot in snapshots {
            if !snapshot.materialized_foreign || !snapshot.rollout_path.exists() {
                continue;
            }
            let Some(identity) = shared_chat_source_identity_from_snapshot(snapshot) else {
                continue;
            };
            if !instance_by_id.contains_key(identity.source_instance_id.as_str()) {
                continue;
            }
            let key = identity.key();
            let candidate = OwnedThreadSnapshot {
                instance: instance.clone(),
                snapshot: snapshot.clone(),
            };
            let replace = newest_materialized_by_key
                .get(&key)
                .map(|(_, current)| {
                    snapshot_content_is_newer(&candidate.snapshot, &current.snapshot)
                })
                .unwrap_or(true);
            if replace {
                newest_materialized_by_key.insert(key, (identity, candidate));
            }
        }
    }

    let mut promoted = 0usize;
    for (key, (identity, candidate)) in newest_materialized_by_key {
        if !source_by_key.contains_key(&key) {
            continue;
        }
        if !materialized_snapshot_has_local_updates(&catalogs_by_instance, &identity, &candidate) {
            continue;
        }
        let Some(source_instance) = instance_by_id.get(identity.source_instance_id.as_str()) else {
            continue;
        };
        copy_snapshot_to_target(
            source_instance,
            &candidate,
            &identity.source_session_id,
            None,
        )?;
        promoted += 1;
    }

    Ok(promoted)
}

fn materialized_snapshot_has_local_updates(
    catalogs_by_instance: &HashMap<String, SharedChatCatalogFile>,
    identity: &SharedChatSourceIdentity,
    candidate: &OwnedThreadSnapshot,
) -> bool {
    let Some(catalog) = catalogs_by_instance.get(&candidate.instance.id) else {
        return false;
    };
    let key = identity.key();
    let Some(mapping) = catalog
        .mappings
        .iter()
        .find(|mapping| mapping.key == key && mapping.local_session_id == candidate.snapshot.id)
    else {
        return false;
    };

    let candidate_updated_at = candidate.snapshot.updated_at.unwrap_or_default();
    let mapped_updated_at = mapping.local_materialized_updated_at.unwrap_or_default();

    candidate_updated_at > mapped_updated_at
        || mapping.local_materialized_rollout_len != candidate.snapshot.rollout_len
        || mapping.local_materialized_rollout_modified_at_ms
            != candidate.snapshot.rollout_modified_at_ms
}

fn snapshot_content_is_newer(candidate: &ThreadSnapshot, current: &ThreadSnapshot) -> bool {
    let candidate_updated_at = candidate.updated_at.unwrap_or_default();
    let current_updated_at = current.updated_at.unwrap_or_default();
    if candidate_updated_at != current_updated_at {
        return candidate_updated_at > current_updated_at;
    }

    match (candidate.rollout_len, current.rollout_len) {
        (Some(candidate_len), Some(current_len)) if candidate_len != current_len => {
            return candidate_len > current_len;
        }
        (Some(_), None) => return true,
        _ => {}
    }

    false
}

fn shared_chat_source_identity_from_snapshot(
    snapshot: &ThreadSnapshot,
) -> Option<SharedChatSourceIdentity> {
    let metadata = snapshot.session_index_entry.get("cockpit_shared_chat")?;
    let source_instance_id = metadata
        .get("source_instance_id")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let source_session_id = metadata
        .get("source_session_id")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let source_account_id = metadata
        .get("source_account_id")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    Some(SharedChatSourceIdentity {
        source_account_id,
        source_instance_id,
        source_session_id,
        archived: snapshot.archived,
    })
}

fn collect_owned_thread_snapshots(
    instances: &[CodexSyncInstance],
) -> Result<Vec<OwnedThreadSnapshot>, String> {
    let mut owners_by_id = HashMap::<String, OwnedThreadSnapshot>::new();
    for instance in instances {
        for snapshot in load_thread_snapshots(instance)? {
            if snapshot.materialized_foreign || !snapshot.rollout_path.exists() {
                continue;
            }
            let candidate = OwnedThreadSnapshot {
                instance: instance.clone(),
                snapshot,
            };
            match owners_by_id.entry(candidate.snapshot.id.clone()) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if snapshot_content_is_newer(&candidate.snapshot, &entry.get().snapshot) {
                        entry.insert(candidate);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
            }
        }
    }
    let mut owners = owners_by_id.into_values().collect::<Vec<_>>();
    owners.sort_by(|left, right| {
        right
            .snapshot
            .updated_at
            .unwrap_or_default()
            .cmp(&left.snapshot.updated_at.unwrap_or_default())
            .then_with(|| left.instance.id.cmp(&right.instance.id))
            .then_with(|| left.snapshot.id.cmp(&right.snapshot.id))
    });
    Ok(owners)
}

fn shared_chat_mapping_key(source: &CodexSyncInstance, snapshot: &ThreadSnapshot) -> String {
    shared_chat_mapping_key_from_parts(
        source.account_id.as_deref(),
        &source.id,
        &snapshot.id,
        snapshot.archived,
    )
}

fn shared_chat_mapping_key_from_parts(
    source_account_id: Option<&str>,
    source_instance_id: &str,
    source_session_id: &str,
    archived: bool,
) -> String {
    format!(
        "{}::{}::{}::{}",
        source_account_id.unwrap_or(""),
        source_instance_id,
        source_session_id,
        if archived { "archived" } else { "active" }
    )
}

fn load_shared_chat_catalog(root_dir: &Path) -> Result<SharedChatCatalogFile, String> {
    let path = root_dir.join(SHARED_CHAT_CATALOG_FILE);
    if !path.exists() {
        return Ok(SharedChatCatalogFile::default());
    }
    let content = fs::read_to_string(&path).map_err(|error| {
        format!(
            "read shared chat catalog failed ({}): {}",
            path.display(),
            error
        )
    })?;
    serde_json::from_str::<SharedChatCatalogFile>(&content).map_err(|error| {
        format!(
            "parse shared chat catalog failed ({}): {}",
            path.display(),
            error
        )
    })
}

fn save_shared_chat_catalog(
    root_dir: &Path,
    catalog: &mut SharedChatCatalogFile,
) -> Result<(), String> {
    catalog.updated_at = Utc::now().to_rfc3339();
    catalog
        .mappings
        .sort_by(|left, right| left.key.cmp(&right.key));
    let path = root_dir.join(SHARED_CHAT_CATALOG_FILE);
    fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(catalog)
                .map_err(|error| format!("serialize shared chat catalog failed: {}", error))?
        ),
    )
    .map_err(|error| {
        format!(
            "write shared chat catalog failed ({}): {}",
            path.display(),
            error
        )
    })
}

fn backup_shared_chat_target_files(
    target_root: &Path,
    dangerous_snapshots: &[ThreadSnapshot],
) -> Result<PathBuf, String> {
    let backup_dir = target_root.join(format!(
        "backup-{}-shared-chat-visibility",
        Utc::now().format("%Y%m%d-%H%M%S")
    ));
    fs::create_dir_all(&backup_dir).map_err(|error| {
        format!(
            "create shared chat backup directory failed ({}): {}",
            backup_dir.display(),
            error
        )
    })?;

    for file_name in [
        STATE_DB_FILE,
        SESSION_INDEX_FILE,
        GLOBAL_STATE_FILE,
        SHARED_CHAT_CATALOG_FILE,
    ] {
        let source = target_root.join(file_name);
        if !source.exists() {
            continue;
        }
        fs::copy(&source, backup_dir.join(format!("{}.bak", file_name))).map_err(|error| {
            format!(
                "backup shared chat file failed ({}): {}",
                source.display(),
                error
            )
        })?;
    }

    for snapshot in dangerous_snapshots {
        if !rollout_path_is_local_to_root(target_root, &snapshot.rollout_path) {
            continue;
        }
        let relative = snapshot
            .rollout_path
            .strip_prefix(target_root)
            .unwrap_or(snapshot.rollout_path.as_path());
        let backup_path = backup_dir.join("files").join(relative);
        if let Some(parent) = backup_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "create shared chat rollout backup parent failed ({}): {}",
                    parent.display(),
                    error
                )
            })?;
        }
        fs::copy(&snapshot.rollout_path, &backup_path).map_err(|error| {
            format!(
                "backup unsafe rollout failed ({} -> {}): {}",
                snapshot.rollout_path.display(),
                backup_path.display(),
                error
            )
        })?;
    }

    Ok(backup_dir)
}

fn rollout_path_is_local_to_root(root: &Path, rollout_path: &Path) -> bool {
    if !rollout_path.exists() {
        return false;
    }
    match (fs::canonicalize(root), fs::canonicalize(rollout_path)) {
        (Ok(root), Ok(rollout)) => rollout.starts_with(root),
        _ => rollout_path.starts_with(root),
    }
}

fn rollout_path_is_declared_local_history(root: &Path, rollout_path: &Path) -> bool {
    let Ok(relative) = rollout_path.strip_prefix(root) else {
        return false;
    };
    relative
        .components()
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .map(|component| component == "sessions" || component == "archived_sessions")
        .unwrap_or(false)
}

fn remove_unsafe_same_id_snapshot(
    target: &CodexSyncInstance,
    snapshot: &ThreadSnapshot,
    backup_dir: Option<&Path>,
) -> Result<(), String> {
    if rollout_path_is_local_to_root(&target.data_dir, &snapshot.rollout_path)
        && snapshot.rollout_path.exists()
    {
        if let Some(backup_dir) = backup_dir {
            let relative = snapshot
                .rollout_path
                .strip_prefix(&target.data_dir)
                .unwrap_or(snapshot.rollout_path.as_path());
            let moved_path = backup_dir.join("removed-unsafe").join(relative);
            if let Some(parent) = moved_path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    format!(
                        "create unsafe rollout move parent failed ({}): {}",
                        parent.display(),
                        error
                    )
                })?;
            }
            fs::rename(&snapshot.rollout_path, &moved_path).map_err(|error| {
                format!(
                    "move unsafe same-id rollout to backup failed ({} -> {}): {}",
                    snapshot.rollout_path.display(),
                    moved_path.display(),
                    error
                )
            })?;
        }
    }

    remove_threads_from_db(&target.data_dir, std::slice::from_ref(snapshot))?;
    rewrite_session_index_without_ids(&target.data_dir, std::slice::from_ref(snapshot))
}

fn ensure_foreign_snapshot_materialized(
    target: &CodexSyncInstance,
    owner: &OwnedThreadSnapshot,
    catalog: &mut SharedChatCatalogFile,
    current_snapshots: &[ThreadSnapshot],
) -> Result<(String, bool), String> {
    let key = shared_chat_mapping_key(&owner.instance, &owner.snapshot);
    let current_snapshot_by_id = current_snapshots
        .iter()
        .map(|snapshot| (snapshot.id.as_str(), snapshot))
        .collect::<HashMap<_, _>>();
    let current_ids = current_snapshot_by_id
        .keys()
        .copied()
        .collect::<HashSet<_>>();

    if let Some(mapping) = catalog.mappings.iter().find(|mapping| mapping.key == key) {
        if let Some(local_snapshot) = current_snapshot_by_id.get(mapping.local_session_id.as_str())
        {
            if local_snapshot.rollout_path.exists()
                && !shared_chat_mapping_needs_refresh(mapping, owner, local_snapshot)
            {
                return Ok((mapping.local_session_id.clone(), false));
            }
        }
    }

    let existing_local_id = catalog
        .mappings
        .iter()
        .find(|mapping| mapping.key == key)
        .map(|mapping| mapping.local_session_id.as_str());
    let mut local_id = existing_local_id
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    while local_id == owner.snapshot.id
        || (current_ids.contains(local_id.as_str()) && Some(local_id.as_str()) != existing_local_id)
    {
        local_id = Uuid::new_v4().to_string();
    }

    let metadata = json!({
        "source_instance_id": owner.instance.id,
        "source_instance_name": owner.instance.name,
        "source_account_id": owner.instance.account_id,
        "source_codex_home": owner.instance.data_dir,
        "source_session_id": owner.snapshot.id,
        "source_rollout_path": owner.snapshot.rollout_path,
        "target_instance_id": target.id,
        "target_account_id": target.account_id,
        "forked_at": Utc::now().to_rfc3339(),
    });
    let target_rollout_path = copy_snapshot_to_target(target, owner, &local_id, Some(&metadata))?;
    let (local_rollout_len, local_rollout_modified_at_ms) =
        rollout_file_fingerprint(&target_rollout_path);

    let mapping = SharedChatMapping {
        key: key.clone(),
        source_instance_id: owner.instance.id.clone(),
        source_account_id: owner.instance.account_id.clone(),
        source_codex_home: owner.instance.data_dir.to_string_lossy().to_string(),
        source_session_id: owner.snapshot.id.clone(),
        source_rollout_path: owner.snapshot.rollout_path.to_string_lossy().to_string(),
        local_instance_id: target.id.clone(),
        local_account_id: target.account_id.clone(),
        local_session_id: local_id.clone(),
        local_rollout_path: target_rollout_path.to_string_lossy().to_string(),
        archived: owner.snapshot.archived,
        last_source_updated_at: owner.snapshot.updated_at,
        last_source_rollout_len: owner.snapshot.rollout_len,
        last_source_rollout_modified_at_ms: owner.snapshot.rollout_modified_at_ms,
        local_materialized_updated_at: owner.snapshot.updated_at,
        local_materialized_rollout_len: local_rollout_len,
        local_materialized_rollout_modified_at_ms: local_rollout_modified_at_ms,
    };

    if let Some(slot) = catalog
        .mappings
        .iter_mut()
        .find(|mapping| mapping.key == key)
    {
        *slot = mapping;
    } else {
        catalog.mappings.push(mapping);
    }

    Ok((local_id, true))
}

fn foreign_snapshot_needs_materialization(
    owner: &OwnedThreadSnapshot,
    catalog: &SharedChatCatalogFile,
    current_snapshots: &[ThreadSnapshot],
) -> bool {
    let key = shared_chat_mapping_key(&owner.instance, &owner.snapshot);
    let Some(mapping) = catalog.mappings.iter().find(|mapping| mapping.key == key) else {
        return true;
    };
    let Some(local_snapshot) = current_snapshots
        .iter()
        .find(|snapshot| snapshot.id == mapping.local_session_id && snapshot.rollout_path.exists())
    else {
        return true;
    };

    shared_chat_mapping_needs_refresh(mapping, owner, local_snapshot)
}

fn shared_chat_mapping_needs_refresh(
    mapping: &SharedChatMapping,
    owner: &OwnedThreadSnapshot,
    local_snapshot: &ThreadSnapshot,
) -> bool {
    let source_updated_at = owner.snapshot.updated_at.unwrap_or_default();
    let mapped_source_updated_at = mapping.last_source_updated_at.unwrap_or_default();
    let mapped_local_updated_at = mapping.local_materialized_updated_at.unwrap_or_default();
    let source_rollout_path = owner.snapshot.rollout_path.to_string_lossy().to_string();
    let local_rollout_path = local_snapshot.rollout_path.to_string_lossy().to_string();
    let source_rollout_len_changed = mapping.last_source_rollout_len != owner.snapshot.rollout_len;
    let source_rollout_modified_at_changed =
        mapping.last_source_rollout_modified_at_ms != owner.snapshot.rollout_modified_at_ms;

    mapped_source_updated_at < source_updated_at
        || mapped_local_updated_at < source_updated_at
        || source_rollout_len_changed
        || source_rollout_modified_at_changed
        || mapping.source_rollout_path != source_rollout_path
        || mapping.local_rollout_path != local_rollout_path
        || mapping.archived != owner.snapshot.archived
}

fn copy_snapshot_to_target(
    target: &CodexSyncInstance,
    owner: &OwnedThreadSnapshot,
    local_id: &str,
    shared_metadata: Option<&JsonValue>,
) -> Result<PathBuf, String> {
    let target_rollout_path =
        target_rollout_path_for_snapshot(&target.data_dir, &owner.snapshot, local_id);
    if let Some(parent) = target_rollout_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "create target rollout parent failed ({}): {}",
                parent.display(),
                error
            )
        })?;
    }
    let sanitize_resume_ids = !accounts_match(&owner.instance.account_id, &target.account_id);
    let rewritten = rewrite_rollout_for_local_session(
        &owner.snapshot.rollout_path,
        local_id,
        shared_metadata,
        sanitize_resume_ids,
    )?;
    fs::write(&target_rollout_path, rewritten).map_err(|error| {
        format!(
            "write target rollout failed ({}): {}",
            target_rollout_path.display(),
            error
        )
    })?;

    let mut row_data = owner.snapshot.row_data.clone();
    row_data.set_text("id", local_id.to_string());
    row_data.set_text(
        "rollout_path",
        target_rollout_path.to_string_lossy().to_string(),
    );
    row_data.set_null("thread_source");
    upsert_thread_row(&target.data_dir, &row_data)?;

    let mut index_entry = owner.snapshot.session_index_entry.clone();
    index_entry["id"] = JsonValue::String(local_id.to_string());
    if let Some(metadata) = shared_metadata {
        index_entry["cockpit_shared_chat"] = metadata.clone();
    } else if let Some(object) = index_entry.as_object_mut() {
        object.remove("cockpit_shared_chat");
    }
    upsert_session_index_entry(&target.data_dir, local_id, &index_entry)?;
    Ok(target_rollout_path)
}

fn clear_legacy_shared_thread_sources(
    target: &CodexSyncInstance,
    snapshots: &[ThreadSnapshot],
) -> Result<usize, String> {
    let mut cleared = 0usize;
    for snapshot in snapshots {
        let mut row_data = snapshot.row_data.clone();
        row_data.set_null("thread_source");
        upsert_thread_row(&target.data_dir, &row_data)?;
        cleared += 1;
    }
    Ok(cleared)
}

fn target_rollout_path_for_snapshot(
    target_root: &Path,
    snapshot: &ThreadSnapshot,
    local_id: &str,
) -> PathBuf {
    let mut relative = relative_rollout_path(snapshot);
    let next_file_name = rollout_file_name_for_local_id(&relative, local_id);
    relative.set_file_name(next_file_name);
    target_root.join(relative)
}

fn relative_rollout_path(snapshot: &ThreadSnapshot) -> PathBuf {
    if let Ok(relative) = snapshot.rollout_path.strip_prefix(&snapshot.source_root) {
        return relative.to_path_buf();
    }

    let components = snapshot
        .rollout_path
        .components()
        .map(|component| component.as_os_str().to_os_string())
        .collect::<Vec<_>>();
    if let Some(index) = components
        .iter()
        .position(|component| component == "sessions" || component == "archived_sessions")
    {
        let mut relative = PathBuf::new();
        for component in components.into_iter().skip(index) {
            relative.push(component);
        }
        return relative;
    }

    let dir_name = if snapshot.archived {
        "archived_sessions"
    } else {
        "sessions"
    };
    PathBuf::from(dir_name).join(
        snapshot
            .rollout_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("rollout.jsonl")),
    )
}

fn rollout_file_name_for_local_id(relative_path: &Path, local_id: &str) -> String {
    let file_name = relative_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if let Some(stem) = file_name
        .strip_prefix("rollout-")
        .and_then(|value| value.strip_suffix(".jsonl"))
    {
        if stem.len() > 37 {
            let prefix_len = stem.len() - 36;
            let prefix = &stem[..prefix_len];
            return format!("rollout-{}{}.jsonl", prefix, local_id);
        }
    }
    format!(
        "rollout-{}-{}.jsonl",
        Utc::now().format("%Y-%m-%dT%H-%M-%S"),
        local_id
    )
}

fn rewrite_rollout_for_local_session(
    source_rollout_path: &Path,
    local_id: &str,
    shared_metadata: Option<&JsonValue>,
    sanitize_resume_ids: bool,
) -> Result<Vec<u8>, String> {
    let bytes = fs::read(source_rollout_path).map_err(|error| {
        format!(
            "read source rollout failed ({}): {}",
            source_rollout_path.display(),
            error
        )
    })?;
    let (line_end, separator_len) = first_line_bounds(&bytes);
    let first_line = String::from_utf8(bytes[..line_end].to_vec()).map_err(|error| {
        format!(
            "decode source rollout first line failed ({}): {}",
            source_rollout_path.display(),
            error
        )
    })?;
    let mut parsed = serde_json::from_str::<JsonValue>(&first_line).unwrap_or_else(|_| json!({}));
    if parsed.get("type").and_then(JsonValue::as_str) == Some("session_meta") {
        if let Some(payload) = parsed.get_mut("payload").and_then(JsonValue::as_object_mut) {
            payload.insert("id".to_string(), JsonValue::String(local_id.to_string()));
            if let Some(metadata) = shared_metadata {
                payload.insert("cockpit_shared_chat".to_string(), metadata.clone());
            } else {
                payload.remove("cockpit_shared_chat");
            }
        }
    }

    let mut result = Vec::with_capacity(bytes.len() + 256);
    result.extend_from_slice(
        serde_json::to_string(&parsed)
            .map_err(|error| format!("serialize rewritten rollout first line failed: {}", error))?
            .as_bytes(),
    );
    if line_end + separator_len <= bytes.len() {
        result.extend_from_slice(&bytes[line_end..line_end + separator_len]);
        let tail = &bytes[line_end + separator_len..];
        if sanitize_resume_ids {
            result.extend_from_slice(&rewrite_shared_chat_rollout_tail(tail)?);
        } else {
            result.extend_from_slice(tail);
        }
    } else {
        result.push(b'\n');
    }
    Ok(result)
}

fn rewrite_shared_chat_rollout_tail(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut result = Vec::with_capacity(bytes.len());
    let mut offset = 0usize;

    while offset < bytes.len() {
        let line_start = offset;
        while offset < bytes.len() && bytes[offset] != b'\n' {
            offset += 1;
        }
        let has_newline = offset < bytes.len();
        let mut line_end = offset;
        let separator = if has_newline {
            if line_end > line_start && bytes[line_end - 1] == b'\r' {
                line_end -= 1;
                b"\r\n".as_slice()
            } else {
                b"\n".as_slice()
            }
        } else {
            b"".as_slice()
        };
        let line = &bytes[line_start..line_end];

        if line.is_empty() {
            result.extend_from_slice(line);
        } else {
            let mut parsed = match serde_json::from_slice::<JsonValue>(line) {
                Ok(value) => value,
                Err(_) => {
                    result.extend_from_slice(line);
                    result.extend_from_slice(separator);
                    if has_newline {
                        offset += 1;
                    }
                    continue;
                }
            };
            remove_upstream_resume_fields(&mut parsed);
            result.extend_from_slice(
                serde_json::to_string(&parsed)
                    .map_err(|error| {
                        format!(
                            "serialize sanitized shared chat rollout line failed: {}",
                            error
                        )
                    })?
                    .as_bytes(),
            );
        }
        result.extend_from_slice(separator);
        if has_newline {
            offset += 1;
        }
    }

    Ok(result)
}

fn remove_upstream_resume_fields(value: &mut JsonValue) {
    match value {
        JsonValue::Object(map) => {
            map.remove("previous_response_id");
            map.remove("previousResponseId");
            for child in map.values_mut() {
                remove_upstream_resume_fields(child);
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                remove_upstream_resume_fields(item);
            }
        }
        _ => {}
    }
}

fn first_line_bounds(bytes: &[u8]) -> (usize, usize) {
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            if index > 0 && bytes[index - 1] == b'\r' {
                return (index - 1, 2);
            }
            return (index, 1);
        }
    }
    (bytes.len(), 0)
}

fn upsert_thread_row(root_dir: &Path, row_data: &ThreadRowData) -> Result<(), String> {
    let db_path = root_dir.join(STATE_DB_FILE);
    let connection = Connection::open(&db_path).map_err(|error| {
        format!(
            "open target state database failed ({}): {}",
            db_path.display(),
            error
        )
    })?;
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| format!("set target database busy_timeout failed: {}", error))?;
    let target_columns = read_thread_columns(&connection)?;
    let db_column_set = target_columns.into_iter().collect::<HashSet<_>>();
    let insert_columns = row_data
        .columns
        .iter()
        .filter(|column| db_column_set.contains(*column))
        .cloned()
        .collect::<Vec<_>>();
    if insert_columns.is_empty() {
        return Err("target threads table has no writable columns".to_string());
    }
    let insert_values = insert_columns
        .iter()
        .map(|column| row_data.get_value(column).cloned().unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    let placeholders = vec!["?"; insert_columns.len()].join(", ");
    let sql = format!(
        "INSERT OR REPLACE INTO threads ({}) VALUES ({})",
        insert_columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders
    );
    connection
        .execute(&sql, params_from_iter(insert_values.iter()))
        .map_err(|error| format!("upsert target thread row failed: {}", error))?;
    Ok(())
}

fn upsert_session_index_entry(
    root_dir: &Path,
    session_id: &str,
    entry: &JsonValue,
) -> Result<(), String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    let mut lines = if path.exists() {
        fs::read_to_string(&path)
            .map_err(|error| {
                format!(
                    "read session_index.jsonl failed ({}): {}",
                    path.display(),
                    error
                )
            })?
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return None;
                }
                let parsed = serde_json::from_str::<JsonValue>(trimmed).ok()?;
                if parsed.get("id").and_then(JsonValue::as_str) == Some(session_id) {
                    return None;
                }
                Some(trimmed.to_string())
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    lines.push(
        serde_json::to_string(entry)
            .map_err(|error| format!("serialize session_index entry failed: {}", error))?,
    );
    fs::write(&path, format!("{}\n", lines.join("\n"))).map_err(|error| {
        format!(
            "write session_index.jsonl failed ({}): {}",
            path.display(),
            error
        )
    })
}

fn load_thread_snapshots(instance: &CodexSyncInstance) -> Result<Vec<ThreadSnapshot>, String> {
    let db_path = instance.data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let connection = match open_readonly_connection(&db_path) {
        Ok(connection) => connection,
        Err(error) if should_skip_state_db_message(&error) => {
            log_skipped_state_db(&instance.name, &db_path, &error);
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    let columns = match read_thread_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if should_skip_state_db_message(&error) => {
            log_skipped_state_db(&instance.name, &db_path, &error);
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    let select_columns = columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT {} FROM threads", select_columns);
    let mut statement = match connection.prepare(&query) {
        Ok(statement) => statement,
        Err(error) if should_skip_state_db_error(&error) => {
            log_skipped_state_db(&instance.name, &db_path, &error.to_string());
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(format!("读取实例会话失败 ({}): {}", instance.name, error));
        }
    };
    let mut rows = match statement.query([]) {
        Ok(rows) => rows,
        Err(error) if should_skip_state_db_error(&error) => {
            log_skipped_state_db(&instance.name, &db_path, &error.to_string());
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(format!("查询实例会话失败 ({}): {}", instance.name, error));
        }
    };
    let session_index_map = read_session_index_map(&instance.data_dir)?;

    let mut snapshots = Vec::new();
    loop {
        let Some(row) = (match rows.next() {
            Ok(row) => row,
            Err(error) if should_skip_state_db_error(&error) => {
                log_skipped_state_db(&instance.name, &db_path, &error.to_string());
                return Ok(Vec::new());
            }
            Err(error) => {
                return Err(format!("迭代实例会话失败 ({}): {}", instance.name, error));
            }
        }) else {
            break;
        };

        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(
                row.get::<usize, Value>(index)
                    .map_err(|error| format!("解析会话记录失败 ({}): {}", instance.name, error))?,
            );
        }

        let row_data = ThreadRowData {
            columns: columns.clone(),
            values,
        };
        let id = row_data
            .get_text("id")
            .ok_or_else(|| format!("会话缺少 id 字段 ({})", instance.name))?;
        let rollout_path = row_data
            .get_text("rollout_path")
            .ok_or_else(|| format!("会话 {} 缺少 rollout_path ({})", id, instance.name))?;
        let title = row_data
            .get_text("title")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| id.clone());
        let cwd = row_data
            .get_text("cwd")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "未知工作目录".to_string());
        let updated_at = row_data.get_i64("updated_at");
        let archived = row_data.get_i64("archived").unwrap_or_default() != 0;
        let rollout_path = PathBuf::from(rollout_path);
        let (rollout_len, rollout_modified_at_ms) = rollout_file_fingerprint(&rollout_path);
        let session_index_entry = session_index_map
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({ "id": id, "thread_name": title }));
        let materialized_foreign = row_data.get_text("thread_source").as_deref()
            == Some(SHARED_CHAT_THREAD_SOURCE)
            || session_index_entry_is_materialized_foreign(&session_index_entry, instance, &id);

        snapshots.push(ThreadSnapshot {
            id,
            title,
            cwd,
            updated_at,
            archived,
            rollout_path,
            rollout_len,
            rollout_modified_at_ms,
            row_data,
            session_index_entry,
            source_root: instance.data_dir.clone(),
            materialized_foreign,
        });
    }

    Ok(snapshots)
}

fn session_index_entry_is_materialized_foreign(
    session_index_entry: &JsonValue,
    instance: &CodexSyncInstance,
    session_id: &str,
) -> bool {
    let Some(metadata) = session_index_entry.get("cockpit_shared_chat") else {
        return false;
    };

    if metadata
        .get("source_session_id")
        .and_then(JsonValue::as_str)
        .map(|source_session_id| source_session_id != session_id)
        .unwrap_or(false)
    {
        return true;
    }

    let target_instance_id = metadata
        .get("target_instance_id")
        .and_then(JsonValue::as_str);
    if target_instance_id == Some(instance.id.as_str()) {
        return true;
    }

    let target_account_id = metadata
        .get("target_account_id")
        .and_then(JsonValue::as_str);
    let source_account_id = metadata
        .get("source_account_id")
        .and_then(JsonValue::as_str);
    match (target_account_id, instance.account_id.as_deref()) {
        (Some(target), Some(current)) if target == current => source_account_id
            .map(|source| source != current)
            .unwrap_or(true),
        _ => false,
    }
}

fn trash_snapshots_for_instance(
    instance: &CodexSyncInstance,
    trash_root: &Path,
    snapshots: &[ThreadSnapshot],
) -> Result<(), String> {
    for snapshot in snapshots {
        move_snapshot_rollout_to_trash(instance, trash_root, snapshot)?;
    }

    remove_threads_from_db(&instance.data_dir, snapshots)?;
    rewrite_session_index_without_ids(&instance.data_dir, snapshots)?;
    Ok(())
}

fn create_trash_root_dir() -> Result<PathBuf, String> {
    let root = get_session_trash_base_dir()?.join(Utc::now().format("%Y%m%d-%H%M%S").to_string());
    fs::create_dir_all(&root)
        .map_err(|error| format!("创建会话废纸篓目录失败 ({}): {}", root.display(), error))?;
    Ok(root)
}

fn get_session_trash_base_dir() -> Result<PathBuf, String> {
    let home = dirs::home_dir().ok_or("无法获取用户主目录")?;
    Ok(home.join(".Trash").join(SESSION_TRASH_ROOT_DIR))
}

fn move_snapshot_rollout_to_trash(
    instance: &CodexSyncInstance,
    trash_root: &Path,
    snapshot: &ThreadSnapshot,
) -> Result<(), String> {
    if !snapshot.rollout_path.exists() {
        return Ok(());
    }

    let relative_path = snapshot
        .rollout_path
        .strip_prefix(&snapshot.source_root)
        .unwrap_or(snapshot.rollout_path.as_path());
    let entry_dir = trash_root.join(format!(
        "{}--{}",
        sanitize_for_file_name(&instance.id),
        sanitize_for_file_name(&snapshot.id)
    ));
    let file_target = entry_dir.join("files").join(relative_path);
    if let Some(parent) = file_target.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("创建废纸篓会话目录失败 ({}): {}", parent.display(), error))?;
    }

    let manifest = json!({
        "sessionId": snapshot.id,
        "title": snapshot.title,
        "cwd": snapshot.cwd,
        "instanceId": instance.id,
        "instanceName": instance.name,
        "instanceRoot": instance.data_dir,
        "originalRolloutPath": snapshot.rollout_path,
        "relativeRolloutPath": relative_path.to_string_lossy(),
        "sessionIndexEntry": snapshot.session_index_entry,
        "threadRow": serialize_row_data(&snapshot.row_data),
        "deletedAt": Utc::now().to_rfc3339(),
    });

    fs::create_dir_all(&entry_dir)
        .map_err(|error| format!("创建废纸篓条目失败 ({}): {}", entry_dir.display(), error))?;
    fs::rename(&snapshot.rollout_path, &file_target).map_err(|error| {
        format!(
            "移动会话文件到废纸篓失败 ({} -> {}): {}",
            snapshot.rollout_path.display(),
            file_target.display(),
            error
        )
    })?;
    fs::write(
        entry_dir.join("manifest.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&manifest)
                .map_err(|error| format!("序列化会话废纸篓清单失败: {}", error))?
        ),
    )
    .map_err(|error| {
        format!(
            "写入会话废纸篓清单失败 ({}): {}",
            entry_dir.display(),
            error
        )
    })?;
    Ok(())
}

fn remove_threads_from_db(root_dir: &Path, snapshots: &[ThreadSnapshot]) -> Result<(), String> {
    let db_path = root_dir.join(STATE_DB_FILE);
    let mut connection = Connection::open(&db_path)
        .map_err(|error| format!("打开实例数据库失败 ({}): {}", db_path.display(), error))?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("开启会话删除事务失败 ({}): {}", db_path.display(), error))?;

    for snapshot in snapshots {
        transaction
            .execute("DELETE FROM threads WHERE id = ?1", [&snapshot.id])
            .map_err(|error| format!("删除会话记录失败 ({}): {}", snapshot.id, error))?;
    }

    transaction
        .commit()
        .map_err(|error| format!("提交会话删除事务失败 ({}): {}", db_path.display(), error))?;
    Ok(())
}

fn rewrite_session_index_without_ids(
    root_dir: &Path,
    snapshots: &[ThreadSnapshot],
) -> Result<(), String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    if !path.exists() {
        return Ok(());
    }

    let removed_ids = snapshots
        .iter()
        .map(|snapshot| snapshot.id.as_str())
        .collect::<HashSet<_>>();
    let content = fs::read_to_string(&path).map_err(|error| {
        format!(
            "读取 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    let retained = content
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return false;
            }
            match serde_json::from_str::<JsonValue>(trimmed) {
                Ok(value) => value
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .map(|id| !removed_ids.contains(id))
                    .unwrap_or(true),
                Err(_) => true,
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let final_content = if retained.is_empty() {
        String::new()
    } else {
        format!("{}\n", retained)
    };
    fs::write(&path, final_content).map_err(|error| {
        format!(
            "重写 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    Ok(())
}

fn read_session_index_map(root_dir: &Path) -> Result<HashMap<String, JsonValue>, String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let content = fs::read_to_string(&path).map_err(|error| {
        format!(
            "读取 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    let mut entries = HashMap::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(parsed) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        let Some(id) = parsed.get("id").and_then(JsonValue::as_str) else {
            continue;
        };
        entries.insert(id.to_string(), parsed);
    }

    Ok(entries)
}

fn open_readonly_connection(db_path: &Path) -> Result<Connection, String> {
    let mut uri = Url::from_file_path(db_path)
        .map_err(|_| format!("无法构建只读数据库 URI: {}", db_path.display()))?;
    uri.set_query(Some("mode=ro"));
    Connection::open_with_flags(
        uri.as_str(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|error| format!("打开只读数据库失败 ({}): {}", db_path.display(), error))
}

fn should_skip_state_db_error(error: &rusqlite::Error) -> bool {
    modules::db::is_unusable_sqlite_database_error(error)
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("no such table: threads")
}

fn should_skip_state_db_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    modules::db::is_unusable_sqlite_database_message(message)
        || lowered.contains("no such table: threads")
        || message.contains("threads 表不存在或没有列定义")
}

fn log_skipped_state_db(instance_name: &str, db_path: &Path, reason: &str) {
    modules::logger::log_warn(&format!(
        "跳过无法读取的 Codex 会话数据库 ({} / {}): {}",
        instance_name,
        db_path.display(),
        reason
    ));
}

fn read_thread_columns(connection: &Connection) -> Result<Vec<String>, String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(threads)")
        .map_err(|error| format!("读取 threads 表结构失败: {}", error))?;
    let mut rows = statement
        .query([])
        .map_err(|error| format!("查询 threads 表结构失败: {}", error))?;
    let mut columns = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("解析 threads 表结构失败: {}", error))?
    {
        columns.push(
            row.get::<usize, String>(1)
                .map_err(|error| format!("解析 threads 列失败: {}", error))?,
        );
    }

    if columns.is_empty() {
        return Err("threads 表不存在或没有列定义".to_string());
    }

    Ok(columns)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn serialize_row_data(row_data: &ThreadRowData) -> JsonValue {
    let mut object = serde_json::Map::new();
    for (column, value) in row_data.columns.iter().zip(row_data.values.iter()) {
        object.insert(column.clone(), sqlite_value_to_json(value));
    }
    JsonValue::Object(object)
}

fn sqlite_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Integer(number) => json!(number),
        Value::Real(number) => json!(number),
        Value::Text(text) => json!(text),
        Value::Blob(bytes) => json!(bytes
            .iter()
            .map(|byte| format!("{:02X}", byte))
            .collect::<String>()),
    }
}

fn sanitize_for_file_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
}

fn parse_deleted_at(value: Option<&str>) -> Option<i64> {
    let parsed = value.and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())?;
    Some(parsed.timestamp())
}

fn load_trash_entries() -> Result<Vec<TrashedSessionEntry>, String> {
    let root = get_session_trash_base_dir()?;
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let timestamp_dirs = fs::read_dir(&root)
        .map_err(|error| format!("读取会话废纸篓目录失败 ({}): {}", root.display(), error))?;
    for timestamp_dir in timestamp_dirs {
        let timestamp_dir = timestamp_dir
            .map_err(|error| format!("读取会话废纸篓目录项失败 ({}): {}", root.display(), error))?;
        let timestamp_path = timestamp_dir.path();
        let file_type = timestamp_dir.file_type().map_err(|error| {
            format!(
                "读取会话废纸篓目录类型失败 ({}): {}",
                timestamp_path.display(),
                error
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }

        let entry_dirs = fs::read_dir(&timestamp_path).map_err(|error| {
            format!(
                "读取会话废纸篓批次目录失败 ({}): {}",
                timestamp_path.display(),
                error
            )
        })?;
        for entry in entry_dirs {
            let entry = entry.map_err(|error| {
                format!(
                    "读取会话废纸篓条目失败 ({}): {}",
                    timestamp_path.display(),
                    error
                )
            })?;
            let entry_path = entry.path();
            let entry_type = entry.file_type().map_err(|error| {
                format!(
                    "读取会话废纸篓条目类型失败 ({}): {}",
                    entry_path.display(),
                    error
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let manifest_path = entry_path.join("manifest.json");
            if !manifest_path.exists() {
                continue;
            }
            let manifest_content = fs::read_to_string(&manifest_path).map_err(|error| {
                format!(
                    "读取会话废纸篓清单失败 ({}): {}",
                    manifest_path.display(),
                    error
                )
            })?;
            let manifest = serde_json::from_str::<TrashedSessionManifest>(&manifest_content)
                .map_err(|error| {
                    format!(
                        "解析会话废纸篓清单失败 ({}): {}",
                        manifest_path.display(),
                        error
                    )
                })?;
            let trashed_rollout_path = entry_path
                .join("files")
                .join(PathBuf::from(&manifest.relative_rollout_path));
            entries.push(TrashedSessionEntry {
                entry_dir: entry_path,
                manifest,
                trashed_rollout_path,
            });
        }
    }

    entries.sort_by(|left, right| {
        parse_deleted_at(right.manifest.deleted_at.as_deref())
            .unwrap_or_default()
            .cmp(&parse_deleted_at(left.manifest.deleted_at.as_deref()).unwrap_or_default())
            .then_with(|| left.manifest.session_id.cmp(&right.manifest.session_id))
            .then_with(|| left.manifest.instance_id.cmp(&right.manifest.instance_id))
    });
    Ok(entries)
}

fn restore_trashed_session_entry(entry: &TrashedSessionEntry) -> Result<(), String> {
    if !entry.trashed_rollout_path.exists() {
        return Err(format!(
            "废纸篓中的会话文件不存在，无法恢复 ({}): {}",
            entry.manifest.session_id,
            entry.trashed_rollout_path.display()
        ));
    }

    let row_data = deserialize_row_data(&entry.manifest.thread_row)?;
    let session_id = row_data
        .get_text("id")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| entry.manifest.session_id.clone());
    let target_rollout_path = entry.manifest.original_rollout_path.clone();
    if target_rollout_path.exists() {
        return Err(format!(
            "目标实例中已存在同名会话文件，无法恢复 ({}): {}",
            session_id,
            target_rollout_path.display()
        ));
    }

    let original_session_index_content = read_session_index_content(&entry.manifest.instance_root)?;
    if session_index_contains_id(&original_session_index_content, &session_id)? {
        return Err(format!(
            "目标实例的 session_index.jsonl 中已存在该会话，无法恢复 ({})",
            session_id
        ));
    }

    if let Some(parent) = target_rollout_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("创建会话恢复目录失败 ({}): {}", parent.display(), error))?;
    }
    fs::copy(&entry.trashed_rollout_path, &target_rollout_path).map_err(|error| {
        format!(
            "恢复会话文件失败 ({} -> {}): {}",
            entry.trashed_rollout_path.display(),
            target_rollout_path.display(),
            error
        )
    })?;

    let restore_result = (|| {
        write_session_index_with_entry(
            &entry.manifest.instance_root,
            &original_session_index_content,
            &session_id,
            &entry.manifest.session_index_entry,
        )?;
        insert_thread_row(&entry.manifest.instance_root, &row_data)?;
        Ok::<(), String>(())
    })();

    if let Err(error) = restore_result {
        let _ = fs::remove_file(&target_rollout_path);
        let _ = restore_session_index_content(
            &entry.manifest.instance_root,
            original_session_index_content.as_deref(),
        );
        return Err(error);
    }

    if let Err(error) = fs::remove_dir_all(&entry.entry_dir) {
        modules::logger::log_warn(&format!(
            "会话已恢复，但清理废纸篓条目失败 ({}): {}",
            entry.entry_dir.display(),
            error
        ));
    } else {
        cleanup_empty_trash_ancestors(&entry.entry_dir);
    }

    Ok(())
}

fn read_session_index_content(root_dir: &Path) -> Result<Option<String>, String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&path).map_err(|error| {
        format!(
            "读取 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    Ok(Some(content))
}

fn session_index_contains_id(content: &Option<String>, session_id: &str) -> Result<bool, String> {
    let Some(content) = content.as_deref() else {
        return Ok(false);
    };

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed = serde_json::from_str::<JsonValue>(trimmed)
            .map_err(|error| format!("解析 session_index.jsonl 条目失败: {}", error))?;
        if parsed.get("id").and_then(JsonValue::as_str) == Some(session_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn write_session_index_with_entry(
    root_dir: &Path,
    original_content: &Option<String>,
    session_id: &str,
    entry: &JsonValue,
) -> Result<(), String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    let serialized_entry = serde_json::to_string(entry)
        .map_err(|error| format!("序列化 session_index 条目失败 ({}): {}", session_id, error))?;
    let mut lines = original_content
        .as_deref()
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    lines.push(serialized_entry);
    let next_content = if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    };
    fs::write(&path, next_content).map_err(|error| {
        format!(
            "写入 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    Ok(())
}

fn restore_session_index_content(root_dir: &Path, content: Option<&str>) -> Result<(), String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    match content {
        Some(value) => fs::write(&path, value).map_err(|error| {
            format!(
                "恢复 session_index.jsonl 失败 ({}): {}",
                path.display(),
                error
            )
        })?,
        None => {
            if path.exists() {
                fs::remove_file(&path).map_err(|error| {
                    format!(
                        "删除恢复失败的 session_index.jsonl 失败 ({}): {}",
                        path.display(),
                        error
                    )
                })?;
            }
        }
    }
    Ok(())
}

fn deserialize_row_data(value: &JsonValue) -> Result<ThreadRowData, String> {
    let object = value
        .as_object()
        .ok_or("废纸篓中的线程数据格式无效，缺少对象结构".to_string())?;
    let mut columns = object.keys().cloned().collect::<Vec<_>>();
    columns.sort();
    let values = columns
        .iter()
        .map(|column| json_to_sqlite_value(object.get(column).unwrap_or(&JsonValue::Null)))
        .collect::<Vec<_>>();
    Ok(ThreadRowData { columns, values })
}

fn json_to_sqlite_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(flag) => Value::Integer(i64::from(*flag)),
        JsonValue::Number(number) => number
            .as_i64()
            .map(Value::Integer)
            .or_else(|| number.as_f64().map(Value::Real))
            .unwrap_or_else(|| Value::Text(number.to_string())),
        JsonValue::String(text) => Value::Text(text.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(value.to_string()),
    }
}

fn insert_thread_row(root_dir: &Path, row_data: &ThreadRowData) -> Result<(), String> {
    let db_path = root_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Err(format!(
            "目标实例缺少 state_5.sqlite，无法恢复会话 ({})",
            db_path.display()
        ));
    }
    let mut connection = Connection::open(&db_path)
        .map_err(|error| format!("打开实例数据库失败 ({}): {}", db_path.display(), error))?;
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| {
            format!(
                "设置数据库 busy_timeout 失败 ({}): {}",
                db_path.display(),
                error
            )
        })?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("开启会话恢复事务失败 ({}): {}", db_path.display(), error))?;
    let session_id = row_data
        .get_text("id")
        .filter(|value| !value.trim().is_empty())
        .ok_or("废纸篓中的线程数据缺少 id 字段".to_string())?;
    let exists = transaction
        .query_row(
            "SELECT COUNT(*) FROM threads WHERE id = ?1",
            [&session_id],
            |row| row.get::<usize, i64>(0),
        )
        .map_err(|error| format!("检查会话是否已存在失败 ({}): {}", session_id, error))?;
    if exists > 0 {
        return Err(format!("目标实例中已存在该会话，无法恢复 ({})", session_id));
    }

    let db_columns = read_thread_columns(&transaction)?;
    let db_column_set = db_columns.into_iter().collect::<HashSet<_>>();
    let insert_columns = row_data
        .columns
        .iter()
        .filter(|column| db_column_set.contains(*column))
        .cloned()
        .collect::<Vec<_>>();
    if insert_columns.is_empty() {
        return Err("threads 表没有可用于恢复的列".to_string());
    }

    let mut insert_values = Vec::with_capacity(insert_columns.len());
    for column in &insert_columns {
        let value = row_data
            .get_value(column)
            .cloned()
            .ok_or_else(|| format!("废纸篓中的线程数据缺少列: {}", column))?;
        insert_values.push(value);
    }
    let placeholders = vec!["?"; insert_columns.len()].join(", ");
    let sql = format!(
        "INSERT INTO threads ({}) VALUES ({})",
        insert_columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders
    );
    transaction
        .execute(&sql, params_from_iter(insert_values.iter()))
        .map_err(|error| format!("恢复会话记录失败 ({}): {}", session_id, error))?;
    transaction
        .commit()
        .map_err(|error| format!("提交会话恢复事务失败 ({}): {}", db_path.display(), error))?;
    Ok(())
}

fn cleanup_empty_trash_ancestors(entry_dir: &Path) {
    let mut current = entry_dir.parent();
    while let Some(dir) = current {
        if dir.file_name().and_then(|value| value.to_str()) == Some(SESSION_TRASH_ROOT_DIR) {
            break;
        }
        let is_empty = fs::read_dir(dir)
            .ok()
            .and_then(|mut iterator| iterator.next().transpose().ok())
            .flatten()
            .is_none();
        if !is_empty {
            break;
        }
        if fs::remove_dir(dir).is_err() {
            break;
        }
        current = dir.parent();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_temp_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), unique));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn test_instance(
        id: &str,
        name: &str,
        account_id: &str,
        data_dir: PathBuf,
    ) -> CodexSyncInstance {
        CodexSyncInstance {
            id: id.to_string(),
            name: name.to_string(),
            data_dir,
            account_id: Some(account_id.to_string()),
            last_pid: None,
        }
    }

    fn create_threads_db(root: &Path) {
        fs::create_dir_all(root).expect("create root");
        let db_path = root.join(STATE_DB_FILE);
        let connection = Connection::open(db_path).expect("open db");
        connection
            .execute_batch(
                r#"
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT NOT NULL,
                    created_at INTEGER,
                    updated_at INTEGER,
                    source TEXT,
                    model_provider TEXT,
                    cwd TEXT,
                    title TEXT,
                    archived INTEGER,
                    archived_at INTEGER,
                    thread_source TEXT
                );
                "#,
            )
            .expect("create threads table");
    }

    fn write_thread(
        root: &Path,
        id: &str,
        title: &str,
        archived: bool,
        updated_at: i64,
    ) -> PathBuf {
        let dir_name = if archived {
            "archived_sessions"
        } else {
            "sessions"
        };
        let rollout_dir = root.join(dir_name).join("2026").join("05").join("11");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join(format!("rollout-2026-05-11T12-00-00-{}.jsonl", id));
        let first_line = json!({
            "type": "session_meta",
            "payload": {
                "id": id,
                "timestamp": "2026-05-11T10:00:00.000Z",
                "cwd": "C:\\workspace",
                "originator": "codex_cli_rs",
                "cli_version": "0.130.0-alpha.5",
                "source": "vscode",
                "model_provider": "openai"
            }
        });
        fs::write(
            &rollout_path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first_line).expect("serialize first line"),
                serde_json::to_string(&json!({
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "redacted"}
                }))
                .expect("serialize event line")
            ),
        )
        .expect("write rollout");

        let connection = Connection::open(root.join(STATE_DB_FILE)).expect("open db");
        connection
            .execute(
                "INSERT INTO threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, archived, archived_at, thread_source)
                 VALUES (?1, ?2, ?3, ?4, 'vscode', 'openai', 'C:\\workspace', ?5, ?6, ?7, NULL)",
                params![
                    id,
                    rollout_path.to_string_lossy().to_string(),
                    updated_at - 10,
                    updated_at,
                    title,
                    if archived { 1 } else { 0 },
                    if archived { updated_at } else { 0 },
                ],
            )
            .expect("insert thread");
        append_index_line(
            root,
            &json!({
                "id": id,
                "thread_name": title,
                "updated_at": updated_at,
            }),
        );
        rollout_path
    }

    fn append_index_line(root: &Path, value: &JsonValue) {
        let path = root.join(SESSION_INDEX_FILE);
        let mut content = String::new();
        if path.exists() {
            content = fs::read_to_string(&path).expect("read existing index");
        }
        content.push_str(&serde_json::to_string(value).expect("serialize index"));
        content.push('\n');
        fs::write(path, content).expect("write index");
    }

    fn thread_ids(root: &Path) -> Vec<String> {
        let connection = Connection::open(root.join(STATE_DB_FILE)).expect("open db");
        let mut statement = connection
            .prepare("SELECT id FROM threads ORDER BY id")
            .expect("prepare ids");
        statement
            .query_map([], |row| row.get::<usize, String>(0))
            .expect("query ids")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect ids")
    }

    fn rollout_path_for_thread(root: &Path, id: &str) -> PathBuf {
        let connection = Connection::open(root.join(STATE_DB_FILE)).expect("open db");
        let value = connection
            .query_row(
                "SELECT rollout_path FROM threads WHERE id = ?1",
                [id],
                |row| row.get::<usize, String>(0),
            )
            .expect("read rollout path");
        PathBuf::from(value)
    }

    fn append_rollout_line_and_update_thread(root: &Path, id: &str, updated_at: i64, marker: &str) {
        let rollout_path = rollout_path_for_thread(root, id);
        let mut content = fs::read_to_string(&rollout_path).expect("read rollout");
        content.push_str(
            &serde_json::to_string(&json!({
                "type": "event_msg",
                "payload": {"type": "agent_message", "message": marker}
            }))
            .expect("serialize appended event"),
        );
        content.push('\n');
        fs::write(&rollout_path, content).expect("rewrite rollout");

        let connection = Connection::open(root.join(STATE_DB_FILE)).expect("open db");
        connection
            .execute(
                "UPDATE threads SET updated_at = ?1 WHERE id = ?2",
                params![updated_at, id],
            )
            .expect("update thread timestamp");
        fs::write(
            root.join(SESSION_INDEX_FILE),
            format!(
                "{}\n",
                serde_json::to_string(&json!({
                    "id": id,
                    "thread_name": "Source title",
                    "updated_at": updated_at,
                }))
                .expect("serialize updated index")
            ),
        )
        .expect("rewrite index");
    }

    fn append_rollout_json_line(root: &Path, id: &str, value: &JsonValue) {
        let rollout_path = rollout_path_for_thread(root, id);
        let mut content = fs::read_to_string(&rollout_path).expect("read rollout");
        content.push_str(&serde_json::to_string(value).expect("serialize appended event"));
        content.push('\n');
        fs::write(&rollout_path, content).expect("rewrite rollout");
    }

    fn update_thread_timestamp(root: &Path, id: &str, updated_at: i64) {
        let connection = Connection::open(root.join(STATE_DB_FILE)).expect("open db");
        connection
            .execute(
                "UPDATE threads SET updated_at = ?1 WHERE id = ?2",
                params![updated_at, id],
            )
            .expect("update thread timestamp");
    }

    fn ensure_shared_chat_visibility_across_instances_for_tests(
        instances: &[CodexSyncInstance],
    ) -> Result<(), String> {
        for instance in instances {
            ensure_shared_chat_visibility_for_instances(instances, &instance.id)?;
        }
        Ok(())
    }

    #[test]
    fn shared_chat_catalog_lists_all_instances_and_marks_foreign() {
        let root = make_temp_dir("codex-shared-catalog-list-test");
        let default_root = root.join("default");
        let second_root = root.join("second");
        let third_root = root.join("third");
        for dir in [&default_root, &second_root, &third_root] {
            create_threads_db(dir);
        }
        write_thread(&default_root, "session-a", "A title", false, 100);
        write_thread(&second_root, "session-b", "B title", false, 200);
        write_thread(&third_root, "session-c", "C title", false, 300);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", default_root),
            test_instance("second", "Second", "account-b", second_root.clone()),
            test_instance("third", "Third", "account-c", third_root),
        ];

        let records = list_shared_chat_catalog_for_instances(&instances, "second")
            .expect("list shared chat catalog");

        assert_eq!(records.len(), 3);
        let own = records
            .iter()
            .find(|record| record.source_session_id == "session-b")
            .expect("own session");
        assert!(!own.foreign);
        let foreign_count = records.iter().filter(|record| record.foreign).count();
        assert_eq!(foreign_count, 2);
        assert!(records
            .iter()
            .any(|record| record.source_instance_id == "__default__"));
        assert!(records
            .iter()
            .any(|record| record.source_instance_id == "third"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_account_id_resolution_prefers_profile_marker_over_stale_binding() {
        let root = make_temp_dir("codex-shared-account-marker-test");
        fs::write(
            root.join(".cockpit_codex_auth.json"),
            serde_json::to_string(&json!({
                "account_id": "account-from-profile",
                "email": "redacted@example.com",
            }))
            .expect("serialize marker"),
        )
        .expect("write marker");

        let resolved = resolve_profile_account_id(&root, Some("stale-bound-account".to_string()));

        assert_eq!(resolved.as_deref(), Some("account-from-profile"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_visibility_noops_when_instances_share_canonical_state_db() {
        let root = make_temp_dir("codex-shared-canonical-history-test");
        create_threads_db(&root);
        write_thread(&root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", root.clone()),
            test_instance("second", "Second", "account-b", root.clone()),
        ];

        let summary = ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("sync shared canonical history");

        assert_eq!(summary.materialized_foreign_count, 0);
        assert_eq!(summary.copied_same_account_count, 0);
        assert_eq!(summary.removed_unsafe_same_id_count, 0);
        assert_eq!(thread_ids(&root), vec!["source-thread".to_string()]);
        assert!(!root.join(SHARED_CHAT_CATALOG_FILE).exists());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_forks_foreign_thread_with_new_local_id() {
        let root = make_temp_dir("codex-shared-catalog-fork-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        let summary = ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("sync shared chat visibility");

        assert_eq!(summary.materialized_foreign_count, 1);
        let ids = thread_ids(&target_root);
        assert_eq!(ids.len(), 1);
        assert_ne!(ids[0], "source-thread");

        let rollout_path = rollout_path_for_thread(&target_root, &ids[0]);
        let first_line = fs::read_to_string(&rollout_path)
            .expect("read forked rollout")
            .lines()
            .next()
            .expect("first line")
            .to_string();
        let first_json: JsonValue = serde_json::from_str(&first_line).expect("parse first line");
        assert_eq!(first_json["payload"]["id"].as_str(), Some(ids[0].as_str()));
        assert_eq!(
            first_json["payload"]["cockpit_shared_chat"]["source_session_id"].as_str(),
            Some("source-thread")
        );
        let connection = Connection::open(target_root.join(STATE_DB_FILE)).expect("open target db");
        let thread_source = connection
            .query_row(
                "SELECT thread_source FROM threads WHERE id = ?1",
                [&ids[0]],
                |row| row.get::<usize, Option<String>>(0),
            )
            .expect("read thread source");
        assert_eq!(thread_source, None);
        assert_eq!(thread_ids(&source_root), vec!["source-thread".to_string()]);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_localizes_dangerous_cross_account_same_id_copy() {
        let root = make_temp_dir("codex-shared-catalog-localize-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "shared-id", "Original", false, 100);
        write_thread(&target_root, "shared-id", "Copied", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        let summary = ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("sync shared chat visibility");

        assert_eq!(summary.removed_unsafe_same_id_count, 1);
        assert_eq!(summary.materialized_foreign_count, 1);
        let ids = thread_ids(&target_root);
        assert_eq!(ids.len(), 1);
        assert_ne!(ids[0], "shared-id");
        assert_eq!(thread_ids(&source_root), vec!["shared-id".to_string()]);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_removes_missing_local_rows_even_when_source_index_is_polluted() {
        let root = make_temp_dir("codex-shared-catalog-polluted-index-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let stale_rollout = write_thread(&source_root, "stale-local-id", "Stale local", false, 100);
        fs::write(
            source_root.join(SESSION_INDEX_FILE),
            format!(
                "{}\n",
                serde_json::to_string(&json!({
                    "id": "stale-local-id",
                    "thread_name": "Stale local",
                    "cockpit_shared_chat": {
                        "source_account_id": "account-a",
                        "source_instance_id": "__default__",
                        "source_session_id": "source-thread",
                        "source_rollout_path": source_root.join("sessions").join("source.jsonl"),
                        "target_account_id": "account-b",
                        "target_instance_id": "second"
                    }
                }))
                .expect("serialize polluted index")
            ),
        )
        .expect("write polluted source index");
        assert!(stale_rollout.exists());

        let missing_target_rollout = write_thread(
            &target_root,
            "stale-local-id",
            "Missing stale local",
            false,
            100,
        );
        fs::remove_file(&missing_target_rollout).expect("remove stale target rollout");
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        let summary = ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("sync shared chat visibility");

        assert_eq!(summary.removed_unsafe_same_id_count, 1);
        assert_eq!(summary.materialized_foreign_count, 1);
        let ids = thread_ids(&target_root);
        assert_eq!(ids.len(), 1);
        assert_ne!(ids[0], "stale-local-id");
        let source_ids = thread_ids(&source_root);
        assert!(source_ids.contains(&"source-thread".to_string()));
        assert!(source_ids.contains(&"stale-local-id".to_string()));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_repairs_materialized_foreign_thread_with_missing_rollout() {
        let root = make_temp_dir("codex-shared-catalog-missing-rollout-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("initial sync shared chat visibility");
        let stale_local_id = thread_ids(&target_root)
            .into_iter()
            .next()
            .expect("initial local fork id");
        let stale_rollout_path = rollout_path_for_thread(&target_root, &stale_local_id);
        fs::remove_file(&stale_rollout_path).expect("remove materialized rollout");

        let summary = ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("repair shared chat visibility");

        assert_eq!(summary.removed_unsafe_same_id_count, 1);
        assert_eq!(summary.materialized_foreign_count, 1);
        let ids = thread_ids(&target_root);
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], stale_local_id);
        assert_ne!(ids[0], "source-thread");
        assert!(rollout_path_for_thread(&target_root, &ids[0]).exists());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_refreshes_existing_materialized_foreign_thread() {
        let root = make_temp_dir("codex-shared-catalog-refresh-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("initial sync shared chat visibility");
        let local_id = thread_ids(&target_root)
            .into_iter()
            .next()
            .expect("initial local fork id");
        append_rollout_line_and_update_thread(&source_root, "source-thread", 200, "fresh reply");

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("refresh shared chat visibility");

        assert_eq!(thread_ids(&target_root), vec![local_id.clone()]);
        let refreshed = fs::read_to_string(rollout_path_for_thread(&target_root, &local_id))
            .expect("read refreshed rollout");
        assert!(refreshed.contains("fresh reply"));
        let first_line = refreshed.lines().next().expect("first line");
        let first_json: JsonValue = serde_json::from_str(first_line).expect("parse first line");
        assert_eq!(
            first_json["payload"]["id"].as_str(),
            Some(local_id.as_str())
        );

        let catalog = load_shared_chat_catalog(&target_root).expect("load catalog");
        let mapping = catalog
            .mappings
            .iter()
            .find(|item| item.source_session_id == "source-thread")
            .expect("mapping");
        assert_eq!(mapping.local_session_id, local_id);
        assert_eq!(mapping.last_source_updated_at, Some(200));
        assert_eq!(mapping.local_materialized_updated_at, Some(200));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_refreshes_same_account_copy_when_rollout_grows_without_timestamp_change() {
        let root = make_temp_dir("codex-shared-same-account-live-refresh-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-a", target_root.clone()),
        ];

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("initial same-account sync");
        append_rollout_json_line(
            &source_root,
            "source-thread",
            &json!({
                "type": "event_msg",
                "payload": {"type": "agent_message", "message": "streamed final answer"}
            }),
        );

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("refresh same-account sync");

        let refreshed = fs::read_to_string(rollout_path_for_thread(&target_root, "source-thread"))
            .expect("read refreshed same-account rollout");
        assert!(refreshed.contains("streamed final answer"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_refreshes_foreign_materialization_when_rollout_grows_without_timestamp_change(
    ) {
        let root = make_temp_dir("codex-shared-foreign-live-refresh-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("initial foreign sync");
        let local_id = thread_ids(&target_root)
            .into_iter()
            .next()
            .expect("initial local fork id");
        append_rollout_json_line(
            &source_root,
            "source-thread",
            &json!({
                "type": "event_msg",
                "payload": {"type": "agent_message", "message": "foreign streamed final answer"}
            }),
        );

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("refresh foreign sync");

        assert_eq!(thread_ids(&target_root), vec![local_id.clone()]);
        let refreshed = fs::read_to_string(rollout_path_for_thread(&target_root, &local_id))
            .expect("read refreshed foreign rollout");
        assert!(refreshed.contains("foreign streamed final answer"));
        let catalog = load_shared_chat_catalog(&target_root).expect("load catalog");
        let mapping = catalog
            .mappings
            .iter()
            .find(|item| item.source_session_id == "source-thread")
            .expect("mapping");
        assert!(mapping.last_source_rollout_len.is_some());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_strips_upstream_resume_ids_from_foreign_materialization() {
        let root = make_temp_dir("codex-shared-catalog-resume-id-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        append_rollout_json_line(
            &source_root,
            "source-thread",
            &json!({
                "type": "event_msg",
                "payload": {
                    "type": "turn_context",
                    "visible": "kept",
                    "request": {
                        "previous_response_id": "resp_source_account"
                    },
                    "nested": [
                        {
                            "previousResponseId": "resp_source_account_camel"
                        }
                    ]
                }
            }),
        );
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("sync shared chat visibility");

        let local_id = thread_ids(&target_root)
            .into_iter()
            .next()
            .expect("local fork id");
        let forked = fs::read_to_string(rollout_path_for_thread(&target_root, &local_id))
            .expect("read forked rollout");
        assert!(forked.contains("kept"));
        assert!(!forked.contains("previous_response_id"));
        assert!(!forked.contains("previousResponseId"));
        assert!(!forked.contains("resp_source_account"));

        let source = fs::read_to_string(rollout_path_for_thread(&source_root, "source-thread"))
            .expect("read source rollout");
        assert!(source.contains("previous_response_id"));
        assert!(source.contains("previousResponseId"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_promotes_foreign_reply_without_duplicate_logical_chat() {
        let root = make_temp_dir("codex-shared-catalog-bidirectional-test");
        let source_root = root.join("default");
        let second_root = root.join("second");
        let third_root = root.join("third");
        create_threads_db(&source_root);
        create_threads_db(&second_root);
        create_threads_db(&third_root);
        write_thread(&source_root, "source-thread", "Source title", false, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root.clone()),
            test_instance("second", "Second", "account-b", second_root.clone()),
            test_instance("third", "Third", "account-c", third_root.clone()),
        ];

        ensure_shared_chat_visibility_across_instances_for_tests(&instances)
            .expect("initial sync shared chat visibility");
        let second_local_id = thread_ids(&second_root)
            .into_iter()
            .next()
            .expect("second local id");
        let third_local_id = thread_ids(&third_root)
            .into_iter()
            .next()
            .expect("third local id");
        append_rollout_json_line(
            &second_root,
            &second_local_id,
            &json!({
                "type": "event_msg",
                "payload": {
                    "type": "agent_message",
                    "message": "reply from account b",
                    "request": {
                        "previous_response_id": "resp_account_b"
                    }
                }
            }),
        );
        update_thread_timestamp(&second_root, &second_local_id, 300);

        ensure_shared_chat_visibility_across_instances_for_tests(&instances)
            .expect("bidirectional sync shared chat visibility");

        assert_eq!(thread_ids(&source_root), vec!["source-thread".to_string()]);
        assert_eq!(thread_ids(&second_root), vec![second_local_id.clone()]);
        assert_eq!(thread_ids(&third_root), vec![third_local_id.clone()]);

        let source = fs::read_to_string(rollout_path_for_thread(&source_root, "source-thread"))
            .expect("read source rollout");
        assert!(source.contains("reply from account b"));
        assert!(!source.contains("cockpit_shared_chat"));
        assert!(!source.contains("previous_response_id"));
        assert!(!source.contains("resp_account_b"));
        let first_line = source.lines().next().expect("source first line");
        let first_json: JsonValue = serde_json::from_str(first_line).expect("parse first line");
        assert_eq!(first_json["payload"]["id"].as_str(), Some("source-thread"));

        let second = fs::read_to_string(rollout_path_for_thread(&second_root, &second_local_id))
            .expect("read second rollout");
        assert!(second.contains("reply from account b"));
        assert!(second.contains("cockpit_shared_chat"));
        assert!(!second.contains("previous_response_id"));
        assert!(!second.contains("resp_account_b"));

        let third = fs::read_to_string(rollout_path_for_thread(&third_root, &third_local_id))
            .expect("read third rollout");
        assert!(third.contains("reply from account b"));
        assert!(third.contains("cockpit_shared_chat"));
        assert!(!third.contains("previous_response_id"));
        assert!(!third.contains("resp_account_b"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_chat_sync_preserves_archived_foreign_visibility() {
        let root = make_temp_dir("codex-shared-catalog-archived-test");
        let source_root = root.join("default");
        let target_root = root.join("second");
        create_threads_db(&source_root);
        create_threads_db(&target_root);
        write_thread(&source_root, "archived-source", "Archived", true, 100);
        let instances = vec![
            test_instance("__default__", "Default", "account-a", source_root),
            test_instance("second", "Second", "account-b", target_root.clone()),
        ];

        ensure_shared_chat_visibility_for_instances(&instances, "second")
            .expect("sync shared chat visibility");

        let ids = thread_ids(&target_root);
        assert_eq!(ids.len(), 1);
        let connection = Connection::open(target_root.join(STATE_DB_FILE)).expect("open db");
        let (archived, rollout_path): (i64, String) = connection
            .query_row(
                "SELECT archived, rollout_path FROM threads WHERE id = ?1",
                [&ids[0]],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read archived row");
        assert_eq!(archived, 1);
        assert!(rollout_path.contains("archived_sessions"));

        let _ = fs::remove_dir_all(&root);
    }
}
