use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use chrono::Utc;
use rusqlite::{params_from_iter, types::Value, Connection, OpenFlags};
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

use crate::models::{DefaultInstanceSettings, InstanceLaunchMode, InstanceProfile, InstanceStore};
use crate::modules;
use crate::modules::instance::InstanceDefaults;
use crate::modules::instance_store;

static CODEX_INSTANCE_STORE_LOCK: std::sync::LazyLock<Mutex<()>> =
    std::sync::LazyLock::new(|| Mutex::new(()));

const CODEX_INSTANCES_FILE: &str = "codex_instances.json";
pub const CODEX_API_SERVICE_BIND_ACCOUNT_ID: &str = "__api_service__";
const CODEX_SHARED_SKILLS_DIR_NAME: &str = "skills";
const CODEX_SHARED_RULES_DIR_NAME: &str = "rules";
const CODEX_SHARED_AGENTS_FILE_NAME: &str = "AGENTS.md";
const CODEX_SHARED_VENDOR_IMPORTS_SKILLS_DIR: &str = "vendor_imports/skills";
const CODEX_SHARED_SESSIONS_DIR_NAME: &str = "sessions";
const CODEX_SHARED_ARCHIVED_SESSIONS_DIR_NAME: &str = "archived_sessions";
const CODEX_SHARED_SESSION_INDEX_FILE_NAME: &str = "session_index.jsonl";
const CODEX_SHARED_GLOBAL_STATE_FILE_NAME: &str = ".codex-global-state.json";
const CODEX_SHARED_STATE_DB_FILE_NAME: &str = "state_5.sqlite";
const CODEX_SHARED_STATE_DB_WAL_FILE_NAME: &str = "state_5.sqlite-wal";
const CODEX_SHARED_STATE_DB_SHM_FILE_NAME: &str = "state_5.sqlite-shm";
const CODEX_SHARED_CHAT_THREAD_SOURCE: &str = "cockpit_shared_foreign";
const CODEX_ELECTRON_USER_DATA_DIR_NAME: &str = "electron-user-data";
const CODEX_ELECTRON_AUTH_MARKER_FILE_NAME: &str = ".cockpit_codex_electron_auth.json";
const CODEX_SHARED_HISTORY_BACKUP_DIR_NAME: &str = ".cockpit-shared-history-backups";

pub fn is_api_service_bind_account_id(account_id: &str) -> bool {
    account_id.trim() == CODEX_API_SERVICE_BIND_ACCOUNT_ID
}

#[derive(Debug, Clone)]
pub struct CreateInstanceParams {
    pub name: String,
    pub user_data_dir: String,
    pub working_dir: Option<String>,
    pub extra_args: String,
    pub bind_account_id: Option<String>,
    pub copy_source_instance_id: Option<String>,
    pub init_mode: Option<String>,
    pub launch_mode: Option<InstanceLaunchMode>,
}

#[derive(Debug, Clone)]
pub struct UpdateInstanceParams {
    pub instance_id: String,
    pub name: Option<String>,
    pub working_dir: Option<String>,
    pub extra_args: Option<String>,
    pub bind_account_id: Option<Option<String>>,
    pub launch_mode: Option<InstanceLaunchMode>,
}

fn instances_path() -> Result<PathBuf, String> {
    let data_dir = modules::account::get_data_dir()?;
    Ok(data_dir.join(CODEX_INSTANCES_FILE))
}

pub fn load_instance_store() -> Result<InstanceStore, String> {
    let path = instances_path()?;
    instance_store::load_instance_store(&path, CODEX_INSTANCES_FILE)
}

pub fn save_instance_store(store: &InstanceStore) -> Result<(), String> {
    let path = instances_path()?;
    instance_store::save_instance_store(&path, CODEX_INSTANCES_FILE, store)
}

pub fn load_default_settings() -> Result<DefaultInstanceSettings, String> {
    let store = load_instance_store()?;
    Ok(store.default_settings)
}

pub fn update_default_settings(
    bind_account_id: Option<Option<String>>,
    extra_args: Option<String>,
    follow_local_account: Option<bool>,
    launch_mode: Option<InstanceLaunchMode>,
) -> Result<DefaultInstanceSettings, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let settings = &mut store.default_settings;

    if follow_local_account == Some(true) {
        settings.follow_local_account = true;
        settings.bind_account_id = None;
    }

    if let Some(bind) = bind_account_id {
        settings.bind_account_id = bind;
        settings.follow_local_account = false;
    }

    if follow_local_account == Some(false) && settings.bind_account_id.is_none() {
        settings.follow_local_account = false;
    }

    if let Some(args) = extra_args {
        settings.extra_args = args.trim().to_string();
    }

    if let Some(mode) = launch_mode {
        settings.launch_mode = mode;
    }

    let updated = settings.clone();
    save_instance_store(&store)?;
    Ok(updated)
}

pub fn get_default_codex_home() -> Result<PathBuf, String> {
    let home = dirs::home_dir().ok_or("无法获取用户主目录")?;
    Ok(home.join(".codex"))
}

pub fn get_default_instances_root_dir() -> Result<PathBuf, String> {
    #[cfg(target_os = "macos")]
    {
        let home = dirs::home_dir().ok_or("无法获取用户主目录")?;
        return Ok(home.join(".antigravity_cockpit/instances/codex"));
    }

    #[cfg(target_os = "windows")]
    {
        let appdata = std::env::var("APPDATA")
            .map_err(|_| "Failed to read APPDATA environment variable".to_string())?;
        return Ok(PathBuf::from(appdata).join(".antigravity_cockpit\\instances\\codex"));
    }

    #[allow(unreachable_code)]
    Err("Codex multi-instance is only supported on macOS and Windows".to_string())
}

pub fn get_instance_defaults() -> Result<InstanceDefaults, String> {
    let root_dir = get_default_instances_root_dir()?;
    let default_user_data_dir = get_default_codex_home()?;
    Ok(InstanceDefaults {
        root_dir: root_dir.to_string_lossy().to_string(),
        default_user_data_dir: default_user_data_dir.to_string_lossy().to_string(),
    })
}

fn remove_path_safely(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }

    let metadata = fs::symlink_metadata(path).map_err(|e| {
        format!(
            "read path metadata failed ({}): {}",
            display_abs_path(path),
            e
        )
    })?;
    if metadata.file_type().is_symlink() || metadata.is_file() {
        return fs::remove_file(path)
            .map_err(|e| format!("remove file failed ({}): {}", display_abs_path(path), e));
    }
    if metadata.is_dir() {
        return fs::remove_dir_all(path).map_err(|e| {
            format!(
                "remove directory failed ({}): {}",
                display_abs_path(path),
                e
            )
        });
    }
    Ok(())
}

fn electron_auth_marker_matches(electron_user_data_dir: &Path, account_id: &str) -> bool {
    let marker_path = electron_user_data_dir.join(CODEX_ELECTRON_AUTH_MARKER_FILE_NAME);
    let Ok(content) = fs::read_to_string(marker_path) else {
        return false;
    };
    content.contains(&format!(
        "\"account_id\":\"{}\"",
        account_id.replace('"', "\\\"")
    )) || content.contains(&format!(
        "\"account_id\": \"{}\"",
        account_id.replace('"', "\\\"")
    ))
}

fn write_electron_auth_marker(
    electron_user_data_dir: &Path,
    account_id: &str,
) -> Result<(), String> {
    fs::create_dir_all(electron_user_data_dir).map_err(|e| {
        format!(
            "create Electron user-data directory failed ({}): {}",
            display_abs_path(electron_user_data_dir),
            e
        )
    })?;
    let escaped_account_id = account_id.replace('\\', "\\\\").replace('"', "\\\"");
    let content = format!(
        "{{\"account_id\":\"{}\",\"prepared_at\":{}}}\n",
        escaped_account_id,
        Utc::now().timestamp_millis()
    );
    fs::write(
        electron_user_data_dir.join(CODEX_ELECTRON_AUTH_MARKER_FILE_NAME),
        content,
    )
    .map_err(|e| format!("write Electron auth marker failed: {}", e))
}

fn default_electron_user_data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        return std::env::var("APPDATA")
            .ok()
            .map(PathBuf::from)
            .map(|dir| dir.join("Codex"));
    }

    #[cfg(target_os = "macos")]
    {
        return dirs::data_dir().map(|dir| dir.join("Codex"));
    }

    #[allow(unreachable_code)]
    None
}

fn electron_user_data_dir_for_profile(profile_dir: &Path) -> PathBuf {
    let default_codex_home = get_default_codex_home().ok();
    if default_codex_home
        .as_ref()
        .map(|default_home| paths_point_to_same_location(profile_dir, default_home))
        .unwrap_or(false)
    {
        if let Some(default_electron_dir) = default_electron_user_data_dir() {
            return default_electron_dir;
        }
    }

    profile_dir.join(CODEX_ELECTRON_USER_DATA_DIR_NAME)
}

pub fn clear_electron_user_data_auth_state(
    profile_dir: &Path,
    account_id: &str,
) -> Result<(), String> {
    let electron_user_data_dir = electron_user_data_dir_for_profile(profile_dir);
    if electron_auth_marker_matches(&electron_user_data_dir, account_id) {
        return Ok(());
    }

    let paths_to_remove = [
        "Local Storage",
        "Session Storage",
        "IndexedDB",
        "Network",
        "Service Worker",
        "Cache",
        "Code Cache",
        "DawnGraphiteCache",
        "DawnWebGPUCache",
        "GPUCache",
        "Shared Dictionary",
        "blob_storage",
        "databases",
        "DIPS",
        "Local State",
        "Preferences",
    ];

    for relative in paths_to_remove {
        remove_path_safely(&electron_user_data_dir.join(relative))?;
    }

    write_electron_auth_marker(&electron_user_data_dir, account_id)
}

#[cfg(unix)]
fn create_directory_symlink(source: &Path, target: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(source, target).map_err(|e| format!("创建目录共享链接失败: {}", e))
}

#[cfg(windows)]
fn create_directory_symlink(source: &Path, target: &Path) -> Result<(), String> {
    create_directory_shared_link_or_copy(
        source,
        target,
        |source, target| {
            std::os::windows::fs::symlink_dir(source, target).map_err(|e| e.to_string())
        },
        create_directory_junction,
    )
}

#[cfg(windows)]
fn create_directory_junction(source: &Path, target: &Path) -> Result<(), String> {
    let output = std::process::Command::new("cmd")
        .arg("/C")
        .arg("mklink")
        .arg("/J")
        .arg(target)
        .arg(source)
        .output()
        .map_err(|e| format!("创建目录 junction 失败: {}", e))?;
    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(format!(
        "junction_status={}, stdout={}, stderr={}",
        output.status, stdout, stderr
    ))
}

#[cfg(windows)]
fn create_directory_shared_link_or_copy<S, J>(
    source: &Path,
    target: &Path,
    create_symlink: S,
    create_junction: J,
) -> Result<(), String>
where
    S: FnOnce(&Path, &Path) -> Result<(), String>,
    J: FnOnce(&Path, &Path) -> Result<(), String>,
{
    match create_symlink(source, target) {
        Ok(()) => Ok(()),
        Err(symlink_err) => {
            modules::logger::log_warn(&format!(
                "Windows directory symlink failed, falling back to junction: source={}, target={}, error={}",
                source.display(),
                target.display(),
                symlink_err
            ));
            match create_junction(source, target) {
                Ok(()) => Ok(()),
                Err(junction_err) => {
                    modules::logger::log_warn(&format!(
                        "Windows directory junction failed, copying shared directory instead: source={}, target={}, error={}",
                        source.display(),
                        target.display(),
                        junction_err
                    ));
                    prepare_directory_copy_fallback_target(target)?;
                    instance_store::copy_dir_recursive(source, target).map_err(|copy_err| {
                        format!(
                            "创建目录共享链接失败: symlink_error={}, junction_error={}, copy_error={}",
                            symlink_err, junction_err, copy_err
                        )
                    })
                }
            }
        }
    }
}

#[cfg(windows)]
fn prepare_directory_copy_fallback_target(target: &Path) -> Result<(), String> {
    if !target.exists() {
        return Ok(());
    }

    let metadata = fs::symlink_metadata(target).map_err(|e| {
        format!(
            "读取目录复制回退目标失败 ({}): {}",
            display_abs_path(target),
            e
        )
    })?;
    if metadata.file_type().is_symlink() {
        return remove_symlink(target);
    }
    if metadata.is_dir() && is_directory_empty(target)? {
        return fs::remove_dir(target).map_err(|e| {
            format!(
                "清理空目录复制回退目标失败 ({}): {}",
                display_abs_path(target),
                e
            )
        });
    }

    Err(format!(
        "目录复制回退目标已存在且不为空: {}",
        display_abs_path(target)
    ))
}

#[cfg(not(any(unix, windows)))]
fn create_directory_symlink(_source: &Path, _target: &Path) -> Result<(), String> {
    Err("当前系统不支持创建目录符号链接".to_string())
}

#[cfg(windows)]
fn create_directory_live_link(source: &Path, target: &Path) -> Result<(), String> {
    match std::os::windows::fs::symlink_dir(source, target) {
        Ok(()) => Ok(()),
        Err(symlink_err) => {
            modules::logger::log_warn(&format!(
                "Windows directory symlink failed for live shared history, falling back to junction: source={}, target={}, error={}",
                source.display(),
                target.display(),
                symlink_err
            ));
            create_directory_junction(source, target).map_err(|junction_err| {
                format!(
                    "create live shared history link failed: symlink_error={}, junction_error={}",
                    symlink_err, junction_err
                )
            })
        }
    }
}

#[cfg(unix)]
fn create_directory_live_link(source: &Path, target: &Path) -> Result<(), String> {
    create_directory_symlink(source, target)
}

#[cfg(not(any(unix, windows)))]
fn create_directory_live_link(_source: &Path, _target: &Path) -> Result<(), String> {
    Err("current system does not support live shared history links".to_string())
}

#[cfg(unix)]
fn create_file_symlink(source: &Path, target: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(source, target).map_err(|e| format!("创建文件共享链接失败: {}", e))
}

#[cfg(windows)]
fn create_file_symlink(source: &Path, target: &Path) -> Result<(), String> {
    match std::os::windows::fs::symlink_file(source, target) {
        Ok(()) => Ok(()),
        Err(symlink_err) => {
            modules::logger::log_warn(&format!(
                "Windows file symlink failed, falling back to hard link: source={}, target={}, error={}",
                source.display(),
                target.display(),
                symlink_err
            ));
            std::fs::hard_link(source, target).map_err(|hardlink_err| {
                format!(
                    "创建文件共享链接失败: symlink_error={}, hardlink_error={}",
                    symlink_err, hardlink_err
                )
            })
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn create_file_symlink(_source: &Path, _target: &Path) -> Result<(), String> {
    Err("当前系统不支持创建文件符号链接".to_string())
}

#[cfg(windows)]
fn create_file_live_link(source: &Path, target: &Path) -> Result<(), String> {
    create_shared_file_link_or_copy_with(source, target, |source, target| {
        match std::os::windows::fs::symlink_file(source, target) {
            Ok(()) => Ok(()),
            Err(symlink_err) => {
                modules::logger::log_warn(&format!(
                    "Windows live shared history file symlink failed, falling back to hard link: source={}, target={}, error={}",
                    source.display(),
                    target.display(),
                    symlink_err
                ));
                std::fs::hard_link(source, target).map_err(|hardlink_err| {
                    format!(
                        "create live shared history file link failed: symlink_error={}, hardlink_error={}",
                        symlink_err, hardlink_err
                    )
                })
            }
        }
    })
}

#[cfg(unix)]
fn create_file_live_link(source: &Path, target: &Path) -> Result<(), String> {
    create_file_symlink(source, target)
}

#[cfg(not(any(unix, windows)))]
fn create_file_live_link(_source: &Path, _target: &Path) -> Result<(), String> {
    Err("current system does not support live shared history file links".to_string())
}

fn create_shared_file_link_or_copy(source: &Path, target: &Path) -> Result<(), String> {
    create_shared_file_link_or_copy_with(source, target, create_file_symlink)
}

fn create_shared_file_link_or_copy_with<L>(
    source: &Path,
    target: &Path,
    create_link: L,
) -> Result<(), String>
where
    L: FnOnce(&Path, &Path) -> Result<(), String>,
{
    create_link(source, target).or_else(|link_err| {
        modules::logger::log_warn(&format!(
            "Shared file link failed, copying shared file instead: source={}, target={}, error={}",
            source.display(),
            target.display(),
            link_err
        ));
        fs::copy(source, target).map(|_| ()).map_err(|copy_err| {
            format!(
                "create shared file link or copy failed: link_error={}, copy_error={}",
                link_err, copy_err
            )
        })
    })
}
fn remove_symlink(path: &Path) -> Result<(), String> {
    fs::remove_file(path)
        .or_else(|_| fs::remove_dir(path))
        .map_err(|e| format!("移除已有共享链接失败: {}", e))
}

fn path_exists_or_is_link(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

fn is_directory_empty(path: &Path) -> Result<bool, String> {
    let mut iter = fs::read_dir(path).map_err(|e| format!("读取目录失败: {}", e))?;
    Ok(iter.next().is_none())
}

fn files_have_same_content(a: &Path, b: &Path) -> Result<bool, String> {
    let meta_a = fs::metadata(a).map_err(|e| format!("读取文件元数据失败: {}", e))?;
    let meta_b = fs::metadata(b).map_err(|e| format!("读取文件元数据失败: {}", e))?;
    if meta_a.len() != meta_b.len() {
        return Ok(false);
    }
    let bytes_a = fs::read(a).map_err(|e| format!("读取文件失败: {}", e))?;
    let bytes_b = fs::read(b).map_err(|e| format!("读取文件失败: {}", e))?;
    Ok(bytes_a == bytes_b)
}

fn sorted_entries(path: &Path) -> Result<Vec<fs::DirEntry>, String> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)
        .map_err(|e| format!("读取目录失败: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取目录项失败: {}", e))?;
    entries.sort_by(|a, b| {
        a.file_name()
            .to_string_lossy()
            .cmp(&b.file_name().to_string_lossy())
    });
    Ok(entries)
}

fn directories_are_equivalent(a: &Path, b: &Path) -> Result<bool, String> {
    let entries_a = sorted_entries(a)?;
    let entries_b = sorted_entries(b)?;
    if entries_a.len() != entries_b.len() {
        return Ok(false);
    }

    for (entry_a, entry_b) in entries_a.into_iter().zip(entries_b.into_iter()) {
        if entry_a.file_name() != entry_b.file_name() {
            return Ok(false);
        }

        let path_a = entry_a.path();
        let path_b = entry_b.path();
        let meta_a =
            fs::symlink_metadata(&path_a).map_err(|e| format!("读取路径元数据失败: {}", e))?;
        let meta_b =
            fs::symlink_metadata(&path_b).map_err(|e| format!("读取路径元数据失败: {}", e))?;
        let type_a = meta_a.file_type();
        let type_b = meta_b.file_type();

        if type_a.is_symlink() || type_b.is_symlink() {
            return Ok(false);
        }

        if type_a.is_dir() && type_b.is_dir() {
            if !directories_are_equivalent(&path_a, &path_b)? {
                return Ok(false);
            }
            continue;
        }

        if type_a.is_file() && type_b.is_file() {
            if !files_have_same_content(&path_a, &path_b)? {
                return Ok(false);
            }
            continue;
        }

        return Ok(false);
    }

    Ok(true)
}

fn paths_point_to_same_location(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(left), Ok(right)) => left == right,
        _ => a == b,
    }
}

#[cfg(windows)]
fn files_are_same_entry(a: &Path, b: &Path) -> Result<bool, String> {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    fn read_file_identity(path: &Path) -> Result<(u32, u32, u32), String> {
        let file = fs::File::open(path).map_err(|e| {
            format!(
                "read file identity open failed ({}): {}",
                display_abs_path(path),
                e
            )
        })?;
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        unsafe {
            GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &mut info).map_err(|e| {
                format!(
                    "read file identity failed ({}): {}",
                    display_abs_path(path),
                    e
                )
            })?;
        }
        Ok((
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ))
    }

    Ok(read_file_identity(a)? == read_file_identity(b)?)
}

#[cfg(unix)]
fn files_are_same_entry(a: &Path, b: &Path) -> Result<bool, String> {
    use std::os::unix::fs::MetadataExt;

    let meta_a = fs::metadata(a)
        .map_err(|e| format!("read file metadata failed ({}): {}", display_abs_path(a), e))?;
    let meta_b = fs::metadata(b)
        .map_err(|e| format!("read file metadata failed ({}): {}", display_abs_path(b), e))?;
    Ok(meta_a.dev() == meta_b.dev() && meta_a.ino() == meta_b.ino())
}

#[cfg(not(any(unix, windows)))]
fn files_are_same_entry(a: &Path, b: &Path) -> Result<bool, String> {
    Ok(paths_point_to_same_location(a, b))
}

fn display_abs_path(path: &Path) -> String {
    instance_store::display_path(path)
}

fn resolve_link_target(link_path: &Path, target: PathBuf) -> PathBuf {
    if target.is_absolute() {
        target
    } else {
        link_path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(target)
    }
}

fn sync_shared_directory(
    profile_dir: &Path,
    default_codex_home: &Path,
    relative_path: &Path,
) -> Result<(), String> {
    let global_dir = default_codex_home.join(relative_path);
    let instance_dir = profile_dir.join(relative_path);
    let relative_display = relative_path.to_string_lossy();

    fs::create_dir_all(&global_dir).map_err(|e| {
        format!(
            "创建全局共享目录失败 ({}): {}",
            display_abs_path(&global_dir),
            e
        )
    })?;
    if let Some(parent) = instance_dir.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "创建实例共享目录父路径失败 ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }

    if !instance_dir.exists() {
        return create_directory_symlink(&global_dir, &instance_dir);
    }

    let metadata = fs::symlink_metadata(&instance_dir).map_err(|e| {
        format!(
            "读取实例共享目录信息失败 ({}): {}",
            display_abs_path(&instance_dir),
            e
        )
    })?;
    if metadata.file_type().is_symlink() {
        let current_target = fs::read_link(&instance_dir).map_err(|e| {
            format!(
                "读取实例共享目录链接失败 ({}): {}",
                display_abs_path(&instance_dir),
                e
            )
        })?;
        let resolved_target = resolve_link_target(&instance_dir, current_target);
        if paths_point_to_same_location(&resolved_target, &global_dir) {
            return Ok(());
        }
        remove_symlink(&instance_dir)?;
        return create_directory_symlink(&global_dir, &instance_dir);
    }

    if !metadata.is_dir() {
        return Err(format!(
            "实例共享目录路径不是目录 ({}): {}",
            relative_display,
            display_abs_path(&instance_dir)
        ));
    }

    let instance_empty = is_directory_empty(&instance_dir)?;
    let global_empty = is_directory_empty(&global_dir)?;
    if instance_empty {
        fs::remove_dir(&instance_dir).map_err(|e| {
            format!(
                "清理空实例共享目录失败 ({}): {}",
                display_abs_path(&instance_dir),
                e
            )
        })?;
        return create_directory_symlink(&global_dir, &instance_dir);
    }

    if global_empty {
        fs::remove_dir(&global_dir).map_err(|e| {
            format!(
                "移除空全局共享目录失败 ({}): {}",
                display_abs_path(&global_dir),
                e
            )
        })?;
        instance_store::copy_dir_recursive(&instance_dir, &global_dir).map_err(|e| {
            format!(
                "迁移实例共享目录到全局失败 ({}): {}",
                display_abs_path(&instance_dir),
                e
            )
        })?;
        fs::remove_dir_all(&instance_dir).map_err(|e| {
            format!(
                "清理实例共享目录失败 ({}): {}",
                display_abs_path(&instance_dir),
                e
            )
        })?;
        return create_directory_symlink(&global_dir, &instance_dir);
    }

    if directories_are_equivalent(&instance_dir, &global_dir)? {
        fs::remove_dir_all(&instance_dir).map_err(|e| {
            format!(
                "清理实例共享目录失败 ({}): {}",
                display_abs_path(&instance_dir),
                e
            )
        })?;
        return create_directory_symlink(&global_dir, &instance_dir);
    }

    fs::remove_dir_all(&instance_dir).map_err(|e| {
        format!(
            "强制重建实例共享目录链接前清理实例目录失败 ({}): {}",
            display_abs_path(&instance_dir),
            e
        )
    })?;
    create_directory_symlink(&global_dir, &instance_dir).map_err(|e| {
        format!(
            "强制重建实例共享目录链接失败 ({} -> {}, {}): {}",
            display_abs_path(&global_dir),
            display_abs_path(&instance_dir),
            relative_display,
            e
        )
    })
}

fn copy_missing_directory_entries(source: &Path, target: &Path) -> Result<(), String> {
    if !source.exists() {
        return Ok(());
    }
    fs::create_dir_all(target).map_err(|e| {
        format!(
            "create shared directory merge target failed ({}): {}",
            display_abs_path(target),
            e
        )
    })?;

    for entry in fs::read_dir(source).map_err(|e| {
        format!(
            "read shared directory merge source failed ({}): {}",
            display_abs_path(source),
            e
        )
    })? {
        let entry = entry.map_err(|e| format!("read shared directory entry failed: {}", e))?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        let source_meta = fs::symlink_metadata(&source_path).map_err(|e| {
            format!(
                "read shared directory merge source entry failed ({}): {}",
                display_abs_path(&source_path),
                e
            )
        })?;

        if source_meta.is_dir() {
            copy_missing_directory_entries(&source_path, &target_path)?;
            continue;
        }

        if target_path.exists() {
            continue;
        }

        fs::copy(&source_path, &target_path).map_err(|e| {
            format!(
                "merge shared directory file failed ({} -> {}): {}",
                display_abs_path(&source_path),
                display_abs_path(&target_path),
                e
            )
        })?;
    }

    Ok(())
}

fn sync_shared_directory_preserving_entries(
    profile_dir: &Path,
    default_codex_home: &Path,
    relative_path: &Path,
) -> Result<(), String> {
    let global_dir = default_codex_home.join(relative_path);
    let instance_dir = profile_dir.join(relative_path);
    let relative_display = relative_path.to_string_lossy();

    fs::create_dir_all(&global_dir).map_err(|e| {
        format!(
            "create global shared directory failed ({}): {}",
            display_abs_path(&global_dir),
            e
        )
    })?;
    if let Some(parent) = instance_dir.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create instance shared directory parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }

    if !instance_dir.exists() {
        return create_directory_live_link(&global_dir, &instance_dir);
    }

    let metadata = fs::symlink_metadata(&instance_dir).map_err(|e| {
        format!(
            "read instance shared directory metadata failed ({}): {}",
            display_abs_path(&instance_dir),
            e
        )
    })?;
    if metadata.file_type().is_symlink() {
        let current_target = fs::read_link(&instance_dir).map_err(|e| {
            format!(
                "read instance shared directory link failed ({}): {}",
                display_abs_path(&instance_dir),
                e
            )
        })?;
        let resolved_target = resolve_link_target(&instance_dir, current_target);
        if paths_point_to_same_location(&resolved_target, &global_dir) {
            return Ok(());
        }
        remove_symlink(&instance_dir)?;
        return create_directory_live_link(&global_dir, &instance_dir);
    }

    if metadata.is_dir() && paths_point_to_same_location(&instance_dir, &global_dir) {
        return Ok(());
    }

    if !metadata.is_dir() {
        return Err(format!(
            "instance shared directory path is not a directory ({}): {}",
            relative_display,
            display_abs_path(&instance_dir)
        ));
    }

    copy_missing_directory_entries(&instance_dir, &global_dir)?;
    fs::remove_dir_all(&instance_dir).map_err(|e| {
        format!(
            "clean instance shared directory failed ({}): {}",
            display_abs_path(&instance_dir),
            e
        )
    })?;
    create_directory_live_link(&global_dir, &instance_dir).map_err(|e| {
        format!(
            "rebuild instance shared directory link failed ({} -> {}, {}): {}",
            display_abs_path(&global_dir),
            display_abs_path(&instance_dir),
            relative_display,
            e
        )
    })
}

fn sync_shared_file(
    profile_dir: &Path,
    default_codex_home: &Path,
    relative_path: &Path,
) -> Result<(), String> {
    let global_file = default_codex_home.join(relative_path);
    let instance_file = profile_dir.join(relative_path);
    let relative_display = relative_path.to_string_lossy();

    if let Some(parent) = global_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "创建全局共享文件父目录失败 ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }
    if let Some(parent) = instance_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "创建实例共享文件父目录失败 ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }

    if !global_file.exists() {
        if instance_file.exists() {
            let meta = fs::symlink_metadata(&instance_file).map_err(|e| {
                format!(
                    "读取实例共享文件信息失败 ({}): {}",
                    display_abs_path(&instance_file),
                    e
                )
            })?;
            if meta.file_type().is_symlink() {
                remove_symlink(&instance_file)?;
            } else if meta.is_file() {
                fs::copy(&instance_file, &global_file).map_err(|e| {
                    format!(
                        "迁移实例共享文件到全局失败 ({} -> {}): {}",
                        display_abs_path(&instance_file),
                        display_abs_path(&global_file),
                        e
                    )
                })?;
                fs::remove_file(&instance_file).map_err(|e| {
                    format!(
                        "清理实例共享文件失败 ({}): {}",
                        display_abs_path(&instance_file),
                        e
                    )
                })?;
            } else {
                return Err(format!(
                    "实例共享文件路径不是文件 ({}): {}",
                    relative_display,
                    display_abs_path(&instance_file)
                ));
            }
        } else {
            return Ok(());
        }
    }

    let global_meta = fs::metadata(&global_file).map_err(|e| {
        format!(
            "读取全局共享文件信息失败 ({}): {}",
            display_abs_path(&global_file),
            e
        )
    })?;
    if !global_meta.is_file() {
        return Err(format!(
            "全局共享路径不是文件 ({}): {}",
            relative_display,
            display_abs_path(&global_file)
        ));
    }

    if !instance_file.exists() {
        return create_shared_file_link_or_copy(&global_file, &instance_file);
    }

    let instance_meta = fs::symlink_metadata(&instance_file).map_err(|e| {
        format!(
            "读取实例共享文件信息失败 ({}): {}",
            display_abs_path(&instance_file),
            e
        )
    })?;
    if instance_meta.file_type().is_symlink() {
        let current_target = fs::read_link(&instance_file).map_err(|e| {
            format!(
                "读取实例共享文件链接失败 ({}): {}",
                display_abs_path(&instance_file),
                e
            )
        })?;
        let resolved_target = resolve_link_target(&instance_file, current_target);
        if paths_point_to_same_location(&resolved_target, &global_file) {
            return Ok(());
        }
        remove_symlink(&instance_file)?;
        return create_shared_file_link_or_copy(&global_file, &instance_file);
    }

    if instance_meta.is_file() && files_are_same_entry(&instance_file, &global_file)? {
        return Ok(());
    }

    if !instance_meta.is_file() {
        return Err(format!(
            "实例共享文件路径不是文件 ({}): {}",
            relative_display,
            display_abs_path(&instance_file)
        ));
    }

    if files_have_same_content(&instance_file, &global_file)? {
        fs::remove_file(&instance_file).map_err(|e| {
            format!(
                "清理实例共享文件失败 ({}): {}",
                display_abs_path(&instance_file),
                e
            )
        })?;
        return create_shared_file_link_or_copy(&global_file, &instance_file);
    }

    fs::remove_file(&instance_file).map_err(|e| {
        format!(
            "强制重建实例共享文件链接前清理实例文件失败 ({}): {}",
            display_abs_path(&instance_file),
            e
        )
    })?;
    create_shared_file_link_or_copy(&global_file, &instance_file).map_err(|e| {
        format!(
            "强制重建实例共享文件链接失败 ({} -> {}, {}): {}",
            display_abs_path(&global_file),
            display_abs_path(&instance_file),
            relative_display,
            e
        )
    })
}

fn backup_shared_history_path(
    profile_dir: &Path,
    relative_path: &Path,
    path: &Path,
    default_path: &Path,
    reason: &str,
) -> Result<(), String> {
    let backup_dir = profile_dir
        .join(CODEX_SHARED_HISTORY_BACKUP_DIR_NAME)
        .join(Utc::now().format("%Y%m%d-%H%M%S-%3f").to_string());
    fs::create_dir_all(&backup_dir).map_err(|e| {
        format!(
            "create shared history backup directory failed ({}): {}",
            display_abs_path(&backup_dir),
            e
        )
    })?;

    let manifest = json!({
        "created_at": Utc::now().to_rfc3339(),
        "reason": reason,
        "relative_path": relative_path.to_string_lossy(),
        "instance_path": path.to_string_lossy(),
        "default_path": default_path.to_string_lossy(),
    });
    fs::write(
        backup_dir.join("manifest.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&manifest)
                .map_err(|e| format!("serialize shared history backup manifest failed: {}", e))?
        ),
    )
    .map_err(|e| format!("write shared history backup manifest failed: {}", e))?;

    if path.is_file() {
        let backup_file = backup_dir.join("files").join(relative_path);
        if let Some(parent) = backup_file.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "create shared history backup file parent failed ({}): {}",
                    display_abs_path(parent),
                    e
                )
            })?;
        }
        fs::copy(path, &backup_file).map_err(|e| {
            format!(
                "copy shared history backup file failed ({} -> {}): {}",
                display_abs_path(path),
                display_abs_path(&backup_file),
                e
            )
        })?;
    }

    Ok(())
}

fn read_jsonl_entries_by_id(
    path: &Path,
) -> Result<(Vec<String>, HashMap<String, JsonValue>), String> {
    if !path.exists() {
        return Ok((Vec::new(), HashMap::new()));
    }

    let content = fs::read_to_string(path).map_err(|e| {
        format!(
            "read shared session index failed ({}): {}",
            display_abs_path(path),
            e
        )
    })?;
    let mut order = Vec::new();
    let mut entries = HashMap::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        let Some(id) = entry
            .get("id")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if !entries.contains_key(id) {
            order.push(id.to_string());
        }
        entries.insert(id.to_string(), entry);
    }
    Ok((order, entries))
}

fn session_index_updated_at(entry: &JsonValue) -> i64 {
    entry
        .get("updated_at")
        .or_else(|| entry.get("updatedAt"))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
        })
        .unwrap_or_default()
}

fn session_index_entry_is_materialized_shared_chat(entry: &JsonValue) -> bool {
    entry.get("cockpit_shared_chat").is_some()
}

fn merge_session_index_into_default(
    profile_dir: &Path,
    default_codex_home: &Path,
) -> Result<(), String> {
    let instance_file = profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME);
    let global_file = default_codex_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME);
    if !instance_file.exists() || paths_point_to_same_location(&instance_file, &global_file) {
        return Ok(());
    }
    if let Some(parent) = global_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create global session index parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }
    if !global_file.exists() {
        fs::copy(&instance_file, &global_file).map_err(|e| {
            format!(
                "promote instance session index failed ({} -> {}): {}",
                display_abs_path(&instance_file),
                display_abs_path(&global_file),
                e
            )
        })?;
        return Ok(());
    }

    let (mut order, mut entries) = read_jsonl_entries_by_id(&global_file)?;
    let (source_order, source_entries) = read_jsonl_entries_by_id(&instance_file)?;
    let mut changed = false;
    for id in source_order {
        let Some(source_entry) = source_entries.get(&id) else {
            continue;
        };
        if session_index_entry_is_materialized_shared_chat(source_entry) {
            continue;
        }
        let replace = entries
            .get(&id)
            .map(|target_entry| {
                session_index_updated_at(source_entry) > session_index_updated_at(target_entry)
            })
            .unwrap_or(true);
        if replace {
            if !entries.contains_key(&id) {
                order.push(id.clone());
            }
            entries.insert(id, source_entry.clone());
            changed = true;
        }
    }
    if !changed {
        return Ok(());
    }

    let lines = order
        .into_iter()
        .filter_map(|id| entries.get(&id).cloned())
        .map(|entry| serde_json::to_string(&entry))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("serialize merged session index failed: {}", e))?;
    fs::write(&global_file, format!("{}\n", lines.join("\n"))).map_err(|e| {
        format!(
            "write merged session index failed ({}): {}",
            display_abs_path(&global_file),
            e
        )
    })
}

fn merge_json_string_array(target: &mut JsonValue, source: &JsonValue, key: &str) {
    let Some(object) = target.as_object_mut() else {
        return;
    };
    let mut values = object
        .get(key)
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect::<Vec<_>>();
    let mut seen = values.iter().cloned().collect::<HashSet<_>>();
    if let Some(source_values) = source.get(key).and_then(JsonValue::as_array) {
        for value in source_values {
            let Some(text) = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            if seen.insert(text.to_string()) {
                values.push(text.to_string());
            }
        }
    }
    object.insert(
        key.to_string(),
        JsonValue::Array(values.into_iter().map(JsonValue::String).collect()),
    );
}

fn merge_global_state_into_default(
    profile_dir: &Path,
    default_codex_home: &Path,
) -> Result<(), String> {
    let instance_file = profile_dir.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME);
    let global_file = default_codex_home.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME);
    if !instance_file.exists() || paths_point_to_same_location(&instance_file, &global_file) {
        return Ok(());
    }
    if let Some(parent) = global_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create global state parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }
    if !global_file.exists() {
        fs::copy(&instance_file, &global_file).map_err(|e| {
            format!(
                "promote instance global state failed ({} -> {}): {}",
                display_abs_path(&instance_file),
                display_abs_path(&global_file),
                e
            )
        })?;
        return Ok(());
    }

    let mut global = fs::read_to_string(&global_file)
        .ok()
        .and_then(|content| serde_json::from_str::<JsonValue>(&content).ok())
        .filter(JsonValue::is_object)
        .unwrap_or_else(|| json!({}));
    let source = fs::read_to_string(&instance_file)
        .ok()
        .and_then(|content| serde_json::from_str::<JsonValue>(&content).ok())
        .filter(JsonValue::is_object)
        .unwrap_or_else(|| json!({}));
    merge_json_string_array(&mut global, &source, "project-order");
    merge_json_string_array(&mut global, &source, "electron-saved-workspace-roots");
    fs::write(
        &global_file,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&global)
                .map_err(|e| format!("serialize merged global state failed: {}", e))?
        ),
    )
    .map_err(|e| {
        format!(
            "write merged global state failed ({}): {}",
            display_abs_path(&global_file),
            e
        )
    })
}

fn ensure_global_history_file(
    default_codex_home: &Path,
    relative_path: &Path,
    default_content: &str,
) -> Result<(), String> {
    let path = default_codex_home.join(relative_path);
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create global history file parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }
    fs::write(&path, default_content).map_err(|e| {
        format!(
            "create global history file failed ({}): {}",
            display_abs_path(&path),
            e
        )
    })
}

fn sqlite_thread_columns(connection: &Connection) -> Result<Vec<String>, String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(threads)")
        .map_err(|e| format!("read threads schema failed: {}", e))?;
    let mut rows = statement
        .query([])
        .map_err(|e| format!("query threads schema failed: {}", e))?;
    let mut columns = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("iterate threads schema failed: {}", e))?
    {
        columns.push(
            row.get::<usize, String>(1)
                .map_err(|e| format!("read threads column failed: {}", e))?,
        );
    }
    if columns.is_empty() {
        return Err("threads table is missing".to_string());
    }
    Ok(columns)
}

fn quote_sqlite_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sqlite_text_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn sqlite_i64_value(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value),
        Value::Text(value) => value.parse::<i64>().ok(),
        _ => None,
    }
}

fn rewrite_instance_history_path_to_default(
    profile_dir: &Path,
    default_codex_home: &Path,
    value: &str,
) -> String {
    let path = PathBuf::from(value);
    if let Ok(relative) = path.strip_prefix(profile_dir) {
        return default_codex_home
            .join(relative)
            .to_string_lossy()
            .to_string();
    }
    value.to_string()
}

fn merge_state_db_into_default(
    profile_dir: &Path,
    default_codex_home: &Path,
) -> Result<(), String> {
    let instance_db = profile_dir.join(CODEX_SHARED_STATE_DB_FILE_NAME);
    let global_db = default_codex_home.join(CODEX_SHARED_STATE_DB_FILE_NAME);
    if !instance_db.exists() || paths_point_to_same_location(&instance_db, &global_db) {
        return Ok(());
    }
    if let Some(parent) = global_db.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create global state database parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }
    if !global_db.exists() {
        fs::copy(&instance_db, &global_db).map_err(|e| {
            format!(
                "promote instance state database failed ({} -> {}): {}",
                display_abs_path(&instance_db),
                display_abs_path(&global_db),
                e
            )
        })?;
        return Ok(());
    }

    let (_, source_index_entries) =
        read_jsonl_entries_by_id(&profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME))?;
    let source = Connection::open_with_flags(&instance_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| {
            format!(
                "open instance state database failed ({}): {}",
                display_abs_path(&instance_db),
                e
            )
        })?;
    let target = Connection::open(&global_db).map_err(|e| {
        format!(
            "open global state database failed ({}): {}",
            display_abs_path(&global_db),
            e
        )
    })?;
    target
        .busy_timeout(Duration::from_secs(3))
        .map_err(|e| format!("set global state database busy timeout failed: {}", e))?;

    let source_columns = sqlite_thread_columns(&source)?;
    let target_columns = sqlite_thread_columns(&target)?;
    let target_column_set = target_columns.iter().cloned().collect::<HashSet<_>>();
    let common_columns = source_columns
        .into_iter()
        .filter(|column| target_column_set.contains(column))
        .collect::<Vec<_>>();
    if !common_columns.iter().any(|column| column == "id") {
        return Ok(());
    }

    let select_columns = common_columns
        .iter()
        .map(|column| quote_sqlite_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut statement = source
        .prepare(&format!("SELECT {} FROM threads", select_columns))
        .map_err(|e| format!("prepare instance thread rows failed: {}", e))?;
    let mut rows = statement
        .query([])
        .map_err(|e| format!("query instance thread rows failed: {}", e))?;

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("iterate instance thread rows failed: {}", e))?
    {
        let mut values = Vec::with_capacity(common_columns.len());
        for index in 0..common_columns.len() {
            values.push(
                row.get::<usize, Value>(index)
                    .map_err(|e| format!("read instance thread value failed: {}", e))?,
            );
        }
        let Some(id_index) = common_columns.iter().position(|column| column == "id") else {
            continue;
        };
        let Some(id) = sqlite_text_value(&values[id_index]) else {
            continue;
        };
        if source_index_entries
            .get(&id)
            .map(session_index_entry_is_materialized_shared_chat)
            .unwrap_or(false)
        {
            continue;
        }
        if common_columns
            .iter()
            .position(|column| column == "thread_source")
            .and_then(|index| sqlite_text_value(&values[index]))
            .as_deref()
            == Some(CODEX_SHARED_CHAT_THREAD_SOURCE)
        {
            continue;
        }
        let source_updated_at = common_columns
            .iter()
            .position(|column| column == "updated_at")
            .and_then(|index| sqlite_i64_value(&values[index]))
            .unwrap_or_default();
        let target_updated_at = target
            .query_row(
                "SELECT updated_at FROM threads WHERE id = ?1",
                [&id],
                |row| row.get::<usize, Option<i64>>(0),
            )
            .ok()
            .flatten();
        if target_updated_at
            .map(|updated_at| updated_at >= source_updated_at)
            .unwrap_or(false)
        {
            continue;
        }
        if let Some(rollout_index) = common_columns
            .iter()
            .position(|column| column == "rollout_path")
        {
            if let Some(path) = sqlite_text_value(&values[rollout_index]) {
                values[rollout_index] = Value::Text(rewrite_instance_history_path_to_default(
                    profile_dir,
                    default_codex_home,
                    &path,
                ));
            }
        }

        let placeholders = vec!["?"; common_columns.len()].join(", ");
        let sql = format!(
            "INSERT OR REPLACE INTO threads ({}) VALUES ({})",
            common_columns
                .iter()
                .map(|column| quote_sqlite_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders
        );
        target
            .execute(&sql, params_from_iter(values.iter()))
            .map_err(|e| format!("merge instance thread row failed ({}): {}", id, e))?;
    }

    Ok(())
}

fn rewrite_session_index_entries(
    root_dir: &Path,
    order: Vec<String>,
    entries: HashMap<String, JsonValue>,
) -> Result<(), String> {
    let path = root_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME);
    let lines = order
        .into_iter()
        .filter_map(|id| entries.get(&id).cloned())
        .map(|entry| serde_json::to_string(&entry))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("serialize pruned session index failed: {}", e))?;
    fs::write(&path, format!("{}\n", lines.join("\n"))).map_err(|e| {
        format!(
            "write pruned session index failed ({}): {}",
            display_abs_path(&path),
            e
        )
    })
}

fn prune_materialized_shared_chat_entries(root_dir: &Path) -> Result<(), String> {
    let (mut order, mut entries) =
        read_jsonl_entries_by_id(&root_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME))?;
    let mut materialized_ids = entries
        .iter()
        .filter_map(|(id, entry)| {
            session_index_entry_is_materialized_shared_chat(entry).then(|| id.clone())
        })
        .collect::<HashSet<_>>();

    let db_path = root_dir.join(CODEX_SHARED_STATE_DB_FILE_NAME);
    if db_path.exists() {
        let connection = Connection::open(&db_path).map_err(|e| {
            format!(
                "open canonical state database for pruning failed ({}): {}",
                display_abs_path(&db_path),
                e
            )
        })?;
        connection
            .busy_timeout(Duration::from_secs(3))
            .map_err(|e| format!("set canonical prune database busy timeout failed: {}", e))?;
        if let Ok(columns) = sqlite_thread_columns(&connection) {
            if columns.iter().any(|column| column == "thread_source") {
                let mut statement = connection
                    .prepare("SELECT id FROM threads WHERE thread_source = ?1")
                    .map_err(|e| format!("prepare materialized thread scan failed: {}", e))?;
                let ids = statement
                    .query_map([CODEX_SHARED_CHAT_THREAD_SOURCE], |row| {
                        row.get::<usize, String>(0)
                    })
                    .map_err(|e| format!("query materialized thread ids failed: {}", e))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("read materialized thread id failed: {}", e))?;
                materialized_ids.extend(ids);
            }
            for id in &materialized_ids {
                connection
                    .execute("DELETE FROM threads WHERE id = ?1", [id])
                    .map_err(|e| {
                        format!("delete materialized thread row failed ({}): {}", id, e)
                    })?;
            }
        }
    }

    if materialized_ids.is_empty() {
        return Ok(());
    }
    order.retain(|id| !materialized_ids.contains(id));
    entries.retain(|id, _| !materialized_ids.contains(id));
    rewrite_session_index_entries(root_dir, order, entries)
}

fn sync_shared_live_file(
    profile_dir: &Path,
    default_codex_home: &Path,
    relative_path: &Path,
    allow_missing_target: bool,
) -> Result<(), String> {
    let global_file = default_codex_home.join(relative_path);
    let instance_file = profile_dir.join(relative_path);
    if let Some(parent) = global_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create global live file parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }
    if let Some(parent) = instance_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create instance live file parent failed ({}): {}",
                display_abs_path(parent),
                e
            )
        })?;
    }

    if path_exists_or_is_link(&instance_file) {
        let metadata = fs::symlink_metadata(&instance_file).map_err(|e| {
            format!(
                "read instance live file metadata failed ({}): {}",
                display_abs_path(&instance_file),
                e
            )
        })?;
        if metadata.file_type().is_symlink() {
            let current_target = fs::read_link(&instance_file).map_err(|e| {
                format!(
                    "read instance live file link failed ({}): {}",
                    display_abs_path(&instance_file),
                    e
                )
            })?;
            let resolved_target = resolve_link_target(&instance_file, current_target);
            if paths_point_to_same_location(&resolved_target, &global_file)
                || resolved_target == global_file
            {
                return Ok(());
            }
            remove_symlink(&instance_file)?;
        } else {
            backup_shared_history_path(
                profile_dir,
                relative_path,
                &instance_file,
                &global_file,
                "replace isolated history file with canonical live link",
            )?;
            remove_path_safely(&instance_file)?;
        }
    }

    if !global_file.exists() && allow_missing_target {
        fs::write(&global_file, "").map_err(|e| {
            format!(
                "create empty global live file failed ({}): {}",
                display_abs_path(&global_file),
                e
            )
        })?;
    }

    if global_file.exists() {
        create_file_live_link(&global_file, &instance_file)
    } else {
        Ok(())
    }
}

fn sync_shared_history_file(
    profile_dir: &Path,
    default_codex_home: &Path,
    relative_path: &Path,
) -> Result<(), String> {
    sync_shared_live_file(profile_dir, default_codex_home, relative_path, false)
}

fn sync_shared_sqlite_history(profile_dir: &Path, default_codex_home: &Path) -> Result<(), String> {
    sync_shared_live_file(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_STATE_DB_FILE_NAME),
        true,
    )?;
    sync_shared_live_file(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_STATE_DB_WAL_FILE_NAME),
        true,
    )?;
    sync_shared_live_file(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_STATE_DB_SHM_FILE_NAME),
        true,
    )
}

fn ensure_instance_history_shared(
    profile_dir: &Path,
    default_codex_home: &Path,
) -> Result<(), String> {
    sync_shared_directory_preserving_entries(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_SESSIONS_DIR_NAME),
    )?;
    sync_shared_directory_preserving_entries(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_ARCHIVED_SESSIONS_DIR_NAME),
    )?;
    merge_session_index_into_default(profile_dir, default_codex_home)?;
    merge_global_state_into_default(profile_dir, default_codex_home)?;
    merge_state_db_into_default(profile_dir, default_codex_home)?;
    prune_materialized_shared_chat_entries(default_codex_home)?;
    ensure_global_history_file(
        default_codex_home,
        Path::new(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
        "",
    )?;
    sync_shared_history_file(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
    )?;
    ensure_global_history_file(
        default_codex_home,
        Path::new(CODEX_SHARED_GLOBAL_STATE_FILE_NAME),
        "{}\n",
    )?;
    sync_shared_history_file(
        profile_dir,
        default_codex_home,
        Path::new(CODEX_SHARED_GLOBAL_STATE_FILE_NAME),
    )?;
    sync_shared_sqlite_history(profile_dir, default_codex_home)?;
    Ok(())
}

pub fn ensure_instance_shared_skills(profile_dir: &Path) -> Result<(), String> {
    let default_codex_home = get_default_codex_home()?;
    if paths_point_to_same_location(profile_dir, &default_codex_home) {
        return Ok(());
    }
    fs::create_dir_all(profile_dir).map_err(|e| format!("创建实例目录失败: {}", e))?;

    sync_shared_directory(
        profile_dir,
        &default_codex_home,
        Path::new(CODEX_SHARED_SKILLS_DIR_NAME),
    )?;
    sync_shared_directory(
        profile_dir,
        &default_codex_home,
        Path::new(CODEX_SHARED_RULES_DIR_NAME),
    )?;
    sync_shared_directory(
        profile_dir,
        &default_codex_home,
        Path::new(CODEX_SHARED_VENDOR_IMPORTS_SKILLS_DIR),
    )?;
    sync_shared_file(
        profile_dir,
        &default_codex_home,
        Path::new(CODEX_SHARED_AGENTS_FILE_NAME),
    )?;
    ensure_instance_history_shared(profile_dir, &default_codex_home)?;

    Ok(())
}

pub fn create_instance(params: CreateInstanceParams) -> Result<InstanceProfile, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;

    let name = instance_store::normalize_name(&params.name)?;
    let user_data_dir = params.user_data_dir.trim().to_string();
    if user_data_dir.is_empty() {
        return Err("实例目录不能为空".to_string());
    }

    instance_store::ensure_unique(&store, &name, &user_data_dir, None)?;

    let user_dir_path = PathBuf::from(&user_data_dir);
    let init_mode = params
        .init_mode
        .as_deref()
        .unwrap_or("copy")
        .to_ascii_lowercase();
    let create_empty = init_mode == "empty";
    let use_existing_dir = init_mode == "existingdir" || init_mode == "existing_dir";

    if use_existing_dir {
        if !user_dir_path.exists() {
            let resolved = instance_store::display_path(&user_dir_path);
            return Err(format!("所选目录不存在: {}", resolved));
        }
        if !user_dir_path.is_dir() {
            return Err("所选路径不是目录".to_string());
        }
    } else if create_empty {
        if user_dir_path.exists() {
            let mut has_entries = false;
            if let Ok(mut iter) = fs::read_dir(&user_dir_path) {
                if iter.next().is_some() {
                    has_entries = true;
                }
            }
            if has_entries {
                let resolved_path = instance_store::display_path(&user_dir_path);
                return Err(format!("空白实例需要目标目录为空: {}", resolved_path));
            }
        }
        fs::create_dir_all(&user_dir_path).map_err(|e| format!("创建实例目录失败: {}", e))?;
    } else {
        let source_dir = match params.copy_source_instance_id.as_deref() {
            Some("__default__") | None => get_default_codex_home()?,
            Some(source_id) => {
                let source_instance = store
                    .instances
                    .iter()
                    .find(|item| item.id == source_id)
                    .ok_or("复制来源实例不存在")?;
                PathBuf::from(&source_instance.user_data_dir)
            }
        };

        if user_dir_path.exists() {
            let mut has_entries = false;
            if let Ok(mut iter) = fs::read_dir(&user_dir_path) {
                if iter.next().is_some() {
                    has_entries = true;
                }
            }
            if has_entries {
                let resolved_path = instance_store::display_path(&user_dir_path);
                modules::logger::log_info(&format!(
                    "[Codex Instance] 复制来源实例需要空目录，但目标已存在: {}",
                    resolved_path
                ));
                return Err(format!("复制来源实例需要目标目录为空: {}", resolved_path));
            }
        }

        if !source_dir.exists() {
            return Err("未找到复制来源目录，请先确保来源实例已初始化".to_string());
        }

        instance_store::copy_dir_recursive(&source_dir, &user_dir_path)?;
        remove_path_safely(&user_dir_path.join(CODEX_ELECTRON_USER_DATA_DIR_NAME))?;
    }

    ensure_instance_shared_skills(&user_dir_path)?;

    let instance = InstanceProfile {
        id: Uuid::new_v4().to_string(),
        name,
        user_data_dir,
        working_dir: params.working_dir,
        extra_args: params.extra_args.trim().to_string(),
        bind_account_id: if create_empty {
            None
        } else {
            params.bind_account_id
        },
        launch_mode: params.launch_mode.unwrap_or_default(),
        created_at: Utc::now().timestamp_millis(),
        last_launched_at: None,
        last_pid: None,
    };

    store.instances.push(instance.clone());
    save_instance_store(&store)?;
    Ok(instance)
}

pub fn update_instance(params: UpdateInstanceParams) -> Result<InstanceProfile, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let index = store
        .instances
        .iter()
        .position(|instance| instance.id == params.instance_id)
        .ok_or("实例不存在")?;

    let current_id = store.instances[index].id.clone();
    let current_dir = store.instances[index].user_data_dir.clone();
    let next_name = params
        .name
        .as_ref()
        .map(|name| instance_store::normalize_name(name))
        .transpose()?;

    if let Some(ref normalized) = next_name {
        instance_store::ensure_unique(&store, normalized, &current_dir, Some(&current_id))?;
    }

    let instance = &mut store.instances[index];
    if let Some(normalized) = next_name {
        instance.name = normalized;
    }
    if let Some(ref extra_args) = params.extra_args {
        instance.extra_args = extra_args.trim().to_string();
    }
    if let Some(working_dir) = params.working_dir {
        instance.working_dir = if working_dir.trim().is_empty() {
            None
        } else {
            Some(working_dir.trim().to_string())
        };
    }
    if let Some(bind) = params.bind_account_id.clone() {
        instance.bind_account_id = bind;
    }
    if let Some(mode) = params.launch_mode {
        instance.launch_mode = mode;
    }

    let updated = instance.clone();
    save_instance_store(&store)?;
    Ok(updated)
}

pub fn delete_instance(instance_id: &str) -> Result<(), String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let index = store
        .instances
        .iter()
        .position(|instance| instance.id == instance_id)
        .ok_or("实例不存在")?;
    let user_data_dir = store.instances[index].user_data_dir.clone();

    if !user_data_dir.trim().is_empty() {
        let dir_path = PathBuf::from(&user_data_dir);
        modules::instance::delete_instance_directory(&dir_path)?;
    }

    store.instances.remove(index);
    save_instance_store(&store)?;
    Ok(())
}

pub fn update_instance_after_start_resolved(
    instance_id: &str,
    pid: Option<u32>,
) -> Result<InstanceProfile, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let mut updated = None;
    for instance in &mut store.instances {
        if instance.id == instance_id {
            instance.last_launched_at = Some(Utc::now().timestamp_millis());
            instance.last_pid = pid;
            updated = Some(instance.clone());
            break;
        }
    }
    let updated = updated.ok_or("实例不存在")?;
    save_instance_store(&store)?;
    Ok(updated)
}

pub fn update_instance_after_cli_prepare(instance_id: &str) -> Result<InstanceProfile, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let mut updated = None;
    for instance in &mut store.instances {
        if instance.id == instance_id {
            instance.last_launched_at = Some(Utc::now().timestamp_millis());
            instance.last_pid = None;
            updated = Some(instance.clone());
            break;
        }
    }
    let updated = updated.ok_or("实例不存在")?;
    save_instance_store(&store)?;
    Ok(updated)
}

pub fn update_instance_pid(instance_id: &str, pid: Option<u32>) -> Result<InstanceProfile, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let mut updated = None;
    for instance in &mut store.instances {
        if instance.id == instance_id {
            instance.last_pid = pid;
            updated = Some(instance.clone());
            break;
        }
    }
    let updated = updated.ok_or("实例不存在")?;
    save_instance_store(&store)?;
    Ok(updated)
}

pub fn update_default_pid(pid: Option<u32>) -> Result<DefaultInstanceSettings, String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    store.default_settings.last_pid = pid;
    let updated = store.default_settings.clone();
    save_instance_store(&store)?;
    Ok(updated)
}

pub fn clear_all_pids() -> Result<(), String> {
    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    store.default_settings.last_pid = None;
    for instance in &mut store.instances {
        instance.last_pid = None;
    }
    save_instance_store(&store)?;
    Ok(())
}

pub fn replace_bind_account_references(
    old_account_id: &str,
    new_account_id: &str,
) -> Result<(), String> {
    let old_id = old_account_id.trim();
    let new_id = new_account_id.trim();
    if old_id.is_empty() || new_id.is_empty() || old_id == new_id {
        return Ok(());
    }

    let _lock = CODEX_INSTANCE_STORE_LOCK
        .lock()
        .map_err(|_| "无法获取实例锁")?;
    let mut store = load_instance_store()?;
    let mut changed = false;

    if store.default_settings.bind_account_id.as_deref() == Some(old_id) {
        store.default_settings.bind_account_id = Some(new_id.to_string());
        store.default_settings.follow_local_account = false;
        changed = true;
    }

    for instance in &mut store.instances {
        if instance.bind_account_id.as_deref() == Some(old_id) {
            instance.bind_account_id = Some(new_id.to_string());
            changed = true;
        }
    }

    if changed {
        save_instance_store(&store)?;
    }

    Ok(())
}

pub async fn inject_account_to_profile(profile_dir: &Path, account_id: &str) -> Result<(), String> {
    modules::codex_account::prepare_account_for_injection_from_auth_dir(
        account_id,
        Some(profile_dir),
    )
    .await
    .map(|_| ())
}

#[cfg(all(test, target_os = "windows"))]
mod tests {
    use super::*;
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

    fn assert_live_shared_file(instance_file: &Path, global_file: &Path) {
        let metadata = fs::symlink_metadata(instance_file).expect("read live shared file metadata");
        if metadata.file_type().is_symlink() {
            return;
        }
        assert!(metadata.is_file(), "live shared path should be a file");
        assert!(
            global_file.exists(),
            "canonical live shared file should exist"
        );
        assert!(
            files_are_same_entry(instance_file, global_file).unwrap_or(false)
                || files_have_same_content(instance_file, global_file).unwrap_or(false),
            "live shared file should be linked or copied from canonical file"
        );
    }

    fn create_test_state_db(root: &Path) {
        fs::create_dir_all(root).expect("create state db root");
        let connection = Connection::open(root.join(CODEX_SHARED_STATE_DB_FILE_NAME))
            .expect("open test state db");
        connection
            .execute_batch(
                r#"
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT NOT NULL,
                    updated_at INTEGER,
                    cwd TEXT,
                    title TEXT,
                    thread_source TEXT
                );
                "#,
            )
            .expect("create test threads table");
    }

    fn insert_test_thread(root: &Path, id: &str, updated_at: i64, title: &str) {
        let rollout_dir = root
            .join(CODEX_SHARED_SESSIONS_DIR_NAME)
            .join("2026")
            .join("05")
            .join("11");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join(format!("rollout-{}.jsonl", id));
        fs::write(&rollout_path, format!("{{\"id\":\"{}\"}}\n", id)).expect("write rollout");
        let connection = Connection::open(root.join(CODEX_SHARED_STATE_DB_FILE_NAME))
            .expect("open test state db");
        connection
            .execute(
                "INSERT OR REPLACE INTO threads (id, rollout_path, updated_at, cwd, title) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    id,
                    rollout_path.to_string_lossy().to_string(),
                    updated_at,
                    "C:\\workspace",
                    title,
                ],
            )
            .expect("insert test thread");
    }

    fn test_thread_ids(root: &Path) -> Vec<String> {
        let connection = Connection::open(root.join(CODEX_SHARED_STATE_DB_FILE_NAME))
            .expect("open test state db");
        let mut statement = connection
            .prepare("SELECT id FROM threads ORDER BY id")
            .expect("prepare ids");
        statement
            .query_map([], |row| row.get::<usize, String>(0))
            .expect("query ids")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect ids")
    }

    #[test]
    fn windows_default_codex_home_ignores_inherited_codex_home() {
        let root = make_temp_dir("codex-default-home-env-test");
        let inherited = root.join("inherited-codex-home");
        let original = std::env::var_os("CODEX_HOME");
        std::env::set_var("CODEX_HOME", &inherited);

        let default_home = get_default_codex_home().expect("default Codex home");

        match original {
            Some(value) => std::env::set_var("CODEX_HOME", value),
            None => std::env::remove_var("CODEX_HOME"),
        }
        assert_ne!(default_home, inherited);
        assert_eq!(
            default_home.file_name().and_then(|value| value.to_str()),
            Some(".codex")
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_default_profile_uses_os_electron_user_data_dir() {
        let default_home = get_default_codex_home().expect("default Codex home");
        let appdata = std::env::var("APPDATA").expect("APPDATA must be set on Windows");
        let expected = PathBuf::from(appdata).join("Codex");

        assert_eq!(electron_user_data_dir_for_profile(&default_home), expected);
    }

    #[test]
    fn windows_custom_profile_uses_instance_electron_user_data_dir() {
        let root = make_temp_dir("codex-custom-electron-user-data-test");
        let profile_dir = root.join("instance");

        assert_eq!(
            electron_user_data_dir_for_profile(&profile_dir),
            profile_dir.join("electron-user-data")
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_directory_shared_link_falls_back_without_admin_symlink_privilege() {
        let root = make_temp_dir("codex-dir-link-test");
        let source = root.join("global-skills");
        let target = root.join("instance-skills");
        fs::create_dir_all(&source).expect("create source dir");

        create_directory_symlink(&source, &target).expect("create shared directory link");
        fs::write(source.join("probe.txt"), "shared").expect("write source probe");

        let content = fs::read_to_string(target.join("probe.txt")).expect("read through link");
        assert_eq!(content, "shared");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_file_shared_link_falls_back_without_admin_symlink_privilege() {
        let root = make_temp_dir("codex-file-link-test");
        let source = root.join("AGENTS.md");
        let target = root.join("instance-AGENTS.md");
        fs::write(&source, "shared").expect("write source file");

        create_file_symlink(&source, &target).expect("create shared file link");

        let content = fs::read_to_string(&target).expect("read through link");
        assert_eq!(content, "shared");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_live_history_file_falls_back_without_admin_symlink_privilege() {
        let root = make_temp_dir("codex-live-file-link-test");
        let source = root.join("session_index.jsonl");
        let target = root.join("instance-session_index.jsonl");
        fs::write(&source, "shared").expect("write source file");

        create_file_live_link(&source, &target).expect("create live shared history file link");

        let content = fs::read_to_string(&target).expect("read live history file");
        assert_eq!(content, "shared");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_shared_file_copies_when_link_methods_fail() {
        let root = make_temp_dir("codex-file-copy-fallback-test");
        let source = root.join("session_index.jsonl");
        let target = root.join("instance-session_index.jsonl");
        fs::write(&source, "shared").expect("write source file");

        create_shared_file_link_or_copy_with(&source, &target, |_, _| {
            Err("link denied".to_string())
        })
        .expect("copy file fallback");

        assert_eq!(
            fs::read_to_string(&target).expect("read copied file"),
            "shared"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_shared_file_preserves_existing_hard_link() {
        let root = make_temp_dir("codex-file-hard-link-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        let global_file = default_home.join("session_index.jsonl");
        let instance_file = profile_dir.join("session_index.jsonl");
        fs::create_dir_all(&default_home).expect("create default home");
        fs::create_dir_all(&profile_dir).expect("create profile dir");
        fs::write(&global_file, "shared").expect("write global file");
        fs::hard_link(&global_file, &instance_file).expect("create instance hard link");

        sync_shared_file(
            &profile_dir,
            &default_home,
            Path::new("session_index.jsonl"),
        )
        .expect("sync shared hard-linked file");

        assert!(files_are_same_entry(&instance_file, &global_file).expect("compare file entries"));
        fs::write(&global_file, "updated").expect("update global hard-linked file");
        assert_eq!(
            fs::read_to_string(&instance_file).expect("read instance hard-linked file"),
            "updated"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_directory_shared_link_copies_when_link_methods_fail() {
        let root = make_temp_dir("codex-dir-copy-fallback-test");
        let source = root.join("global-skills");
        let nested = source.join("nested");
        let target = root.join("instance-skills");
        fs::create_dir_all(&nested).expect("create nested source dir");
        fs::write(nested.join("probe.txt"), "shared").expect("write source probe");

        create_directory_shared_link_or_copy(
            &source,
            &target,
            |_, _| Err("symlink denied".to_string()),
            |_, _| Err("junction denied".to_string()),
        )
        .expect("copy directory fallback");

        let content =
            fs::read_to_string(target.join("nested").join("probe.txt")).expect("read copied file");
        assert_eq!(content, "shared");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_shared_session_directory_accepts_existing_junction() {
        let root = make_temp_dir("codex-session-junction-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        let global_sessions = default_home.join("sessions");
        let instance_sessions = profile_dir.join("sessions");
        fs::create_dir_all(&global_sessions).expect("create global sessions");
        fs::create_dir_all(&profile_dir).expect("create profile dir");
        create_directory_junction(&global_sessions, &instance_sessions)
            .expect("create instance session junction");

        sync_shared_directory_preserving_entries(
            &profile_dir,
            &default_home,
            Path::new("sessions"),
        )
        .expect("sync existing session junction");

        fs::write(global_sessions.join("global.jsonl"), "global").expect("write global session");
        assert_eq!(
            fs::read_to_string(instance_sessions.join("global.jsonl"))
                .expect("read global session through junction"),
            "global"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_shared_session_directory_preserves_instance_entries() {
        let root = make_temp_dir("codex-session-share-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        let global_sessions = default_home.join("sessions");
        let instance_sessions = profile_dir.join("sessions");
        fs::create_dir_all(&global_sessions).expect("create global sessions");
        fs::create_dir_all(&instance_sessions).expect("create instance sessions");
        fs::write(global_sessions.join("global.jsonl"), "global").expect("write global session");
        fs::write(instance_sessions.join("instance.jsonl"), "instance")
            .expect("write instance session");

        sync_shared_directory_preserving_entries(
            &profile_dir,
            &default_home,
            Path::new("sessions"),
        )
        .expect("share sessions");

        assert_eq!(
            fs::read_to_string(global_sessions.join("instance.jsonl"))
                .expect("read merged session"),
            "instance"
        );
        assert_eq!(
            fs::read_to_string(instance_sessions.join("global.jsonl"))
                .expect("read shared global session"),
            "global"
        );
        fs::write(global_sessions.join("new-global.jsonl"), "new global")
            .expect("write new global session");
        assert_eq!(
            fs::read_to_string(instance_sessions.join("new-global.jsonl"))
                .expect("read new global session through instance link"),
            "new global"
        );
        fs::write(instance_sessions.join("new-instance.jsonl"), "new instance")
            .expect("write new instance session through instance link");
        assert_eq!(
            fs::read_to_string(global_sessions.join("new-instance.jsonl"))
                .expect("read new instance session through global dir"),
            "new instance"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_instance_history_shared_links_canonical_history_and_merges_local_entries() {
        let root = make_temp_dir("codex-history-share-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        let global_sessions = default_home.join(CODEX_SHARED_SESSIONS_DIR_NAME);
        let instance_sessions = profile_dir.join(CODEX_SHARED_SESSIONS_DIR_NAME);
        fs::create_dir_all(&global_sessions).expect("create global sessions");
        fs::create_dir_all(&instance_sessions).expect("create instance sessions");
        fs::write(global_sessions.join("global.jsonl"), "global").expect("write global session");
        fs::write(instance_sessions.join("instance.jsonl"), "instance")
            .expect("write instance session");
        fs::write(
            default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            "{\"id\":\"global\",\"updated_at\":100}\n",
        )
        .expect("write global index");
        fs::write(
            profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            "{\"id\":\"instance\",\"updated_at\":200}\n",
        )
        .expect("write instance index");
        fs::write(
            default_home.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME),
            "{\"project-order\":[\"C:\\\\global\"],\"electron-saved-workspace-roots\":[\"C:\\\\global\"]}\n",
        )
        .expect("write global state");
        fs::write(
            profile_dir.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME),
            "{\"project-order\":[\"C:\\\\instance\"],\"electron-saved-workspace-roots\":[\"C:\\\\instance\"]}\n",
        )
        .expect("write instance state");
        create_test_state_db(&default_home);
        create_test_state_db(&profile_dir);
        insert_test_thread(&default_home, "global", 100, "Global");
        insert_test_thread(&profile_dir, "instance", 200, "Instance");
        fs::write(
            default_home.join(CODEX_SHARED_STATE_DB_WAL_FILE_NAME),
            "wal",
        )
        .expect("write wal");
        fs::write(
            default_home.join(CODEX_SHARED_STATE_DB_SHM_FILE_NAME),
            "shm",
        )
        .expect("write shm");

        ensure_instance_history_shared(&profile_dir, &default_home).expect("share history paths");

        assert_eq!(
            fs::read_to_string(global_sessions.join("instance.jsonl"))
                .expect("read merged instance session"),
            "instance"
        );
        assert_eq!(
            fs::read_to_string(instance_sessions.join("global.jsonl"))
                .expect("read global session through live link"),
            "global"
        );
        let merged_index =
            fs::read_to_string(default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME))
                .expect("read merged index");
        assert!(merged_index.contains("\"id\":\"global\""));
        assert!(merged_index.contains("\"id\":\"instance\""));
        let instance_index = profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME);
        assert_live_shared_file(
            &instance_index,
            &default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
        );
        let merged_state =
            fs::read_to_string(default_home.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME))
                .expect("read merged global state");
        assert!(merged_state.contains("C:\\\\global"));
        assert!(merged_state.contains("C:\\\\instance"));
        assert_eq!(test_thread_ids(&default_home), vec!["global", "instance"]);
        for name in [
            CODEX_SHARED_STATE_DB_FILE_NAME,
            CODEX_SHARED_STATE_DB_WAL_FILE_NAME,
            CODEX_SHARED_STATE_DB_SHM_FILE_NAME,
        ] {
            assert_live_shared_file(&profile_dir.join(name), &default_home.join(name));
        }

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_instance_history_shared_drops_old_materialized_chat_projections() {
        let root = make_temp_dir("codex-history-materialized-prune-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        fs::create_dir_all(&default_home).expect("create default home");
        fs::create_dir_all(&profile_dir).expect("create profile dir");
        fs::write(
            default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            "{\"id\":\"source-thread\",\"updated_at\":100}\n{\"id\":\"old-fork\",\"cockpit_shared_chat\":{\"source_session_id\":\"source-thread\"}}\n",
        )
        .expect("write global index");
        fs::write(
            profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            "{\"id\":\"new-fork\",\"cockpit_shared_chat\":{\"source_session_id\":\"source-thread\"}}\n",
        )
        .expect("write instance index");
        create_test_state_db(&default_home);
        create_test_state_db(&profile_dir);
        insert_test_thread(&default_home, "source-thread", 100, "Source");
        insert_test_thread(&default_home, "old-fork", 90, "Old fork");
        insert_test_thread(&profile_dir, "new-fork", 200, "New fork");
        let connection =
            Connection::open(profile_dir.join(CODEX_SHARED_STATE_DB_FILE_NAME)).expect("open db");
        connection
            .execute(
                "UPDATE threads SET thread_source = ?1 WHERE id = 'new-fork'",
                [CODEX_SHARED_CHAT_THREAD_SOURCE],
            )
            .expect("mark instance fork");
        drop(connection);

        ensure_instance_history_shared(&profile_dir, &default_home).expect("share history paths");

        assert_eq!(test_thread_ids(&default_home), vec!["source-thread"]);
        let merged_index =
            fs::read_to_string(default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME))
                .expect("read pruned index");
        assert!(merged_index.contains("source-thread"));
        assert!(!merged_index.contains("old-fork"));
        assert!(!merged_index.contains("new-fork"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_shared_sqlite_history_accepts_existing_dangling_sidecar_links() {
        let root = make_temp_dir("codex-history-dangling-sidecar-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        fs::create_dir_all(&default_home).expect("create default home");
        fs::create_dir_all(&profile_dir).expect("create profile dir");
        fs::write(default_home.join(CODEX_SHARED_STATE_DB_FILE_NAME), "sqlite")
            .expect("write state db placeholder");

        sync_shared_sqlite_history(&profile_dir, &default_home).expect("share sqlite history");
        assert_live_shared_file(
            &profile_dir.join(CODEX_SHARED_STATE_DB_WAL_FILE_NAME),
            &default_home.join(CODEX_SHARED_STATE_DB_WAL_FILE_NAME),
        );

        sync_shared_sqlite_history(&profile_dir, &default_home)
            .expect("share sqlite history again");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_shared_sqlite_history_creates_canonical_database_through_missing_link() {
        let root = make_temp_dir("codex-history-missing-sqlite-link-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        fs::create_dir_all(&default_home).expect("create default home");
        fs::create_dir_all(&profile_dir).expect("create profile dir");

        sync_shared_sqlite_history(&profile_dir, &default_home).expect("share sqlite history");
        let instance_db = profile_dir.join(CODEX_SHARED_STATE_DB_FILE_NAME);
        assert_live_shared_file(
            &instance_db,
            &default_home.join(CODEX_SHARED_STATE_DB_FILE_NAME),
        );

        let connection = Connection::open(&instance_db).expect("open db through symlink");
        connection
            .execute("CREATE TABLE marker (id TEXT PRIMARY KEY)", [])
            .expect("create marker table");
        connection
            .execute(
                "INSERT INTO marker (id) VALUES ('created-through-link')",
                [],
            )
            .expect("insert marker");
        drop(connection);

        let canonical_connection =
            Connection::open(default_home.join(CODEX_SHARED_STATE_DB_FILE_NAME))
                .expect("open canonical db");
        let marker = canonical_connection
            .query_row("SELECT id FROM marker", [], |row| row.get::<_, String>(0))
            .expect("read canonical marker");
        assert_eq!(marker, "created-through-link");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_instance_history_shared_creates_canonical_empty_history_files() {
        let root = make_temp_dir("codex-history-empty-canonical-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        fs::create_dir_all(&default_home).expect("create default home");
        fs::create_dir_all(&profile_dir).expect("create profile dir");

        ensure_instance_history_shared(&profile_dir, &default_home).expect("share history paths");

        assert_eq!(
            fs::read_to_string(default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME))
                .expect("read canonical index"),
            ""
        );
        assert_eq!(
            fs::read_to_string(default_home.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME))
                .expect("read canonical global state"),
            "{}\n"
        );
        assert_live_shared_file(
            &profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            &default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
        );
        assert_live_shared_file(
            &profile_dir.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME),
            &default_home.join(CODEX_SHARED_GLOBAL_STATE_FILE_NAME),
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn windows_instance_history_shared_keeps_auth_and_electron_runtime_isolated() {
        let root = make_temp_dir("codex-history-auth-isolation-test");
        let default_home = root.join("default");
        let profile_dir = root.join("instance");
        fs::create_dir_all(&default_home).expect("create default home");
        fs::create_dir_all(profile_dir.join(CODEX_ELECTRON_USER_DATA_DIR_NAME))
            .expect("create electron user data");
        fs::write(
            default_home.join("auth.json"),
            "{\"account\":\"default\"}\n",
        )
        .expect("write default auth");
        fs::write(
            profile_dir.join("auth.json"),
            "{\"account\":\"instance\"}\n",
        )
        .expect("write instance auth");
        fs::write(
            profile_dir
                .join(CODEX_ELECTRON_USER_DATA_DIR_NAME)
                .join(CODEX_ELECTRON_AUTH_MARKER_FILE_NAME),
            "{\"account_id\":\"instance\"}\n",
        )
        .expect("write electron marker");
        fs::write(
            default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            "{\"id\":\"global\"}\n",
        )
        .expect("write global index");

        ensure_instance_history_shared(&profile_dir, &default_home).expect("share history paths");

        assert_eq!(
            fs::read_to_string(profile_dir.join("auth.json")).expect("read instance auth"),
            "{\"account\":\"instance\"}\n"
        );
        assert!(profile_dir.join(CODEX_ELECTRON_USER_DATA_DIR_NAME).is_dir());
        assert!(profile_dir
            .join(CODEX_ELECTRON_USER_DATA_DIR_NAME)
            .join(CODEX_ELECTRON_AUTH_MARKER_FILE_NAME)
            .exists());
        assert_live_shared_file(
            &profile_dir.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
            &default_home.join(CODEX_SHARED_SESSION_INDEX_FILE_NAME),
        );

        let _ = fs::remove_dir_all(&root);
    }
}
