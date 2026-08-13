//! Compatibility shim for fork-local Kiro machine identity handling.
//!
//! Upstream removed the legacy device-fingerprint module, but Kiro's OAuth and
//! local gateway still use the shared serviceMachineId fallback. Keep only
//! that narrow contract here instead of restoring the unrelated fingerprint
//! commands and models.

use rusqlite::Connection;

/// Read the shared Antigravity serviceMachineId, or create and persist one.
pub fn get_service_machine_id() -> String {
    if let Ok(path) = crate::modules::db::get_db_path() {
        if let Ok(connection) = Connection::open(path) {
            let stored: Result<String, _> = connection.query_row(
                "SELECT value FROM ItemTable WHERE key = 'storage.serviceMachineId'",
                [],
                |row| row.get(0),
            );
            if let Ok(value) = stored {
                let value = value.trim();
                if !value.is_empty() {
                    return value.to_string();
                }
            }
        }
    }

    let generated = uuid::Uuid::new_v4().to_string();
    let _ = crate::modules::db::write_service_machine_id(&generated);
    generated
}
