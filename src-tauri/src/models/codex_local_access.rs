use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodexLocalAccessRoutingStrategy {
    Auto,
    QuotaHighFirst,
    QuotaLowFirst,
    PlanHighFirst,
    PlanLowFirst,
    ExpirySoonFirst,
}

impl Default for CodexLocalAccessRoutingStrategy {
    fn default() -> Self {
        Self::Auto
    }
}

fn default_restrict_free_accounts() -> bool {
    true
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessApiKey {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub key: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub monthly_token_limit: Option<u64>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessCollection {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKey>,
    #[serde(default, rename = "apiKey", skip_serializing)]
    pub legacy_api_key: Option<String>,
    #[serde(default)]
    pub routing_strategy: CodexLocalAccessRoutingStrategy,
    #[serde(default = "default_restrict_free_accounts")]
    pub restrict_free_accounts: bool,
    #[serde(default)]
    pub account_ids: Vec<String>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessUsageStats {
    #[serde(default)]
    pub request_count: u64,
    #[serde(default)]
    pub success_count: u64,
    #[serde(default)]
    pub failure_count: u64,
    #[serde(default)]
    pub total_latency_ms: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessAccountStats {
    pub account_id: String,
    pub email: String,
    #[serde(default)]
    pub usage: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessApiKeyStats {
    pub api_key_id: String,
    pub api_key_name: String,
    #[serde(default)]
    pub usage: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessStatsWindow {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub totals: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub accounts: Vec<CodexLocalAccessAccountStats>,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKeyStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessUsageEvent {
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub api_key_id: String,
    #[serde(default)]
    pub api_key_name: String,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub latency_ms: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessStats {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub totals: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub accounts: Vec<CodexLocalAccessAccountStats>,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKeyStats>,
    #[serde(default)]
    pub daily: CodexLocalAccessStatsWindow,
    #[serde(default)]
    pub weekly: CodexLocalAccessStatsWindow,
    #[serde(default)]
    pub monthly: CodexLocalAccessStatsWindow,
    #[serde(default)]
    pub events: Vec<CodexLocalAccessUsageEvent>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessState {
    pub collection: Option<CodexLocalAccessCollection>,
    pub running: bool,
    pub api_port_url: Option<String>,
    pub base_url: Option<String>,
    pub model_ids: Vec<String>,
    pub last_error: Option<String>,
    pub member_count: usize,
    pub stats: CodexLocalAccessStats,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessPortCleanupResult {
    pub killed_count: u32,
    pub state: CodexLocalAccessState,
}
