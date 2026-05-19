use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QoderLocalAccessRoutingStrategy {
    Auto,
    QuotaHighFirst,
    QuotaLowFirst,
}

impl Default for QoderLocalAccessRoutingStrategy {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QoderLocalAccessScope {
    Localhost,
    Lan,
}

fn default_access_scope() -> QoderLocalAccessScope {
    QoderLocalAccessScope::Localhost
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QoderLocalAccessCollection {
    pub enabled: bool,
    pub port: u16,
    pub api_key: String,
    #[serde(default = "default_access_scope")]
    pub access_scope: QoderLocalAccessScope,
    #[serde(default)]
    pub routing_strategy: QoderLocalAccessRoutingStrategy,
    pub account_ids: Vec<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QoderLocalAccessUsageStats {
    #[serde(default)]
    pub request_count: u64,
    #[serde(default)]
    pub success_count: u64,
    #[serde(default)]
    pub failure_count: u64,
    #[serde(default)]
    pub total_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QoderLocalAccessStats {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub totals: QoderLocalAccessUsageStats,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QoderLocalAccessState {
    pub collection: Option<QoderLocalAccessCollection>,
    pub running: bool,
    pub base_url: Option<String>,
    pub model_ids: Vec<String>,
    pub last_error: Option<String>,
    pub member_count: usize,
    pub stats: QoderLocalAccessStats,
}
