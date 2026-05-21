use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroLocalAccessCollection {
    pub enabled: bool,
    pub port: u16,
    pub api_key: String,
    pub account_ids: Vec<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KiroLocalAccessUsageStats {
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
pub struct KiroLocalAccessStats {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub totals: KiroLocalAccessUsageStats,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroLocalAccessState {
    pub collection: Option<KiroLocalAccessCollection>,
    pub running: bool,
    pub base_url: Option<String>,
    pub model_ids: Vec<String>,
    pub last_error: Option<String>,
    pub member_count: usize,
    pub stats: KiroLocalAccessStats,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroLocalAccessTestFailure {
    pub title: String,
    pub stage: String,
    pub cause: String,
    pub suggestion: String,
    pub status: Option<u16>,
    pub model_id: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroLocalAccessTestResult {
    pub model_id: Option<String>,
    pub latency_ms: Option<u64>,
    pub output: Option<String>,
    pub failure: Option<KiroLocalAccessTestFailure>,
}
