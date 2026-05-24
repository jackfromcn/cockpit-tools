use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct CompletionRequest {
    pub model: String,
    pub stream: bool,
    pub prompt: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GatewayProxyRequest {
    pub model: String,
    pub stream: bool,
    pub messages: Vec<GatewayMessage>,
    pub tools: Vec<GatewayTool>,
    pub tool_choice: Option<Value>,
    pub previous_response_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct GatewayMessage {
    pub role: String,
    pub content: Value,
    pub tool_calls: Vec<GatewayToolCall>,
    pub tool_call_id: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct GatewayTool {
    pub tool_type: String,
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub web_search_max_uses: Option<i32>,
    pub allowed_domains: Option<Vec<String>>,
    pub blocked_domains: Option<Vec<String>>,
    pub user_location: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GatewayToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ServerToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
    pub result_content: Value,
    pub tool_result_text: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AggregatedKiroResponse {
    pub text: String,
    pub thinking: String,
    pub tool_calls: Vec<GatewayToolCall>,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub cache_read_input_tokens: Option<i32>,
    pub cache_creation_input_tokens: Option<i32>,
    pub context_usage_percentage: Option<f32>,
    pub metering_usage: Option<f64>,
    pub citations: Vec<AggregatedCitation>,
}

#[derive(Debug, Clone)]
pub(crate) struct AggregatedCitation {
    pub text: Option<String>,
    pub link: String,
    pub target: Value,
}

#[derive(Debug)]
pub(crate) enum KiroEvent {
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
    ContextUsage {
        percentage: f32,
    },
    Metering {
        unit: String,
        unit_plural: String,
        usage: f64,
    },
    Citation {
        text: Option<String>,
        link: String,
        target: Value,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct KiroUpstreamCredentials {
    pub access_token: String,
    pub profile_arn: Option<String>,
    pub region: String,
    pub user_agent: String,
    pub provider: Option<String>,
    pub account_email: String,
}

#[derive(Debug)]
pub(crate) struct DirectProxyError {
    pub status: u16,
    pub message: String,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum ApiProtocol {
    OpenAi,
    Anthropic,
    Responses,
}

#[derive(Debug)]
pub(crate) struct ProxyResult {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub is_stream: bool,
}

#[derive(Debug, Default)]
pub(crate) struct KiroCliDbSnapshot {
    pub auth_values: HashMap<String, Option<String>>,
    pub profile_value: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResponsesSessionEntry {
    pub previous_response_id: Option<String>,
    pub request_messages: Vec<GatewayMessage>,
    pub assistant_message: GatewayMessage,
    pub updated_at: Instant,
}

pub(crate) enum KiroCliAuthMode {
    ReuseCurrent,
    Injected(KiroCliDbSnapshot),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KiroPayload {
    pub conversation_state: ConversationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConversationState {
    pub chat_trigger_type: String,
    pub conversation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_continuation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_task_type: Option<String>,
    pub current_message: CurrentMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<HistoryItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customization_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CurrentMessage {
    pub user_input_message: UserInputMessage,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserInputMessage {
    pub content: String,
    pub model_id: String,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_point: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_cache_config: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<ImageBlock>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_input_message_context: Option<UserInputMessageContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_intent: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ImageBlock {
    pub format: String,
    pub source: ImageSource,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ImageSource {
    Bytes {
        bytes: String,
    },
    Other {
        #[serde(flatten)]
        data: Value,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserInputMessageContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_studio_context: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub console_state: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub editor_state: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_state: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_state: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shell_state: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_results: Option<Vec<KiroToolResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<KiroTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_settings: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum KiroTool {
    CachePoint {
        #[serde(rename = "cachePoint")]
        cache_point: Value,
    },
    ToolSpecification {
        #[serde(rename = "toolSpecification")]
        tool_specification: KiroToolSpec,
    },
    Other {
        #[serde(flatten)]
        data: Value,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KiroToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: KiroInputSchema,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct KiroInputSchema {
    pub json: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KiroToolResult {
    pub content: Vec<KiroToolResultContent>,
    pub status: String,
    pub tool_use_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum KiroToolResultContent {
    Text {
        text: String,
    },
    Json {
        json: Value,
    },
    Other {
        #[serde(flatten)]
        data: Value,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum HistoryItem {
    User {
        #[serde(rename = "userInputMessage")]
        user_input_message: HistoryUserMessage,
    },
    Assistant {
        #[serde(rename = "assistantResponseMessage")]
        assistant_response_message: HistoryAssistantMessage,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoryUserMessage {
    pub content: String,
    pub model_id: String,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<ImageBlock>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_input_message_context: Option<UserInputMessageContext>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoryAssistantMessage {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_uses: Option<Vec<KiroToolUse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplementary_web_links: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub followup_prompt: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_point: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KiroToolUse {
    pub name: String,
    pub input: Value,
    pub tool_use_id: String,
}
