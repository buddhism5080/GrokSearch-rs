use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Web Fast channel model. Grok2API Web (`grok-chat-fast`) already runs hosted
/// web/X search natively; Fast mode must not send `x_search` or `reasoning.effort`.
pub const FAST_MODEL: &str = "grok-chat-fast";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchRequest {
    pub model: String,
    pub system: Option<String>,
    pub messages: Vec<SearchMessage>,
    pub tools: Vec<SearchTool>,
    /// Optional reasoning effort for Grok / OpenAI-compatible gateways.
    /// Responses: `reasoning.effort`. Chat Completions: top-level `reasoning_effort`.
    /// Values: `low` | `medium` | `high` | `xhigh`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Web Fast mode. When true the Responses payload pins [`FAST_MODEL`],
    /// omits `reasoning`, and omits `x_search` (Web already searches).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fast: bool,
}

impl SearchRequest {
    /// Pin this request to Web Fast mode. Callers that only have a bool should
    /// use this rather than mutating the three fields independently.
    pub fn apply_fast_mode(&mut self) {
        self.model = FAST_MODEL.to_string();
        self.reasoning_effort = None;
        self.fast = true;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchMessage {
    pub role: String,
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
}

impl ContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn as_text(&self) -> &str {
        match self {
            Self::Text { text } => text,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchTool {
    pub name: String,
    pub input_schema: Value,
}

impl SearchTool {
    pub fn web_search() -> Self {
        Self {
            name: "web_search".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchResponse {
    pub content: String,
    pub sources: Vec<crate::model::source::Source>,
}

/// Structured retrieval filters shared by source providers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchFilters {
    pub recency_days: Option<u32>,
    pub include_domains: Vec<String>,
    pub exclude_domains: Vec<String>,
}

impl SearchFilters {
    pub fn is_empty(&self) -> bool {
        self.recency_days.is_none()
            && self.include_domains.is_empty()
            && self.exclude_domains.is_empty()
    }
}
