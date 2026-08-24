use crate::error::{GrokSearchError, Result};
use crate::model::search::{ContentBlock, SearchRequest, FAST_MODEL};
use serde_json::{json, Value};

pub fn to_grok_responses_payload(
    req: &SearchRequest,
    require_web_search: bool,
    include_x_search: bool,
) -> Result<Value> {
    if require_web_search && !req.tools.iter().any(|tool| tool.name == "web_search") {
        return Err(GrokSearchError::Parse(
            "web_search is enabled but request does not include web_search tool intent".to_string(),
        ));
    }

    let mut input = Vec::new();
    if let Some(system) = req
        .system
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        input.push(json!({ "role": "system", "content": system }));
    }

    for message in &req.messages {
        let content = message
            .content
            .iter()
            .map(|block| match block {
                ContentBlock::Text { text } => text.as_str(),
            })
            .collect::<Vec<_>>()
            .join("\n");
        input.push(json!({ "role": message.role, "content": content }));
    }

    // Web Fast (`grok-chat-fast`) already runs hosted web/X search; sending
    // `x_search` is a 400 (`invalid_tools`) on grok2api Web.
    let include_x_search = include_x_search && !req.fast;

    let mut tools = Vec::new();
    if require_web_search {
        tools.push(json!({ "type": "web_search" }));
    }
    if include_x_search {
        tools.push(json!({ "type": "x_search" }));
    }

    let model = if req.fast {
        FAST_MODEL
    } else {
        req.model.as_str()
    };

    let mut payload = json!({
        "model": model,
        "input": input,
        "tools": tools,
        "stream": true
    });
    if !req.fast {
        if let Some(effort) = req
            .reasoning_effort
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            // xAI Responses API: nested `reasoning.effort` (chat uses top-level
            // `reasoning_effort` — see chat_completions_request).
            payload["reasoning"] = json!({ "effort": effort });
        }
    }
    Ok(payload)
}
