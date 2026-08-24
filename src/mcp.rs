use crate::error::{GrokSearchError, Result};
use crate::model::tool::WebSearchInput;
use crate::service::SearchService;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub async fn run_stdio(service: SearchService) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut lines = BufReader::new(stdin).lines();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                let response = error_response(Value::Null, -32700, format!("parse error: {err}"));
                stdout.write_all(response.to_string().as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                continue;
            }
        };

        if let Some(response) = handle_message(&service, request).await {
            stdout.write_all(response.to_string().as_bytes()).await?;
            stdout.write_all(b"\n").await?;
        }
    }

    Ok(())
}

/// Protocol revisions this server speaks, newest first. `initialize` echoes the
/// client's requested version when it is one of these — so existing stdio
/// clients that still request "2024-11-05" keep getting "2024-11-05" — and
/// otherwise declares [`LATEST_PROTOCOL_VERSION`].
pub(crate) const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Latest revision we support; declared when the client requests nothing or an
/// unsupported version.
pub(crate) const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";

/// Pick the protocol revision to declare in the `initialize` result: the
/// client's requested revision when supported, else our latest.
pub(crate) fn negotiate_protocol_version(requested: Option<&str>) -> &'static str {
    match requested {
        Some(req) => SUPPORTED_PROTOCOL_VERSIONS
            .iter()
            .find(|version| **version == req)
            .copied()
            .unwrap_or(LATEST_PROTOCOL_VERSION),
        None => LATEST_PROTOCOL_VERSION,
    }
}

pub(crate) async fn handle_message(service: &SearchService, request: Value) -> Option<Value> {
    request.get("id")?;
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    Some(
        handle_request(service, request)
            .await
            .unwrap_or_else(|err| {
                let code = err.code() as i64;
                error_response(id, code, err.to_string())
            }),
    )
}

async fn handle_request(service: &SearchService, request: Value) -> Result<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| GrokSearchError::InvalidParams("missing method".to_string()))?;

    match method {
        "initialize" => {
            // Negotiate: echo the client's requested revision when we speak it
            // (keeps existing stdio clients that still ask for "2024-11-05"
            // working), otherwise declare our latest supported revision.
            let requested = request
                .get("params")
                .and_then(|params| params.get("protocolVersion"))
                .and_then(Value::as_str);
            Ok(success_response(
                id,
                json!({
                    "protocolVersion": negotiate_protocol_version(requested),
                    "serverInfo": {
                        "name": "grok-search-rs",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "tools": {}
                    }
                }),
            ))
        }
        "ping" => Ok(success_response(id, json!({}))),
        "tools/list" => Ok(success_response(id, tools_list())),
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| GrokSearchError::InvalidParams("missing tool name".to_string()))?;
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let result = call_tool(service, name, args).await?;
            Ok(success_response(
                id,
                json!({
                    "content": [
                        {
                            "type": "text",
                            "text": result.to_string()
                        }
                    ],
                    "structuredContent": result
                }),
            ))
        }
        _ => Err(GrokSearchError::NotFound(format!(
            "unsupported method: {method}"
        ))),
    }
}

async fn call_tool(service: &SearchService, name: &str, args: Value) -> Result<Value> {
    match name {
        "doctor" => Ok(service.doctor().await),
        "web_search" | "web_search_standard" => {
            let mut input = parse_web_search_input(&args)?;
            // Tool identity selects the channel. `web_search` is Fast
            // (grok-chat-fast, no reasoning, no x_search). `web_search_standard`
            // is Console / operator model.
            input.fast = name == "web_search";
            let output = service.web_search(input).await?;
            Ok(serde_json::to_value(output)
                .map_err(|err| GrokSearchError::Parse(format!("serialize output: {err}")))?)
        }
        "get_sources" => {
            let session_id = args
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    GrokSearchError::InvalidParams("get_sources.session_id is required".into())
                })?;
            let offset = args
                .get("offset")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(0);
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .filter(|value| *value > 0);
            let output = service.get_sources(session_id, offset, limit).await?;
            Ok(serde_json::to_value(output)
                .map_err(|err| GrokSearchError::Parse(format!("serialize sources: {err}")))?)
        }
        "web_fetch" => {
            let url = args.get("url").and_then(Value::as_str).ok_or_else(|| {
                GrokSearchError::InvalidParams("web_fetch.url is required".into())
            })?;
            let max_chars = args
                .get("max_chars")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .filter(|value| *value > 0);
            let output = service.web_fetch(url, max_chars).await?;
            Ok(serde_json::to_value(output)
                .map_err(|err| GrokSearchError::Parse(format!("serialize fetch: {err}")))?)
        }
        "web_map" => {
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| GrokSearchError::InvalidParams("web_map.url is required".into()))?;
            let max_results = args
                .get("max_results")
                .and_then(Value::as_u64)
                .unwrap_or(10) as usize;
            let sources = service.web_map(url, max_results).await?;
            let mapped_sources: Vec<Value> = sources
                .iter()
                .map(|source| json!({ "url": &source.url, "provider": &source.provider }))
                .collect();
            Ok(
                json!({ "url": url, "sources_count": mapped_sources.len(), "sources": mapped_sources }),
            )
        }
        _ => Err(GrokSearchError::NotFound(format!("unknown tool: {name}"))),
    }
}

fn parse_web_search_input(args: &Value) -> Result<WebSearchInput> {
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .ok_or_else(|| GrokSearchError::InvalidParams("web_search.query is required".into()))?;
    Ok(WebSearchInput {
        query: query.to_string(),
        // model/platform are intentionally NOT read from tool input: the
        // calling client's LLM must not choose the Grok model or focus
        // platform (issue #15) — it hallucinates names like `grok-4` that
        // override the operator's configured model. The model is fixed by
        // config (GROK_SEARCH_MODEL) or the per-request X-Grok-Model
        // header, except `web_search` which pins grok-chat-fast via the
        // dispatcher (`input.fast`), not a free-form model name.
        platform: None,
        model: None,
        extra_sources: args
            .get("extra_sources")
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        recency_days: args
            .get("recency_days")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
            .filter(|value| *value > 0),
        include_domains: parse_string_array(args.get("include_domains")),
        exclude_domains: parse_string_array(args.get("exclude_domains")),
        include_content: args.get("include_content").and_then(Value::as_bool),
        response_format: args
            .get("response_format")
            .and_then(Value::as_str)
            .map(str::to_string),
        // Per-call reasoning intensity. Validated/normalized here so a
        // hallucinated value does not poison the upstream payload; bad
        // values fall through to the server default. Ignored by `web_search`
        // (Fast). Honored by `web_search_standard`.
        reasoning_effort: args
            .get("reasoning_effort")
            .and_then(Value::as_str)
            .and_then(crate::config::parse_reasoning_effort),
        // Dispatcher overwrites this from the tool name.
        fast: false,
    })
}

fn search_input_schema(include_reasoning_effort: bool) -> Value {
    let mut properties = json!({
        "query": { "type": "string" },
        "extra_sources": {
            "type": "integer",
            "minimum": 0,
            "description": "Optional supplemental source count, served by the configured source chain (default order: Tavily, then Exa, TinyFish, Firecrawl — first provider with results wins; GROK_SEARCH_SOURCE_PROVIDERS overrides). If omitted, GROK_SEARCH_EXTRA_SOURCES is used."
        },
        "recency_days": {
            "type": "integer",
            "minimum": 1,
            "description": "Restrict supplemental results to sources published within the last N days. Honored natively by Tavily (days+topic=news), Exa (startPublishedDate), and TinyFish (recency window); providers that cannot honor filters (Firecrawl) are skipped for filtered requests. Also hinted to Grok prompt."
        },
        "include_domains": {
            "type": "array",
            "items": { "type": "string" },
            "description": "Only return supplemental results from these domains. Tavily/Exa/TinyFish honor strictly via native domain parameters; filter-blind providers are skipped. Grok receives as soft preference."
        },
        "exclude_domains": {
            "type": "array",
            "items": { "type": "string" },
            "description": "Suppress supplemental results from these domains. Tavily/Exa/TinyFish honor strictly via native domain parameters; filter-blind providers are skipped. Grok receives as soft instruction."
        },
        "include_content": {
            "type": "boolean",
            "default": true,
            "description": "Inline source content via the resolve_content pipeline. Default true. Pass false to get summary + source-list only (legacy behavior, no content field in sources). Superseded by response_format when both are set."
        },
        "response_format": {
            "type": "string",
            "enum": ["concise", "detailed"],
            "description": "concise = synthesized answer + source metadata only (smallest payload); detailed = inline source content, subject to the response budget. Takes precedence over include_content."
        }
    });
    if include_reasoning_effort {
        properties["reasoning_effort"] = json!({
            "type": "string",
            "enum": ["low", "medium", "high", "xhigh"],
            "description": "Per-call reasoning intensity for the Grok / OpenAI-compatible upstream. This tool is a powerful multi-agent deep search; higher effort scales agent count and latency, not answer length. low — simple fact lookup and known-part / official-page checks (enough for most lookups). medium — ordinary multi-source retrieval. high — comparisons, conflicting specs, multi-step research. xhigh — maximum multi-agent scale (e.g. grok-4.20-multi-agent). When omitted, uses the server default (GROK_SEARCH_REASONING_EFFORT / X-Grok-Reasoning-Effort)."
        });
    }
    json!({
        "type": "object",
        "required": ["query"],
        "properties": properties
    })
}

fn tools_list() -> Value {
    json!({
        "tools": [
            {
                "name": "web_search",
                "description": "DEFAULT search. Fast Web channel (grok-chat-fast). Prefer this over web_search_standard for ordinary lookups, current facts, news, and discovery when you don't have a specific URL. Returns an AI-synthesised answer plus a source list. By default the first few sources carry inline content (max_inline_sources, default 5); the rest are metadata-only — drill into any of them with web_fetch(url). The whole response is capped by a character budget; when truncated=true, trimmed sources carry a note telling you how to recover the full text via web_fetch or get_sources. Pass response_format=\"concise\" for answer + source metadata only. If you already know the exact page URL, use web_fetch instead. Does not take reasoning_effort (Web fast ignores it) and does not send x_search (Web already searches natively).",
                "inputSchema": search_input_schema(false)
            },
            {
                "name": "web_search_standard",
                "description": "Standard/deep Console multi-agent search. Prefer web_search for ordinary lookups. Use this only when fast is not enough: comparisons, contradictions, multi-step research, or when you need X/Twitter search. Use for discovery when you don't have a specific URL. Returns an AI-synthesised answer plus a source list. By default the first few sources carry inline content (max_inline_sources, default 5); the rest are metadata-only — drill into any of them with web_fetch(url). The whole response is capped by a character budget; when truncated=true, trimmed sources carry a note telling you how to recover the full text via web_fetch or get_sources. Pass response_format=\"concise\" for answer + source metadata only. If you already know the exact page URL, use web_fetch instead. Set reasoning_effort: simple fact lookup → low (enough); comparisons / contradictions / multi-source research → high or xhigh. Omit to use the server default.",
                "inputSchema": search_input_schema(true)
            },
            {
                "name": "get_sources",
                "description": "Return cached sources from a previous web_search call by session_id. Use to re-examine sources already retrieved without issuing a new search — it reuses the prior session and runs no new search or fetch. Paginate with offset/limit: the response reports total_sources and, when more pages remain, next_offset to pass as the next offset.",
                "inputSchema": {
                    "type": "object",
                    "required": ["session_id"],
                    "properties": {
                        "session_id": { "type": "string" },
                        "offset": {
                            "type": "integer",
                            "minimum": 0,
                            "default": 0,
                            "description": "Index of the first source to return. Use next_offset from the previous page to continue."
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Max sources in this page. Omit to return all remaining sources (still subject to the response budget)."
                        }
                    }
                }
            },
            {
                "name": "web_fetch",
                "description": "Use when you already have a specific URL and want to read a single page in depth. GitHub issue/PR, StackOverflow (StackExchange), arXiv, and Wikipedia URLs are automatically parsed into structured, de-noised Markdown ready to feed an LLM; all other pages fall back to generic extraction. Hard 60s timeout for the whole call (independent of GROK_SEARCH_TIMEOUT_SECONDS). Returns {url, content, original_length, truncated, source_type, fallback_reason?}. If you don't have a URL yet and need to discover sources, use web_search instead.",
                "inputSchema": {
                    "type": "object",
                    "required": ["url"],
                    "properties": {
                        "url": { "type": "string" },
                        "max_chars": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Optional character cap on returned content. Falls back to GROK_SEARCH_FETCH_MAX_CHARS, otherwise unlimited."
                        }
                    }
                }
            },
            {
                "name": "web_map",
                "description": "Map/discover URLs through Tavily Map.",
                "inputSchema": {
                    "type": "object",
                    "required": ["url"],
                    "properties": {
                        "url": { "type": "string" },
                        "max_results": { "type": "integer", "minimum": 1 }
                    }
                }
            },
            {
                "name": "doctor",
                "description": "Diagnostic probe: live connectivity check for the Grok backend and every configured source provider (Tavily / Exa / TinyFish / Firecrawl), plus the effective source chain and masked configuration. Use to verify the server is wired up and reachable.",
                "inputSchema": { "type": "object", "properties": {} }
            }
        ]
    })
}

fn parse_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn success_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

pub(crate) fn error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initialized_notification_does_not_emit_response() {
        let service = SearchService::fake_with_sources();
        let response = handle_message(
            &service,
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }),
        )
        .await;

        assert_eq!(response, None);
    }

    #[test]
    fn negotiate_protocol_version_prefers_supported_client_version() {
        // Backward compatibility: an old client asking for 2024-11-05 is echoed.
        assert_eq!(negotiate_protocol_version(Some("2024-11-05")), "2024-11-05");
        assert_eq!(negotiate_protocol_version(Some("2025-06-18")), "2025-06-18");
        assert_eq!(negotiate_protocol_version(Some("2025-11-25")), "2025-11-25");
        // Unknown / absent -> our latest.
        assert_eq!(
            negotiate_protocol_version(Some("1999-01-01")),
            LATEST_PROTOCOL_VERSION
        );
        assert_eq!(negotiate_protocol_version(None), LATEST_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn initialize_echoes_legacy_client_version() {
        let service = SearchService::fake_with_sources();
        let response = handle_request(
            &service,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2024-11-05", "capabilities": {} }
            }),
        )
        .await
        .expect("initialize response");
        // An existing stdio client must still see its own requested revision.
        assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(response["result"]["serverInfo"]["name"], "grok-search-rs");
    }

    #[tokio::test]
    async fn initialize_declares_latest_for_unknown_version() {
        let service = SearchService::fake_with_sources();
        let response = handle_request(
            &service,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "initialize",
                "params": { "protocolVersion": "3000-01-01", "capabilities": {} }
            }),
        )
        .await
        .expect("initialize response");
        assert_eq!(
            response["result"]["protocolVersion"],
            LATEST_PROTOCOL_VERSION
        );
    }

    #[tokio::test]
    async fn ping_request_gets_empty_success_response() {
        let service = SearchService::fake_with_sources();
        let response = handle_request(
            &service,
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "ping"
            }),
        )
        .await
        .expect("ping response");

        assert_eq!(response["id"], 7);
        assert_eq!(response["result"], json!({}));
    }

    #[tokio::test]
    async fn web_map_returns_url_sources_without_search_metadata() {
        let service = SearchService::fake_with_sources();
        let response = handle_request(
            &service,
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "web_map",
                    "arguments": {
                        "url": "https://example.com",
                        "max_results": 2
                    }
                }
            }),
        )
        .await
        .expect("web_map response");

        let output = &response["result"]["structuredContent"];
        let sources = output["sources"].as_array().expect("sources");
        assert_eq!(output["sources_count"], 2);
        assert_eq!(
            sources[0],
            json!({
                "url": "https://example.com/page-0",
                "provider": "tavily"
            })
        );
        assert!(sources[0].get("title").is_none());
        assert!(sources[0].get("description").is_none());
        assert!(sources[0].get("published_date").is_none());
    }

    #[test]
    fn tools_list_descriptions_guide_routing() {
        let listed = tools_list();
        let tools = listed["tools"].as_array().expect("tools array");

        let desc = |name: &str| -> String {
            tools
                .iter()
                .find(|t| t["name"] == name)
                .unwrap_or_else(|| panic!("tool {name} missing"))["description"]
                .as_str()
                .unwrap_or_else(|| panic!("tool {name} description not a string"))
                .to_string()
        };

        // web_search: DEFAULT fast channel. Discovery cue, no single-page-read role.
        let web_search = desc("web_search");
        assert!(web_search.contains("DEFAULT"), "web_search: {web_search}");
        assert!(
            web_search.contains("grok-chat-fast"),
            "web_search: {web_search}"
        );
        assert!(web_search.contains("discovery"), "web_search: {web_search}");
        assert!(
            web_search.contains("don't have a specific URL"),
            "web_search: {web_search}"
        );
        assert!(
            !web_search.contains("read a single page"),
            "web_search must not claim the single-page-read role: {web_search}"
        );
        assert!(
            web_search.contains("web_search_standard"),
            "web_search must point at the standard tool: {web_search}"
        );

        // web_search_standard: multi-agent, effort steering, prefer fast first.
        let standard = desc("web_search_standard");
        assert!(
            standard.contains("multi-agent"),
            "web_search_standard: {standard}"
        );
        assert!(
            standard.contains("simple fact lookup"),
            "web_search_standard must steer simple lookups to low: {standard}"
        );
        assert!(
            standard.contains("Prefer web_search"),
            "web_search_standard: {standard}"
        );

        let effort = {
            let tools = listed["tools"].as_array().expect("tools array");
            tools
                .iter()
                .find(|t| t["name"] == "web_search_standard")
                .expect("web_search_standard")["inputSchema"]["properties"]["reasoning_effort"]
                ["description"]
                .as_str()
                .expect("reasoning_effort description")
                .to_string()
        };
        assert!(
            effort.contains("simple fact lookup"),
            "reasoning_effort must say low is enough for fact lookup: {effort}"
        );
        assert!(
            effort.contains("server default"),
            "reasoning_effort must point omit at the server default: {effort}"
        );
        assert!(
            !effort.contains("this host"),
            "schema must not bake in this-host defaults: {effort}"
        );

        // web_fetch: targeted single-page read, names all four special
        // sources, cross-references web_search.
        let web_fetch = desc("web_fetch");
        assert!(web_fetch.contains("specific URL"), "web_fetch: {web_fetch}");
        assert!(
            web_fetch.contains("read a single page"),
            "web_fetch: {web_fetch}"
        );
        assert!(web_fetch.contains("GitHub issue"), "web_fetch: {web_fetch}");
        assert!(
            web_fetch.contains("StackOverflow") || web_fetch.contains("StackExchange"),
            "web_fetch: {web_fetch}"
        );
        assert!(web_fetch.contains("arXiv"), "web_fetch: {web_fetch}");
        assert!(web_fetch.contains("Wikipedia"), "web_fetch: {web_fetch}");
        assert!(web_fetch.contains("web_search"), "web_fetch: {web_fetch}");
        assert!(
            web_fetch.contains("60s"),
            "web_fetch must name the 60s cap: {web_fetch}"
        );

        // get_sources: reuses a prior web_search session, runs no new search.
        let get_sources = desc("get_sources");
        assert!(
            get_sources.contains("session_id"),
            "get_sources: {get_sources}"
        );
        assert!(
            get_sources.contains("new search"),
            "get_sources: {get_sources}"
        );
    }

    #[test]
    fn web_search_schema_hides_model_and_platform() {
        // issue #15: the calling client's LLM must not be offered `model` or
        // `platform` — it fills them with hallucinated values (e.g. `grok-4`)
        // that override the operator's configured model. The schema must not
        // advertise them, so the client never learns they exist.
        let listed = tools_list();
        let tools = listed["tools"].as_array().expect("tools array");
        assert_eq!(
            tools[0]["name"], "web_search",
            "DEFAULT search tool must be listed first"
        );
        for name in ["web_search", "web_search_standard"] {
            let tool = tools
                .iter()
                .find(|t| t["name"] == name)
                .unwrap_or_else(|| panic!("{name} tool present"));
            let props = tool["inputSchema"]["properties"]
                .as_object()
                .unwrap_or_else(|| panic!("{name} inputSchema.properties object"));

            assert!(
                !props.contains_key("model"),
                "{name} must not expose `model`: {props:?}"
            );
            assert!(
                !props.contains_key("platform"),
                "{name} must not expose `platform`: {props:?}"
            );
            assert!(
                !props.contains_key("fast"),
                "{name} must not expose `fast`: {props:?}"
            );
            assert!(
                props.contains_key("query"),
                "{name} query must remain: {props:?}"
            );
            assert!(
                props.contains_key("response_format"),
                "{name} response_format must remain: {props:?}"
            );
        }

        let web_search = tools
            .iter()
            .find(|t| t["name"] == "web_search")
            .expect("web_search");
        let fast_props = web_search["inputSchema"]["properties"]
            .as_object()
            .expect("web_search properties");
        assert!(
            !fast_props.contains_key("reasoning_effort"),
            "web_search must not expose reasoning_effort: {fast_props:?}"
        );

        let standard = tools
            .iter()
            .find(|t| t["name"] == "web_search_standard")
            .expect("web_search_standard");
        let std_props = standard["inputSchema"]["properties"]
            .as_object()
            .expect("web_search_standard properties");
        assert!(
            std_props.contains_key("reasoning_effort"),
            "web_search_standard must expose reasoning_effort: {std_props:?}"
        );
        let effort_enum = std_props["reasoning_effort"]["enum"]
            .as_array()
            .expect("reasoning_effort enum");
        for level in ["low", "medium", "high", "xhigh"] {
            assert!(
                effort_enum.iter().any(|v| v == level),
                "missing {level} in {effort_enum:?}"
            );
        }
    }

    #[test]
    fn parse_web_search_input_ignores_fast_arg() {
        let input = parse_web_search_input(&json!({
            "query": "capital of France",
            "fast": true,
            "reasoning_effort": "high"
        }))
        .expect("parse");
        assert_eq!(input.query, "capital of France");
        assert!(!input.fast, "fast is dispatcher-owned, not a tool arg");
        assert_eq!(input.reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn parse_web_search_input_defaults_fast_false() {
        let input = parse_web_search_input(&json!({ "query": "q" })).expect("parse");
        assert!(!input.fast);
    }

    #[test]
    fn tools_list_splits_fast_and_standard() {
        let listed = tools_list();
        let tools = listed["tools"].as_array().expect("tools array");
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(
            names.contains(&"web_search") && names.contains(&"web_search_standard"),
            "expected both search tools, got {names:?}"
        );
        let web_search = tools
            .iter()
            .find(|t| t["name"] == "web_search")
            .expect("web_search");
        let desc = web_search["description"].as_str().expect("desc");
        assert!(desc.contains("DEFAULT"), "web_search: {desc}");
        assert!(desc.contains("grok-chat-fast"), "web_search: {desc}");
        assert!(desc.contains("x_search"), "web_search: {desc}");
        let standard = tools
            .iter()
            .find(|t| t["name"] == "web_search_standard")
            .expect("web_search_standard");
        let std_desc = standard["description"].as_str().expect("desc");
        assert!(std_desc.contains("multi-agent"), "{std_desc}");
        assert!(std_desc.contains("Prefer web_search"), "{std_desc}");
    }
}
