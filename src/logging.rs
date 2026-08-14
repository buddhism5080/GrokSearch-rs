use std::cell::RefCell;
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::task::futures::TaskLocalFuture;

use crate::error::{GrokSearchError, Result};

pub const REQUEST_LOG_QUERY_MAX: usize = 120;

thread_local! {
    static CAPTURE: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
}

tokio::task_local! {
    static REQUEST_TRACE: RequestTrace;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugEvent {
    pub event: String,
    pub payload: Value,
}

impl DebugEvent {
    pub fn new(event: impl Into<String>, payload: Value) -> Self {
        Self {
            event: event.into(),
            payload: redact_json_value(payload),
        }
    }
}

pub fn write_jsonl_event(path: impl AsRef<Path>, event: &DebugEvent) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| GrokSearchError::Provider(format!("create log dir failed: {err}")))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| GrokSearchError::Provider(format!("open log failed: {err}")))?;
    let line = serde_json::to_string(event)
        .map_err(|err| GrokSearchError::Parse(format!("serialize log failed: {err}")))?;
    writeln!(file, "{line}")
        .map_err(|err| GrokSearchError::Provider(format!("write log failed: {err}")))?;
    Ok(())
}

pub fn redact_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(redact_object(map)),
        Value::Array(items) => Value::Array(items.into_iter().map(redact_json_value).collect()),
        other => other,
    }
}

fn redact_object(map: Map<String, Value>) -> Map<String, Value> {
    map.into_iter()
        .map(|(key, value)| {
            if is_secret_key(&key) {
                (key, json!("***"))
            } else {
                (key, redact_json_value(value))
            }
        })
        .collect()
}

fn is_secret_key(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    lowered.contains("authorization")
        || lowered.contains("api_key")
        || lowered.contains("apikey")
        || lowered.contains("token")
        || lowered.contains("secret")
}

/// One-line operator log: `grok-search-rs: sid=… phase=… k=v …`
pub fn format_request_log(sid: &str, phase: &str, fields: &[(&str, &str)]) -> String {
    let mut out = format!("grok-search-rs: sid={sid} phase={phase}");
    for (key, value) in fields {
        out.push(' ');
        out.push_str(key);
        out.push('=');
        out.push_str(&sanitize_field(key, value));
    }
    out
}

fn sanitize_field(key: &str, value: &str) -> String {
    let collapsed: String = value
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    let collapsed = collapsed.trim();
    let truncated = if key == "query" && collapsed.chars().count() > REQUEST_LOG_QUERY_MAX {
        let cut: String = collapsed.chars().take(REQUEST_LOG_QUERY_MAX).collect();
        format!("{cut}…")
    } else {
        collapsed.to_string()
    };
    redact_secret_text(&truncated)
}

fn redact_secret_text(value: &str) -> String {
    let mut out = value.to_string();
    // Bearer tokens and g2a_ client keys must never hit the operator log.
    if let Some(idx) = out.find("Bearer ") {
        let rest = &out[idx + "Bearer ".len()..];
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        out.replace_range(idx + "Bearer ".len()..idx + "Bearer ".len() + end, "***");
    }
    if let Some(idx) = out.find("g2a_") {
        let rest = &out[idx..];
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        out.replace_range(idx..idx + end, "g2a_***");
    }
    out
}

pub fn emit_request_log(sid: &str, phase: &str, fields: &[(&str, &str)]) {
    let line = format_request_log(sid, phase, fields);
    let captured = CAPTURE.with(|slot| {
        if let Some(buf) = slot.borrow_mut().as_mut() {
            buf.push(line.clone());
            true
        } else {
            false
        }
    });
    if !captured {
        eprintln!("{line}");
    }
}

#[derive(Clone)]
pub struct RequestTrace {
    sid: Arc<String>,
}

impl RequestTrace {
    pub fn new(sid: impl Into<String>) -> Self {
        Self {
            sid: Arc::new(sid.into()),
        }
    }

    pub fn sid(&self) -> &str {
        self.sid.as_str()
    }

    pub fn emit(&self, phase: &str, fields: &[(&str, &str)]) {
        emit_request_log(self.sid(), phase, fields);
    }

    pub fn scope<F>(&self, fut: F) -> TaskLocalFuture<RequestTrace, F>
    where
        F: Future,
    {
        REQUEST_TRACE.scope(self.clone(), fut)
    }
}

/// Emit against the current task-local trace, if any.
pub fn emit_current(phase: &str, fields: &[(&str, &str)]) {
    let _ = REQUEST_TRACE.try_with(|trace| trace.emit(phase, fields));
}

/// Capture log lines written on this thread (sync tests).
pub fn capture_logs<R>(f: impl FnOnce() -> R) -> (R, Vec<String>) {
    CAPTURE.with(|slot| {
        *slot.borrow_mut() = Some(Vec::new());
    });
    let result = f();
    let lines = CAPTURE.with(|slot| slot.borrow_mut().take().unwrap_or_default());
    (result, lines)
}

/// Capture log lines written while `fut` runs (same thread / worker).
pub async fn capture_logs_async<T>(fut: impl Future<Output = T>) -> (T, Vec<String>) {
    CAPTURE.with(|slot| {
        *slot.borrow_mut() = Some(Vec::new());
    });
    let result = fut.await;
    let lines = CAPTURE.with(|slot| slot.borrow_mut().take().unwrap_or_default());
    (result, lines)
}
