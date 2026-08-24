use std::time::Duration;

use grok_search_rs::logging::{
    capture_logs, capture_logs_async, format_request_log, RequestTrace, REQUEST_LOG_QUERY_MAX,
};
use grok_search_rs::model::search::{ContentBlock, SearchMessage, SearchRequest, SearchTool};
use grok_search_rs::providers::grok::GrokResponsesProvider;
use grok_search_rs::providers::http::{build_client, post_json};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn request_log_line_has_sid_phase_and_fields() {
    let line = format_request_log(
        "abc123def456",
        "start",
        &[
            ("query", "what is grok"),
            ("model", "grok-4.20-multi-agent-0309"),
            ("effort", "medium"),
        ],
    );
    assert!(
        line.starts_with("grok-search-rs: sid=abc123def456 phase=start "),
        "got {line}"
    );
    assert!(line.contains("query=what is grok"));
    assert!(line.contains("model=grok-4.20-multi-agent-0309"));
    assert!(line.contains("effort=medium"));
    assert!(!line.contains('\n'), "one line only: {line}");
}

#[test]
fn request_log_truncates_long_query_and_strips_newlines() {
    let long = format!("line1\nline2 {}", "x".repeat(REQUEST_LOG_QUERY_MAX + 20));
    let line = format_request_log("sid", "start", &[("query", &long)]);
    assert!(
        !line.contains('\n'),
        "newlines must not split the log line: {line}"
    );
    assert!(
        line.contains("query=line1 line2"),
        "newlines become spaces: {line}"
    );
    assert!(
        line.contains("…") || line.contains("..."),
        "long query must be truncated: {line}"
    );
}

#[test]
fn request_log_does_not_echo_secrets() {
    let line = format_request_log(
        "sid",
        "start",
        &[
            ("query", "Authorization: Bearer g2a_secret_value"),
            ("key", "g2a_c8ea35a62b74_should_not_appear"),
        ],
    );
    assert!(
        !line.contains("g2a_secret_value") && !line.contains("g2a_c8ea35a62b74"),
        "secret leaked: {line}"
    );
}

#[test]
fn capture_logs_records_trace_emit() {
    let ((), lines) = capture_logs(|| {
        let trace = RequestTrace::new("sess01");
        trace.emit("start", &[("query", "hello")]);
        trace.emit(
            "return",
            &[("provider", "grok_responses"), ("sources", "3")],
        );
    });
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[0].contains("phase=start") && lines[0].contains("sid=sess01"));
    assert!(lines[1].contains("phase=return") && lines[1].contains("provider=grok_responses"));
}

fn sample_request() -> SearchRequest {
    SearchRequest {
        model: "grok-4.20-multi-agent-0309".to_string(),
        system: None,
        messages: vec![SearchMessage {
            role: "user".to_string(),
            content: vec![ContentBlock::text("latest rust release")],
        }],
        tools: vec![SearchTool::web_search()],
        reasoning_effort: Some("medium".into()),
        fast: false,
    }
}

fn sse_completed() -> &'static [u8] {
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
event: response.created\n\
data: {\"type\":\"response.created\"}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n\
event: response.completed\n\
data: {\"type\":\"response.completed\"}\n\n"
}

fn sse_incomplete() -> &'static [u8] {
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"half\"}\n\n"
}

async fn spawn_one_shot(body: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8192];
        let _ = sock.read(&mut buf).await;
        let _ = sock.write_all(body).await;
        let _ = sock.shutdown().await;
    });
    format!("http://{addr}/v1")
}

#[tokio::test]
async fn sse_logs_first_event_and_completed() {
    let base = spawn_one_shot(sse_completed()).await;
    let client = build_client(Duration::from_secs(5));
    let trace = RequestTrace::new("sse01");
    let (raw, lines) = capture_logs_async(trace.scope(async {
        post_json(
            &client,
            &format!("{base}/responses"),
            "dummy-key",
            &json!({"model": "grok-4-fast", "input": "test", "stream": true}),
            "Grok Responses",
        )
        .await
        .expect("completed stream")
    }))
    .await;
    assert_eq!(raw["output_text"], "ok");
    let joined = lines.join("\n");
    assert!(
        lines.iter().any(|l| l.contains("phase=grok_first")
            && l.contains("event=response.created")
            && l.contains("sid=sse01")),
        "missing grok_first: {joined}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("phase=grok_completed") && l.contains("sid=sse01")),
        "missing grok_completed: {joined}"
    );
}

#[tokio::test]
async fn grok_retry_logs_fail_then_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // first: incomplete
        {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let _ = sock.read(&mut buf).await;
            let _ = sock.write_all(sse_incomplete()).await;
            let _ = sock.shutdown().await;
        }
        // second: completed
        {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let _ = sock.read(&mut buf).await;
            let _ = sock.write_all(sse_completed()).await;
            let _ = sock.shutdown().await;
        }
    });
    let provider = GrokResponsesProvider::with_client(
        build_client(Duration::from_secs(5)),
        format!("http://{addr}/v1"),
        "dummy-key",
        true,
        false,
    );
    let trace = RequestTrace::new("retry01");
    let (got, lines) = capture_logs_async(
        trace.scope(async { provider.search(&sample_request()).await.expect("retried") }),
    )
    .await;
    assert_eq!(got.content, "ok");
    let joined = lines.join("\n");
    assert!(
        lines
            .iter()
            .any(|l| l.contains("phase=grok_retry") && l.contains("sid=retry01")),
        "missing grok_retry: {joined}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("phase=grok_completed") && l.contains("sid=retry01")),
        "missing grok_completed after retry: {joined}"
    );
}
