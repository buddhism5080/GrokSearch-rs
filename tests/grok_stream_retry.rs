// Grok Responses: request SSE, discard incomplete streams, retry retryable
// failures only while elapsed < 120s from the first attempt.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use grok_search_rs::error::GrokSearchError;
use grok_search_rs::model::search::{ContentBlock, SearchMessage, SearchRequest, SearchTool};
use grok_search_rs::providers::grok::{
    should_retry_grok, GrokResponsesProvider, GROK_RETRY_WINDOW,
};
use grok_search_rs::providers::http::build_client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok after retry\"}\n\n\
event: response.completed\n\
data: {\"type\":\"response.completed\"}\n\n"
}

fn sse_incomplete() -> &'static [u8] {
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"half\"}\n\n"
}

fn http_status(status: u16, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} ERR\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

async fn spawn_scripted_server(script: Vec<Vec<u8>>) -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits_clone = hits.clone();
    tokio::spawn(async move {
        let mut i = 0;
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            let n = hits_clone.fetch_add(1, Ordering::SeqCst);
            let body = script.get(n).cloned().or_else(|| script.last().cloned());
            let Some(body) = body else {
                return;
            };
            i += 1;
            let _ = i;
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (format!("http://{addr}/v1"), hits)
}

#[test]
fn retries_transport_and_incomplete_inside_window() {
    let timeout = GrokSearchError::Timeout("Grok Responses request timed out".into());
    let incomplete = GrokSearchError::Provider(
        "Grok Responses SSE stream ended incompletely (no response.completed)".into(),
    );
    assert!(should_retry_grok(&timeout, None, Duration::from_secs(0)));
    assert!(should_retry_grok(
        &incomplete,
        None,
        Duration::from_millis(119_999)
    ));
    assert!(should_retry_grok(
        &timeout,
        Some(503),
        Duration::from_secs(1)
    ));
    assert!(should_retry_grok(
        &timeout,
        Some(429),
        Duration::from_secs(10)
    ));
}

#[test]
fn does_not_retry_after_window_or_on_client_errors() {
    let timeout = GrokSearchError::Timeout("timed out".into());
    let failed = GrokSearchError::Provider(
        "Grok Responses stream ended with response.failed: upstream failed".into(),
    );
    let parse = GrokSearchError::Parse("bad json".into());
    assert!(!should_retry_grok(&timeout, None, GROK_RETRY_WINDOW));
    assert!(!should_retry_grok(
        &timeout,
        None,
        GROK_RETRY_WINDOW + Duration::from_millis(1)
    ));
    assert!(!should_retry_grok(
        &timeout,
        Some(401),
        Duration::from_secs(1)
    ));
    assert!(!should_retry_grok(
        &timeout,
        Some(400),
        Duration::from_secs(1)
    ));
    assert!(!should_retry_grok(
        &timeout,
        Some(403),
        Duration::from_secs(1)
    ));
    assert!(!should_retry_grok(&failed, None, Duration::from_secs(1)));
    assert!(!should_retry_grok(&parse, None, Duration::from_secs(1)));
}

#[tokio::test]
async fn retries_incomplete_stream_then_succeeds() {
    let (base, hits) =
        spawn_scripted_server(vec![sse_incomplete().to_vec(), sse_completed().to_vec()]).await;
    let provider = GrokResponsesProvider::with_client(
        build_client(Duration::from_secs(5)),
        base,
        "dummy-key",
        true,
        false,
    );
    let got = provider
        .search(&sample_request())
        .await
        .expect("incomplete first attempt should retry");
    assert_eq!(got.content, "ok after retry");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retries_until_success_inside_window() {
    let (base, hits) = spawn_scripted_server(vec![
        sse_incomplete().to_vec(),
        sse_incomplete().to_vec(),
        sse_incomplete().to_vec(),
        sse_completed().to_vec(),
    ])
    .await;
    let provider = GrokResponsesProvider::with_client(
        build_client(Duration::from_secs(5)),
        base,
        "dummy-key",
        true,
        false,
    );
    let got = provider
        .search(&sample_request())
        .await
        .expect("three incomplete attempts inside 120s must still retry");
    assert_eq!(got.content, "ok after retry");
    assert_eq!(hits.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn does_not_retry_unauthorized() {
    let (base, hits) =
        spawn_scripted_server(vec![http_status(401, r#"{"error":{"message":"bad key"}}"#)]).await;
    let provider = GrokResponsesProvider::with_client(
        build_client(Duration::from_secs(5)),
        base,
        "dummy-key",
        true,
        false,
    );
    let err = provider
        .search(&sample_request())
        .await
        .expect_err("401 must not retry");
    assert!(err.to_string().contains("401"), "{err}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn does_not_retry_response_failed() {
    let failed = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
event: response.failed\n\
data: {\"type\":\"response.failed\",\"error\":{\"message\":\"upstream failed\"}}\n\n";
    let (base, hits) = spawn_scripted_server(vec![failed.to_vec(), sse_completed().to_vec()]).await;
    let provider = GrokResponsesProvider::with_client(
        build_client(Duration::from_secs(5)),
        base,
        "dummy-key",
        true,
        false,
    );
    let err = provider
        .search(&sample_request())
        .await
        .expect_err("response.failed is a terminal business error");
    assert!(err.to_string().contains("response.failed"), "{err}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_payload_asks_for_stream() {
    let hits = Arc::new(AtomicUsize::new(0));
    let saw_stream = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits_c = hits.clone();
    let saw_c = saw_stream.clone();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        hits_c.fetch_add(1, Ordering::SeqCst);
        let mut buf = [0u8; 8192];
        let n = sock.read(&mut buf).await.unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        if req.contains(r#""stream":true"#) {
            saw_c.fetch_add(1, Ordering::SeqCst);
        }
        let _ = sock.write_all(sse_completed()).await;
        let _ = sock.shutdown().await;
    });
    let provider = GrokResponsesProvider::with_client(
        build_client(Duration::from_secs(5)),
        format!("http://{addr}/v1"),
        "dummy-key",
        true,
        false,
    );
    let got = provider.search(&sample_request()).await.expect("ok");
    assert_eq!(got.content, "ok after retry");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        saw_stream.load(Ordering::SeqCst),
        1,
        "Grok request body must set stream:true"
    );
}
