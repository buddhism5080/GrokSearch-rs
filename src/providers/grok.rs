use crate::adapters::grok_responses_request::to_grok_responses_payload;
use crate::adapters::grok_responses_response::parse_grok_responses;
use crate::credentials::{CredentialProvider, StaticApiKeyCredential};
use crate::error::{GrokSearchError, Result};
use crate::logging;
use crate::model::search::{SearchRequest, SearchResponse};
use crate::providers::http::{build_client, post_json_with_status};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

/// From the first Grok attempt, retryable failures may be retried until this
/// window elapses. After it, the last error is returned as-is.
pub const GROK_RETRY_WINDOW: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct GrokResponsesProvider {
    client: Client,
    api_url: String,
    credential: Arc<dyn CredentialProvider>,
    require_web_search: bool,
    include_x_search: bool,
}

/// Retry transport / incomplete-stream / 429 / 5xx while still inside
/// [`GROK_RETRY_WINDOW`]. Client errors and terminal `response.failed` do not retry.
pub fn should_retry_grok(err: &GrokSearchError, status: Option<u16>, elapsed: Duration) -> bool {
    if elapsed >= GROK_RETRY_WINDOW {
        return false;
    }
    if matches!(status, Some(400 | 401 | 403 | 404 | 422)) {
        return false;
    }
    match err {
        GrokSearchError::Timeout(_) => true,
        GrokSearchError::Provider(msg) => {
            !msg.contains("response.failed")
                && !msg.contains("stream ended with response.incomplete")
        }
        _ => false,
    }
}

impl GrokResponsesProvider {
    pub fn new(
        api_url: impl Into<String>,
        api_key: impl Into<String>,
        require_web_search: bool,
        include_x_search: bool,
        timeout: Duration,
    ) -> Self {
        Self::with_client(
            build_client(timeout),
            api_url,
            api_key,
            require_web_search,
            include_x_search,
        )
    }

    /// Construct with an externally provided `reqwest::Client`. Used by
    /// `SearchService::new` to share one tuned client across providers; the
    /// `new(.., timeout)` form remains for callers that prefer per-provider
    /// timeouts (tests, integration users).
    pub fn with_client(
        client: Client,
        api_url: impl Into<String>,
        api_key: impl Into<String>,
        require_web_search: bool,
        include_x_search: bool,
    ) -> Self {
        Self::with_credential_client(
            client,
            api_url,
            Arc::new(StaticApiKeyCredential::new(api_key.into())),
            require_web_search,
            include_x_search,
        )
    }

    pub fn with_credential_client(
        client: Client,
        api_url: impl Into<String>,
        credential: Arc<dyn CredentialProvider>,
        require_web_search: bool,
        include_x_search: bool,
    ) -> Self {
        Self {
            client,
            api_url: api_url.into().trim_end_matches('/').to_string(),
            credential,
            require_web_search,
            include_x_search,
        }
    }

    pub fn endpoint(&self) -> String {
        format!("{}/responses", self.api_url)
    }

    pub async fn search(&self, request: &SearchRequest) -> Result<SearchResponse> {
        let payload =
            to_grok_responses_payload(request, self.require_web_search, self.include_x_search)?;
        let token = self.credential.bearer_token().await?;
        let started = tokio::time::Instant::now();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match post_json_with_status(
                &self.client,
                &self.endpoint(),
                &token,
                &payload,
                "Grok Responses",
            )
            .await
            {
                Ok(raw) => return parse_grok_responses(&raw),
                Err(failure) => {
                    if should_retry_grok(&failure.error, failure.status, started.elapsed()) {
                        let status = failure
                            .status
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "-".into());
                        let err = failure.error.to_string();
                        let n = attempt.to_string();
                        logging::emit_current(
                            "grok_retry",
                            &[("attempt", &n), ("status", &status), ("err", &err)],
                        );
                        // Tiny pause so a fast-failing upstream cannot busy-loop
                        // for the whole 120s window.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    return Err(failure.error);
                }
            }
        }
    }
}
