use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use notifier_runtime::{
    PluginMetadata, RoutePluginInput, SourceContext, SourcePlugin, ValidatedSource,
};
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tracing::{debug, warn};

use crate::input::{BroadcasterValidator, configured_broadcasters};

/// Error returned by [`WebhookProvider::parse`]. The dispatcher maps
/// [`WebhookError::Unauthorized`] to HTTP 401 and [`WebhookError::BadRequest`]
/// to HTTP 400; other failures from the provider flow through the dispatcher's
/// generic error handler as 400 responses.
#[derive(Debug)]
pub enum WebhookError {
    /// The webhook signature was missing or did not verify.
    Unauthorized,
    /// The request could not be decoded into a processable webhook event.
    BadRequest(String),
}

impl WebhookError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("unauthorized webhook"),
            Self::BadRequest(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for WebhookError {}

impl From<anyhow::Error> for WebhookError {
    fn from(error: anyhow::Error) -> Self {
        Self::BadRequest(error.to_string())
    }
}

/// The result of [`WebhookProvider::dispatch`]. The dispatcher translates each
/// variant to the corresponding HTTP status. Providers are responsible for
/// performing ingestion (via `context.sink`) and emitting any acceptance log
/// lines before returning `WebhookOutcome::Accepted`.
///
/// | Variant       | HTTP status |
/// |---------------|--------------|
/// | Challenge(s)  | 200 OK with `s` as the body |
/// | Revoked       | 200 OK (empty body) |
/// | Accepted      | 204 NO_CONTENT |
/// | Ignored       | 204 NO_CONTENT |
pub enum WebhookOutcome {
    /// Echo the supplied challenge string with 200 OK (Twitch EventSub
    /// callback verification flow).
    Challenge(String),
    /// Acknowledge a subscription revocation with 200 OK.
    Revoked,
    /// An event was accepted and persisted; respond with 204 NO_CONTENT.
    Accepted,
    /// An event was received but intentionally not processed; respond with
    /// 204 NO_CONTENT.
    Ignored,
}

/// Trait implemented by a webhook source provider. Combined with
/// [`WebhookSource`] this yields a full [`notifier_runtime::SourcePlugin`].
///
/// `parse` runs first on each inbound HTTP request and is expected to perform
/// signature/shape verification. `dispatch` then carries out provider-specific
/// matching, enrichment, ingestion, and logging. Splitting these phases lets
/// the unified dispatcher normalize the 401/400/204/200 status responses
/// across providers.
#[async_trait]
pub trait WebhookProvider: Send + Sync + 'static {
    type Spec: DeserializeOwned + Serialize + JsonSchema + Clone + Send + Sync + 'static;
    type Event: Send + Sync + 'static;

    fn metadata(&self) -> PluginMetadata;
    fn template_context_schema(&self) -> Value;
    fn template_variables(&self) -> Vec<String>;

    /// Per-plugin broadcaster validation hook. Defaults to no per-character
    /// restriction; Twitch overrides this with
    /// [`BroadcasterValidator::ascii_alphanumeric_or_underscore`].
    fn broadcaster_validator(&self) -> BroadcasterValidator {
        BroadcasterValidator::none()
    }

    /// The HTTP path this provider serves for the supplied spec. Used to
    /// build the [`ValidatedSource::http_paths`] list and the Axum router.
    fn webhook_path(&self, spec: &Self::Spec) -> String;

    /// Parse and validate the provider spec from raw JSON. Called during
    /// `validate_spec` and `reconcile`; the dispatcher also parses the spec
    /// once per source when building the router.
    fn parse_spec(&self, value: &Value) -> Result<Self::Spec>;

    /// Verify and decode the inbound webhook request into a typed event.
    /// Return [`WebhookError::Unauthorized`] when the request signature does
    /// not verify and [`WebhookError::BadRequest`] when the request cannot be
    /// decoded.
    async fn parse(
        &self,
        spec: &Self::Spec,
        headers: &HeaderMap,
        body: &Bytes,
    ) -> Result<Self::Event, WebhookError>;

    /// Resolve the decoded event into an outcome. Providers are expected to
    /// perform broadcaster matching, enrichment, ingestion, and any
    /// provider-specific logging here. Errors bubble up to the dispatcher as
    /// HTTP 400 responses.
    async fn dispatch(
        &self,
        spec: &Self::Spec,
        event: Self::Event,
        context: &SourceContext,
    ) -> Result<WebhookOutcome>;

    /// Reconcile external provider subscriptions / webhooks at startup.
    async fn reconcile(&self, spec: &Self::Spec, context: &SourceContext) -> Result<()>;
}

/// State shared by the unified Axum webhook handler. Holds the parsed provider
/// spec so each request can access it without reparsing the raw JSON.
pub struct WebhookState<P: WebhookProvider> {
    pub provider: Arc<P>,
    pub spec: P::Spec,
    pub context: SourceContext,
}

impl<P: WebhookProvider> Clone for WebhookState<P> {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            spec: self.spec.clone(),
            context: self.context.clone(),
        }
    }
}

/// Construct the standard single-route Axum router for a webhook source.
pub fn single_webhook_router<P: WebhookProvider>(
    webhook_path: &str,
    provider: Arc<P>,
    spec: P::Spec,
    context: SourceContext,
) -> Router {
    Router::new()
        .route(webhook_path, post(webhook::<P>))
        .with_state(WebhookState {
            provider,
            spec,
            context,
        })
}

/// Generic wrapper that exposes any [`WebhookProvider`] as a
/// [`notifier_runtime::SourcePlugin`].
pub struct WebhookSource<P: WebhookProvider> {
    provider: Arc<P>,
}

impl<P: WebhookProvider> WebhookSource<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }
}

#[async_trait]
impl<P: WebhookProvider> SourcePlugin for WebhookSource<P> {
    fn metadata(&self) -> PluginMetadata {
        self.provider.metadata()
    }

    fn template_context_schema(&self) -> Value {
        self.provider.template_context_schema()
    }

    fn template_variables(&self) -> Vec<String> {
        self.provider.template_variables()
    }

    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<ValidatedSource> {
        let parsed = self.provider.parse_spec(spec)?;
        configured_broadcasters(inputs, &self.provider.broadcaster_validator())?;
        Ok(ValidatedSource {
            allowed_template_variables: self.provider.template_variables(),
            http_paths: vec![self.provider.webhook_path(&parsed)],
        })
    }

    fn router(&self, context: SourceContext) -> Router {
        let spec = self
            .provider
            .parse_spec(&context.spec)
            .expect("validated provider spec");
        let path = self.provider.webhook_path(&spec);
        single_webhook_router(&path, self.provider.clone(), spec, context)
    }

    async fn reconcile(&self, context: &SourceContext) -> Result<()> {
        let spec = self.provider.parse_spec(&context.spec)?;
        self.provider.reconcile(&spec, context).await
    }
}

async fn webhook<P: WebhookProvider>(
    State(state): State<WebhookState<P>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let source_id = state.context.source_id.clone();
    debug!(
        source_id = %source_id,
        body_bytes = body.len(),
        "received webhook"
    );
    let event = match state.provider.parse(&state.spec, &headers, &body).await {
        Ok(event) => event,
        Err(WebhookError::Unauthorized) => {
            warn!(
                source_id = %source_id,
                body_bytes = body.len(),
                "rejected webhook with invalid signature"
            );
            return StatusCode::UNAUTHORIZED.into_response();
        }
        Err(WebhookError::BadRequest(message)) => {
            warn!(
                source_id = %source_id,
                body_bytes = body.len(),
                error = %message,
                "rejected webhook"
            );
            return (StatusCode::BAD_REQUEST, message).into_response();
        }
    };
    match state
        .provider
        .dispatch(&state.spec, event, &state.context)
        .await
    {
        Ok(WebhookOutcome::Challenge(challenge)) => (StatusCode::OK, challenge).into_response(),
        Ok(WebhookOutcome::Revoked) => StatusCode::OK.into_response(),
        Ok(WebhookOutcome::Accepted) => StatusCode::NO_CONTENT.into_response(),
        Ok(WebhookOutcome::Ignored) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            warn!(
                source_id = %source_id,
                body_bytes = body.len(),
                error = %error,
                "rejected webhook"
            );
            (StatusCode::BAD_REQUEST, error.to_string()).into_response()
        }
    }
}
