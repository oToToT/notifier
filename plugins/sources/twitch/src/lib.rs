use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{Router, body::Bytes, http::HeaderMap};
use futures::TryStreamExt;
use notifier_runtime::{
    PluginMetadata, RoutePluginInput, SourceContext, SourcePlugin, ValidatedSource, schema_value,
};
use notifier_webhook::{
    BroadcasterValidator, CommonSpec, WebhookError, WebhookOutcome, WebhookProvider, WebhookSource,
    configured_broadcasters, expected_hmac_sha256_parts, matching_route_ids, validate_common_spec,
    verify_hmac_sha256_parts,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, info, warn};
use twitch_api::{
    TwitchClient,
    eventsub::{EventType, Transport, stream::StreamOnlineV1},
    helix::streams::GetStreamsRequest,
    twitch_oauth2::{AppAccessToken, ClientId, ClientSecret},
};

pub struct TwitchSource(pub WebhookSource<TwitchProvider>);

impl TwitchSource {
    pub fn new() -> Self {
        Self(WebhookSource::new(TwitchProvider::default()))
    }
}

impl Default for TwitchSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourcePlugin for TwitchSource {
    fn metadata(&self) -> PluginMetadata {
        self.0.metadata()
    }
    fn template_context_schema(&self) -> Value {
        self.0.template_context_schema()
    }
    fn template_variables(&self) -> Vec<String> {
        self.0.template_variables()
    }
    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<ValidatedSource> {
        self.0.validate_spec(spec, inputs)
    }
    fn router(&self, context: SourceContext) -> Router {
        self.0.router(context)
    }
    async fn reconcile(&self, context: &SourceContext) -> Result<()> {
        self.0.reconcile(context).await
    }
    async fn run(&self, context: SourceContext) -> Result<()> {
        self.0.run(context).await
    }
}

#[derive(Clone, Default)]
pub struct TwitchProvider {
    client: TwitchClient<'static, reqwest13::Client>,
}

impl TwitchProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TwitchSpec {
    #[serde(flatten)]
    pub common: CommonSpec,
    pub webhook_secret: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct Input {
    broadcasters: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct TwitchTemplateContext {
    event: EventContext,
    broadcaster: BroadcasterContext,
    stream: StreamContext,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct EventContext {
    id: String,
    kind: String,
    occurred_at: String,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct BroadcasterContext {
    id: String,
    login: String,
    name: String,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct StreamContext {
    title: String,
    url: String,
}

#[derive(Deserialize)]
struct WebhookBody {
    challenge: Option<String>,
    subscription: Subscription,
    event: Option<TwitchEvent>,
}

#[derive(Deserialize)]
struct Subscription {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Deserialize)]
pub struct TwitchEvent {
    pub broadcaster_user_id: String,
    pub broadcaster_user_login: String,
    pub broadcaster_user_name: String,
    pub started_at: Option<String>,
}

#[derive(Debug)]
pub enum ParsedMessage {
    Challenge {
        challenge: Option<String>,
    },
    Revocation {
        kind: String,
    },
    Notification {
        kind: String,
        event: Option<TwitchEvent>,
        message_id: String,
        timestamp: String,
    },
    Unsupported {
        message_type: String,
        message_id: String,
    },
}

#[async_trait]
impl WebhookProvider for TwitchProvider {
    type Spec = TwitchSpec;
    type Event = ParsedMessage;

    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "twitch",
            description: "Receives Twitch EventSub stream.online webhooks.",
            spec_schema: schema_value::<TwitchSpec>(),
            input_schema: schema_value::<Input>(),
        }
    }

    fn template_context_schema(&self) -> Value {
        schema_value::<TwitchTemplateContext>()
    }

    fn template_variables(&self) -> Vec<String> {
        vec!["event".into(), "broadcaster".into(), "stream".into()]
    }

    fn broadcaster_validator(&self) -> BroadcasterValidator {
        BroadcasterValidator::ascii_alphanumeric_or_underscore()
    }

    fn webhook_path(&self, spec: &Self::Spec) -> String {
        spec.common.webhook_path.clone()
    }

    fn parse_spec(&self, value: &Value) -> Result<Self::Spec> {
        let spec: TwitchSpec =
            serde_json::from_value(value.clone()).context("invalid Twitch spec")?;
        validate_common_spec(&spec.common).context("invalid Twitch spec")?;
        if spec.webhook_secret.trim().is_empty() {
            bail!("webhook_secret cannot be empty");
        }
        if spec.webhook_secret.len() < 10 || spec.webhook_secret.len() > 100 {
            bail!("webhook_secret must contain 10 to 100 characters");
        }
        Ok(spec)
    }

    async fn parse(
        &self,
        spec: &Self::Spec,
        headers: &HeaderMap,
        body: &Bytes,
    ) -> Result<Self::Event, WebhookError> {
        let message_id = header_str(headers, "twitch-eventsub-message-id")?;
        let timestamp = header_str(headers, "twitch-eventsub-message-timestamp")?;
        let signature = header_str(headers, "twitch-eventsub-message-signature")?;
        let message_type = header_str(headers, "twitch-eventsub-message-type")?;

        let parsed_body: WebhookBody = serde_json::from_slice(body)
            .context("invalid webhook JSON")
            .map_err(|error| {
                debug!(
                    message_id,
                    message_type,
                    body_bytes = body.len(),
                    body = %String::from_utf8_lossy(body),
                    error = %error,
                    "Twitch webhook JSON decode failed"
                );
                WebhookError::bad_request(error.to_string())
            })?;

        let verified = verify_hmac_sha256_parts(
            &spec.webhook_secret,
            &[message_id.as_bytes(), timestamp.as_bytes(), body],
            signature,
        );
        if let Err(error) = verified {
            let expected = expected_hmac_sha256_parts(
                &spec.webhook_secret,
                &[message_id.as_bytes(), timestamp.as_bytes(), body],
            );
            debug!(
                message_id,
                message_type,
                timestamp,
                received_signature = signature,
                expected_signature = %expected,
                body_bytes = body.len(),
                error = %error,
                "Twitch webhook signature mismatch details"
            );
            warn!(
                message_id,
                message_type, "rejected Twitch webhook with invalid signature"
            );
            return Err(WebhookError::Unauthorized);
        }

        let kind = parsed_body.subscription.kind;
        let message = match message_type {
            "webhook_callback_verification" => ParsedMessage::Challenge {
                challenge: parsed_body.challenge,
            },
            "revocation" => ParsedMessage::Revocation { kind },
            "notification" => ParsedMessage::Notification {
                kind,
                event: parsed_body.event,
                message_id: message_id.into(),
                timestamp: timestamp.into(),
            },
            other => ParsedMessage::Unsupported {
                message_type: other.to_string(),
                message_id: message_id.into(),
            },
        };
        Ok(message)
    }

    async fn dispatch(
        &self,
        spec: &Self::Spec,
        event: Self::Event,
        context: &SourceContext,
    ) -> Result<WebhookOutcome> {
        match event {
            ParsedMessage::Challenge { challenge } => {
                let challenge = challenge
                    .context("challenge is missing")
                    .inspect_err(|error| {
                        debug!(
                            source_id = %context.source_id,
                            error = %error,
                            "rejected Twitch webhook verification because challenge is missing"
                        );
                    })?;
                info!(
                    source_id = %context.source_id,
                    "accepted Twitch webhook verification challenge"
                );
                Ok(WebhookOutcome::Challenge(challenge))
            }
            ParsedMessage::Revocation { kind } => {
                debug!(
                    source_id = %context.source_id,
                    subscription_type = %kind,
                    "received Twitch subscription revocation details"
                );
                warn!(
                    source_id = %context.source_id,
                    subscription_type = %kind,
                    "received Twitch subscription revocation"
                );
                Ok(WebhookOutcome::Revoked)
            }
            ParsedMessage::Notification {
                kind,
                event,
                message_id,
                timestamp,
            } => {
                if kind != "stream.online" {
                    debug!(
                        source_id = %context.source_id,
                        message_id,
                        subscription_type = %kind,
                        "ignored unsupported Twitch notification"
                    );
                    return Ok(WebhookOutcome::Ignored);
                }
                let event = event.context("event is missing").inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        message_id,
                        subscription_type = %kind,
                        error = %error,
                        "rejected Twitch notification because event is missing"
                    );
                })?;
                let route_ids = matching_route_ids(
                    &context.route_inputs,
                    &event.broadcaster_user_login,
                    &self.broadcaster_validator(),
                )
                .inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        message_id,
                        broadcaster_login = %event.broadcaster_user_login,
                        broadcaster_id = %event.broadcaster_user_id,
                        configured_routes = context.route_inputs.len(),
                        error = %error,
                        "Twitch webhook rejected route input configuration"
                    );
                })?;
                if route_ids.is_empty() {
                    debug!(
                        source_id = %context.source_id,
                        message_id,
                        broadcaster_login = %event.broadcaster_user_login,
                        broadcaster_id = %event.broadcaster_user_id,
                        configured_routes = context.route_inputs.len(),
                        "Twitch notification broadcaster did not match any configured route"
                    );
                    warn!(
                        source_id = %context.source_id,
                        message_id,
                        broadcaster_login = %event.broadcaster_user_login,
                        broadcaster_id = %event.broadcaster_user_id,
                        configured_routes = context.route_inputs.len(),
                        "ignored Twitch notification for unconfigured broadcaster"
                    );
                    return Ok(WebhookOutcome::Ignored);
                }
                let access_token = token(&self.client, spec).await.inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        message_id,
                        broadcaster_login = %event.broadcaster_user_login,
                        broadcaster_id = %event.broadcaster_user_id,
                        error = %error,
                        "failed to request Twitch token while handling webhook"
                    );
                })?;
                let stream = get_stream(&self.client, &access_token, &event.broadcaster_user_id)
                    .await
                    .inspect_err(|error| {
                        debug!(
                            source_id = %context.source_id,
                            message_id,
                            broadcaster_login = %event.broadcaster_user_login,
                            broadcaster_id = %event.broadcaster_user_id,
                            error = %error,
                            "failed to fetch Twitch stream while handling webhook"
                        );
                    })?;
                let context_json = json!({
                    "event": {
                        "id": message_id,
                        "kind": "stream.online",
                        "occurred_at": event.started_at.clone().unwrap_or_else(|| timestamp.clone()),
                    },
                    "broadcaster": {
                        "id": event.broadcaster_user_id,
                        "login": event.broadcaster_user_login,
                        "name": event.broadcaster_user_name,
                    },
                    "stream": {
                        "title": stream.title,
                        "url": format!("https://www.twitch.tv/{}", stream.user_login),
                    }
                });
                let queued = context
                    .sink
                    .ingest(&context.source_id, &route_ids, &message_id, &context_json)
                    .inspect_err(|error| {
                        debug!(
                            source_id = %context.source_id,
                            message_id,
                            broadcaster_login = %event.broadcaster_user_login,
                            broadcaster_id = %event.broadcaster_user_id,
                            route_count = route_ids.len(),
                            error = %error,
                            "failed to ingest Twitch webhook delivery"
                        );
                    })?;
                info!(
                    source_id = %context.source_id,
                    message_id,
                    broadcaster_login = %event.broadcaster_user_login,
                    broadcaster_id = %event.broadcaster_user_id,
                    route_count = route_ids.len(),
                    queued,
                    "accepted Twitch stream.online webhook"
                );
                Ok(WebhookOutcome::Accepted)
            }
            ParsedMessage::Unsupported {
                message_type,
                message_id,
            } => {
                debug!(
                    source_id = %context.source_id,
                    message_id,
                    message_type,
                    "rejected Twitch webhook with unsupported message type"
                );
                bail!("unsupported Twitch message type {message_type:?}");
            }
        }
    }

    async fn reconcile(&self, spec: &Self::Spec, context: &SourceContext) -> Result<()> {
        let callback = context
            .public_base_url
            .join(&spec.common.webhook_path)?
            .to_string();
        info!(
            source_id = %context.source_id,
            callback,
            "reconciling Twitch EventSub subscriptions"
        );
        let token = token(&self.client, spec).await.inspect_err(|error| {
            debug!(
                source_id = %context.source_id,
                error = %error,
                "failed to request Twitch token during reconciliation"
            );
        })?;
        let broadcasters =
            configured_broadcasters(&context.route_inputs, &self.broadcaster_validator())
                .inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        route_count = context.route_inputs.len(),
                        error = %error,
                        "Twitch reconciliation rejected route input configuration"
                    );
                })?;
        let pages = self
            .client
            .helix
            .get_eventsub_subscriptions(None, Some(EventType::StreamOnline), None, &token)
            .try_collect::<Vec<_>>()
            .await
            .inspect_err(|error| {
                debug!(
                    source_id = %context.source_id,
                    callback,
                    error = %error,
                    "failed to list Twitch EventSub subscriptions during reconciliation"
                );
            })?;
        for broadcaster_login in broadcasters {
            let broadcaster = resolve_user(&self.client, &broadcaster_login, &token)
                .await
                .inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        broadcaster_login,
                        error = %error,
                        "failed to resolve Twitch broadcaster during reconciliation"
                    );
                })?;
            let exists = pages
                .iter()
                .flat_map(|page| &page.subscriptions)
                .any(|subscription| {
                    subscription.condition["broadcaster_user_id"] == broadcaster.id.as_str()
                        && subscription
                            .transport
                            .as_webhook()
                            .is_some_and(|transport| transport.callback == callback)
                });
            if !exists {
                let broadcaster_id = broadcaster.id.clone();
                info!(
                    source_id = %context.source_id,
                    broadcaster_login,
                    broadcaster_id = %broadcaster_id,
                    "creating Twitch EventSub subscription"
                );
                self.client
                    .helix
                    .create_eventsub_subscription(
                        StreamOnlineV1::broadcaster_user_id(broadcaster_id.clone()),
                        Transport::webhook(&callback, spec.webhook_secret.clone()),
                        &token,
                    )
                    .await
                    .inspect_err(|error| {
                        debug!(
                            source_id = %context.source_id,
                            broadcaster_login,
                            broadcaster_id = %broadcaster_id,
                            callback,
                            error = %error,
                            "failed to create Twitch EventSub subscription during reconciliation"
                        );
                    })?;
            } else {
                debug!(
                    source_id = %context.source_id,
                    broadcaster_login,
                    broadcaster_id = %broadcaster.id,
                    "Twitch EventSub subscription already exists"
                );
            }
        }
        info!(source_id = %context.source_id, "Twitch reconciliation completed");
        Ok(())
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, WebhookError> {
    match headers.get(name) {
        Some(value) => match value.to_str() {
            Ok(value) => Ok(value),
            Err(_) => {
                debug!(header = name, "Twitch header is not valid UTF-8");
                Err(WebhookError::bad_request(format!(
                    "header {name:?} is not valid UTF-8"
                )))
            }
        },
        None => {
            debug!(header = name, "required Twitch header is missing");
            Err(WebhookError::bad_request(format!(
                "required Twitch header {name:?} is missing"
            )))
        }
    }
}

async fn token(
    client: &TwitchClient<'static, reqwest13::Client>,
    spec: &TwitchSpec,
) -> Result<AppAccessToken> {
    AppAccessToken::get_app_access_token(
        client,
        ClientId::new(spec.common.client_id.clone()),
        ClientSecret::new(spec.common.client_secret.clone()),
        vec![],
    )
    .await
    .context("failed to request Twitch app access token")
}

async fn resolve_user(
    client: &TwitchClient<'static, reqwest13::Client>,
    broadcaster: &str,
    token: &AppAccessToken,
) -> Result<twitch_api::helix::users::User> {
    client
        .helix
        .get_user_from_login(broadcaster, token)
        .await?
        .with_context(|| format!("Twitch broadcaster {broadcaster:?} was not found"))
}

async fn get_stream(
    client: &TwitchClient<'static, reqwest13::Client>,
    token: &AppAccessToken,
    user_id: &str,
) -> Result<twitch_api::helix::streams::Stream> {
    client
        .helix
        .req_get(GetStreamsRequest::user_ids(&[user_id]), token)
        .await?
        .data
        .into_iter()
        .next()
        .context("Twitch stream data was unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use notifier_webhook::expected_hmac_sha256_parts;

    fn spec_value() -> Value {
        serde_json::json!({
            "webhook_path": "/hooks/twitch",
            "client_id": "id",
            "client_secret": "secret",
            "webhook_secret": "0123456789"
        })
    }

    #[test]
    fn validates_configured_webhook_path() {
        let validated = TwitchSource::new()
            .validate_spec(
                &spec_value(),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: serde_json::json!({"broadcasters": ["Example", "Another"]}),
                }],
            )
            .unwrap();
        assert_eq!(validated.http_paths, ["/hooks/twitch"]);
    }

    #[test]
    fn rejects_webhook_secret_outside_length_range() {
        for length in [0, 9, 101] {
            let secret = "x".repeat(length);
            let mut spec = spec_value();
            spec["webhook_secret"] = serde_json::Value::String(secret);
            assert!(TwitchSource::new().validate_spec(&spec, &[]).is_err());
        }
        let mut spec = spec_value();
        spec["webhook_secret"] = serde_json::Value::String("0123456789".into());
        assert!(TwitchSource::new().validate_spec(&spec, &[]).is_ok());
    }

    #[test]
    fn rejects_duplicate_route_broadcasters_case_insensitively() {
        let error = TwitchSource::new()
            .validate_spec(
                &spec_value(),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: serde_json::json!({"broadcasters": ["Example", "example"]}),
                }],
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("duplicate broadcaster"));
    }

    #[test]
    fn rejects_broadcasters_with_invalid_characters() {
        let error = TwitchSource::new()
            .validate_spec(
                &spec_value(),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: serde_json::json!({"broadcasters": ["dash-name"]}),
                }],
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("invalid characters"));
    }

    #[test]
    fn matches_only_routes_with_the_event_broadcaster() {
        let route_ids = matching_route_ids(
            &[
                RoutePluginInput {
                    route_id: "one".into(),
                    input: serde_json::json!({"broadcasters": ["hanon", "kotoha"]}),
                },
                RoutePluginInput {
                    route_id: "two".into(),
                    input: serde_json::json!({"broadcasters": ["other"]}),
                },
            ],
            "KOTOHA",
            &BroadcasterValidator::ascii_alphanumeric_or_underscore(),
        )
        .unwrap();

        assert_eq!(route_ids, ["one"]);
    }

    #[tokio::test]
    async fn parse_verifies_hmac_and_revides_challenge() {
        let provider = TwitchProvider::new();
        let spec = provider.parse_spec(&spec_value()).unwrap();
        let body = br#"{"subscription":{"type":"stream.online"},"challenge":"abc"}"#;
        let message_id = "msg-id";
        let timestamp = "2024-01-02T03:04:05Z";
        let signature = expected_hmac_sha256_parts(
            "0123456789",
            &[message_id.as_bytes(), timestamp.as_bytes(), body],
        );
        let mut headers = HeaderMap::new();
        headers.insert("twitch-eventsub-message-id", message_id.try_into().unwrap());
        headers.insert(
            "twitch-eventsub-message-timestamp",
            timestamp.try_into().unwrap(),
        );
        headers.insert(
            "twitch-eventsub-message-signature",
            signature.try_into().unwrap(),
        );
        headers.insert(
            "twitch-eventsub-message-type",
            "webhook_callback_verification".try_into().unwrap(),
        );
        let parsed = provider
            .parse(&spec, &headers, &Bytes::from_static(body))
            .await
            .unwrap();
        assert!(
            matches!(parsed, ParsedMessage::Challenge { challenge, .. } if challenge.as_deref() == Some("abc"))
        );
    }

    #[tokio::test]
    async fn parse_rejects_tampered_signature_with_unauthorized() {
        let provider = TwitchProvider::new();
        let spec = provider.parse_spec(&spec_value()).unwrap();
        let body = br#"{"subscription":{"type":"stream.online"},"challenge":"abc"}"#;
        let signature =
            expected_hmac_sha256_parts("0123456789", &["injected-id".as_bytes(), b"time", body]);
        let mut headers = HeaderMap::new();
        headers.insert("twitch-eventsub-message-id", "msg-id".try_into().unwrap());
        headers.insert(
            "twitch-eventsub-message-timestamp",
            "time".try_into().unwrap(),
        );
        headers.insert(
            "twitch-eventsub-message-signature",
            signature.try_into().unwrap(),
        );
        headers.insert(
            "twitch-eventsub-message-type",
            "webhook_callback_verification".try_into().unwrap(),
        );
        let result = provider
            .parse(&spec, &headers, &Bytes::from_static(body))
            .await;
        assert!(matches!(result, Err(WebhookError::Unauthorized)));
    }
}
