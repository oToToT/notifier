use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use futures::TryStreamExt;
use notifier_runtime::{
    PluginMetadata, SourceContext, SourcePlugin, ValidatedSource, schema_value,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use twitch_api::{
    TwitchClient,
    eventsub::{Event, EventType, Transport, stream::StreamOnlineV1},
    helix::streams::GetStreamsRequest,
    twitch_oauth2::{AppAccessToken, ClientId, ClientSecret},
};

#[derive(Clone)]
pub struct TwitchSource {
    client: TwitchClient<'static, reqwest13::Client>,
}

impl TwitchSource {
    pub fn new() -> Self {
        Self {
            client: TwitchClient::default(),
        }
    }
}

impl Default for TwitchSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    client_id: String,
    client_secret: String,
    webhook_secret: String,
    broadcaster: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct TwitchTemplateContext {
    event: EventContext,
    broadcaster: BroadcasterContext,
    stream: StreamContext,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct EventContext {
    id: String,
    kind: String,
    occurred_at: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct BroadcasterContext {
    id: String,
    login: String,
    name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct StreamContext {
    title: String,
    url: String,
}

#[derive(Clone)]
struct WebhookState {
    client: TwitchClient<'static, reqwest13::Client>,
    context: SourceContext,
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

#[derive(Deserialize)]
struct TwitchEvent {
    broadcaster_user_id: String,
    broadcaster_user_login: String,
    broadcaster_user_name: String,
    started_at: Option<String>,
}

fn parse_spec(value: &Value) -> Result<Spec> {
    let spec: Spec = serde_json::from_value(value.clone()).context("invalid Twitch spec")?;
    for (name, value) in [
        ("client_id", &spec.client_id),
        ("client_secret", &spec.client_secret),
        ("webhook_secret", &spec.webhook_secret),
        ("broadcaster", &spec.broadcaster),
    ] {
        if value.trim().is_empty() {
            bail!("{name} cannot be empty");
        }
    }
    if spec.webhook_secret.len() < 10 || spec.webhook_secret.len() > 100 {
        bail!("webhook_secret must contain 10 to 100 characters");
    }
    if !spec
        .broadcaster
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        bail!("broadcaster contains invalid characters");
    }
    Ok(spec)
}

fn watch_key(spec: &Spec) -> String {
    let material = format!(
        "{}\0{}\0{}\0{}",
        spec.client_id,
        spec.client_secret,
        spec.webhook_secret,
        spec.broadcaster.to_ascii_lowercase()
    );
    hex::encode(Sha256::digest(material.as_bytes()))
}

#[async_trait]
impl SourcePlugin for TwitchSource {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "twitch",
            description: "Receives Twitch EventSub stream.online webhooks.",
            spec_schema: schema_value::<Spec>(),
        }
    }

    fn template_context_schema(&self) -> Value {
        schema_value::<TwitchTemplateContext>()
    }

    fn template_variables(&self) -> Vec<String> {
        vec!["event".into(), "broadcaster".into(), "stream".into()]
    }

    fn validate_spec(&self, spec: &Value) -> Result<ValidatedSource> {
        let spec = parse_spec(spec)?;
        Ok(ValidatedSource {
            watch_key: watch_key(&spec),
            allowed_template_variables: self.template_variables(),
        })
    }

    fn router(&self, context: SourceContext) -> Router {
        Router::new()
            .route("/webhooks/twitch", post(webhook))
            .with_state(WebhookState {
                client: self.client.clone(),
                context,
            })
    }

    async fn reconcile(&self, context: &SourceContext) -> Result<()> {
        let callback = context
            .public_base_url
            .join("/webhooks/twitch")?
            .to_string();
        let mut unique = HashMap::new();
        for route in &context.routes {
            unique
                .entry(route.watch_key.clone())
                .or_insert(parse_spec(&route.spec)?);
        }
        for spec in unique.values() {
            let token = token(&self.client, spec).await?;
            let broadcaster = resolve_user(&self.client, spec, &token).await?;
            let pages = self
                .client
                .helix
                .get_eventsub_subscriptions(None, Some(EventType::StreamOnline), None, &token)
                .try_collect::<Vec<_>>()
                .await?;
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
                self.client
                    .helix
                    .create_eventsub_subscription(
                        StreamOnlineV1::broadcaster_user_id(broadcaster.id),
                        Transport::webhook(&callback, spec.webhook_secret.clone()),
                        &token,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

async fn webhook(State(state): State<WebhookState>, headers: HeaderMap, body: Bytes) -> Response {
    match handle_webhook(&state, &headers, &body).await {
        Ok(response) => response,
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn handle_webhook(
    state: &WebhookState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let message_id = header(headers, "twitch-eventsub-message-id")?;
    let timestamp = header(headers, "twitch-eventsub-message-timestamp")?;
    let signature = header(headers, "twitch-eventsub-message-signature")?;
    let message_type = header(headers, "twitch-eventsub-message-type")?;
    let parsed: WebhookBody = serde_json::from_slice(body).context("invalid webhook JSON")?;

    let matching_specs: Vec<(String, Spec)> = state
        .context
        .routes
        .iter()
        .filter_map(|route| {
            let spec = parse_spec(&route.spec).ok()?;
            verify_signature(&spec.webhook_secret, message_id, timestamp, body, signature)
                .then(|| (route.route_id.clone(), spec))
        })
        .collect();
    if matching_specs.is_empty() {
        return Ok(StatusCode::UNAUTHORIZED.into_response());
    }

    match message_type {
        "webhook_callback_verification" => {
            let challenge = parsed.challenge.context("challenge is missing")?;
            Ok((StatusCode::OK, challenge).into_response())
        }
        "revocation" => Ok(StatusCode::OK.into_response()),
        "notification" => {
            if parsed.subscription.kind != "stream.online" {
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
            let event = parsed.event.context("event is missing")?;
            let selected: Vec<(String, Spec)> = matching_specs
                .into_iter()
                .filter(|(_, spec)| {
                    spec.broadcaster
                        .eq_ignore_ascii_case(&event.broadcaster_user_login)
                })
                .collect();
            if selected.is_empty() {
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
            let spec = &selected[0].1;
            let access_token = token(&state.client, spec).await?;
            let stream = get_stream(
                &state.client,
                spec,
                &access_token,
                &event.broadcaster_user_id,
            )
            .await?;
            let context = json!({
                "event": {
                    "id": message_id,
                    "kind": "stream.online",
                    "occurred_at": event.started_at.unwrap_or_else(|| timestamp.to_owned()),
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
            let route_ids = selected
                .into_iter()
                .map(|(route_id, _)| route_id)
                .collect::<Vec<_>>();
            state
                .context
                .sink
                .ingest("twitch", &route_ids, message_id, &context)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        _ => Ok(StatusCode::BAD_REQUEST.into_response()),
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    headers
        .get(name)
        .context("required Twitch header is missing")?
        .to_str()
        .context("Twitch header is not valid UTF-8")
}

fn verify_signature(
    secret: &str,
    message_id: &str,
    timestamp: &str,
    body: &[u8],
    signature: &str,
) -> bool {
    let Ok(request) = http::Request::builder()
        .header("Twitch-Eventsub-Message-Id", message_id)
        .header("Twitch-Eventsub-Message-Timestamp", timestamp)
        .header("Twitch-Eventsub-Message-Signature", signature)
        .body(body)
    else {
        return false;
    };
    Event::verify_payload(&request, secret.as_bytes())
}

async fn token(
    client: &TwitchClient<'static, reqwest13::Client>,
    spec: &Spec,
) -> Result<AppAccessToken> {
    AppAccessToken::get_app_access_token(
        client,
        ClientId::new(spec.client_id.clone()),
        ClientSecret::new(spec.client_secret.clone()),
        vec![],
    )
    .await
    .context("failed to request Twitch app access token")
}

async fn resolve_user(
    client: &TwitchClient<'static, reqwest13::Client>,
    spec: &Spec,
    token: &AppAccessToken,
) -> Result<twitch_api::helix::users::User> {
    client
        .helix
        .get_user_from_login(spec.broadcaster.as_str(), token)
        .await?
        .with_context(|| format!("Twitch broadcaster {:?} was not found", spec.broadcaster))
}

async fn get_stream(
    client: &TwitchClient<'static, reqwest13::Client>,
    _spec: &Spec,
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
    use hmac::{Hmac, Mac};

    #[test]
    fn verifies_hmac_and_rejects_changes() {
        let secret = "0123456789";
        let body = br#"{"event":{}}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(b"id");
        mac.update(b"time");
        mac.update(body);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        assert!(verify_signature(secret, "id", "time", body, &signature));
        assert!(!verify_signature(
            secret, "id", "time", b"changed", &signature
        ));
    }

    #[test]
    fn groups_identical_watches_without_exposing_credentials() {
        let spec = Spec {
            client_id: "id".into(),
            client_secret: "secret".into(),
            webhook_secret: "0123456789".into(),
            broadcaster: "Example".into(),
        };
        assert_eq!(watch_key(&spec), watch_key(&spec));
        assert!(!watch_key(&spec).contains("secret"));
    }
}
