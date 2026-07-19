use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{Router, body::Bytes, http::HeaderMap};
use notifier_runtime::{
    PluginMetadata, RoutePluginInput, SourceContext, SourcePlugin, ValidatedSource, schema_value,
};
use notifier_webhook::{
    BroadcasterValidator, CommonSpec, WebhookError, WebhookOutcome, WebhookProvider, WebhookSource,
    configured_broadcasters, dedupe_sha256, matching_route_ids, validate_common_spec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, error, info, warn};
use twitcasting::{
    AppAuth, Client, ScreenId, SecretString, UserRef, WebhookEvent, WebhookEvents,
    WebhookListRequest, WebhookPayload, decode_webhook,
};
use url::Url;

pub struct TwitCastingSource(pub WebhookSource<TwitCastingProvider>);

impl TwitCastingSource {
    pub fn new() -> Self {
        Self(WebhookSource::new(TwitCastingProvider))
    }
}

impl Default for TwitCastingSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourcePlugin for TwitCastingSource {
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
pub struct TwitCastingProvider;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TwitCastingSpec {
    #[serde(flatten)]
    pub common: CommonSpec,
    pub webhook_signature: String,
    #[serde(default = "default_enforce_signature_verification")]
    pub enforce_signature_verification: bool,
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
}

fn default_api_base_url() -> String {
    "https://apiv2.twitcasting.tv".into()
}

fn default_enforce_signature_verification() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct Input {
    broadcasters: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct TwitCastingTemplateContext {
    event: EventContext,
    broadcaster: BroadcasterContext,
    movie: MovieContext,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct EventContext {
    id: String,
    kind: String,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct BroadcasterContext {
    id: String,
    screen_id: String,
    name: String,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct MovieContext {
    id: String,
    title: String,
    subtitle: String,
    comment: String,
    url: String,
}

#[async_trait]
impl WebhookProvider for TwitCastingProvider {
    type Spec = TwitCastingSpec;
    type Event = WebhookPayload;

    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "twitcasting",
            description: "Receives TwitCasting livestart webhooks.",
            spec_schema: schema_value::<TwitCastingSpec>(),
            input_schema: schema_value::<Input>(),
        }
    }

    fn template_context_schema(&self) -> Value {
        schema_value::<TwitCastingTemplateContext>()
    }

    fn template_variables(&self) -> Vec<String> {
        vec!["event".into(), "broadcaster".into(), "movie".into()]
    }

    fn webhook_path(&self, spec: &Self::Spec) -> String {
        spec.common.webhook_path.clone()
    }

    fn parse_spec(&self, value: &Value) -> Result<Self::Spec> {
        let spec: TwitCastingSpec =
            serde_json::from_value(value.clone()).context("invalid TwitCasting spec")?;
        validate_common_spec(&spec.common).context("invalid TwitCasting spec")?;
        if spec.webhook_signature.trim().is_empty() {
            bail!("webhook_signature cannot be empty");
        }
        Ok(spec)
    }

    async fn parse(
        &self,
        spec: &Self::Spec,
        _headers: &HeaderMap,
        body: &Bytes,
    ) -> Result<Self::Event, WebhookError> {
        let payload = decode_webhook(body)
            .context("invalid TwitCasting webhook")
            .map_err(|error| {
                debug!(
                    body_bytes = body.len(),
                    body = %String::from_utf8_lossy(body),
                    error = %error,
                    "TwitCasting webhook decode failure details"
                );
                warn!(
                    body_bytes = body.len(),
                    "rejected undecodable TwitCasting webhook"
                );
                WebhookError::BadRequest(error.to_string())
            })?;
        verify_signature(spec, &payload, body)?;
        Ok(payload)
    }

    async fn dispatch(
        &self,
        _spec: &Self::Spec,
        payload: Self::Event,
        context: &SourceContext,
    ) -> Result<WebhookOutcome> {
        let (movie, broadcaster) = match payload {
            WebhookPayload::LiveStart {
                movie, broadcaster, ..
            } => (movie, broadcaster),
            WebhookPayload::LiveEnd {
                movie, broadcaster, ..
            } => {
                debug!(
                    source_id = %context.source_id,
                    event_kind = "liveend",
                    broadcaster_id = %broadcaster.id,
                    broadcaster_screen_id = %broadcaster.screen_id,
                    movie_id = %movie.id,
                    "ignored unsupported TwitCasting webhook event"
                );
                return Ok(WebhookOutcome::Ignored);
            }
            WebhookPayload::LiveScheduleCreate { live_schedule, .. } => {
                debug!(
                    source_id = %context.source_id,
                    event_kind = "liveschedulecreate",
                    live_schedule_id = %live_schedule.id,
                    "ignored unsupported TwitCasting webhook event"
                );
                return Ok(WebhookOutcome::Ignored);
            }
            WebhookPayload::LiveScheduleUpdate { live_schedule, .. } => {
                debug!(
                    source_id = %context.source_id,
                    event_kind = "livescheduleupdate",
                    live_schedule_id = %live_schedule.id,
                    "ignored unsupported TwitCasting webhook event"
                );
                return Ok(WebhookOutcome::Ignored);
            }
            WebhookPayload::LiveScheduleDelete {
                live_schedule_id, ..
            } => {
                debug!(
                    source_id = %context.source_id,
                    event_kind = "livescheduledelete",
                    live_schedule_id = %live_schedule_id,
                    "ignored unsupported TwitCasting webhook event"
                );
                return Ok(WebhookOutcome::Ignored);
            }
            WebhookPayload::Unknown {
                event, signature, ..
            } => {
                debug!(
                    source_id = %context.source_id,
                    event_kind = %event,
                    has_signature = signature.is_some(),
                    "ignored unknown TwitCasting webhook event"
                );
                return Ok(WebhookOutcome::Ignored);
            }
        };

        let route_ids = matching_route_ids(
            &context.route_inputs,
            broadcaster.screen_id.as_str(),
            &BroadcasterValidator::none(),
        )
        .inspect_err(|error| {
            debug!(
                source_id = %context.source_id,
                broadcaster_id = %broadcaster.id,
                broadcaster_screen_id = %broadcaster.screen_id,
                movie_id = %movie.id,
                configured_routes = context.route_inputs.len(),
                error = %error,
                "TwitCasting webhook rejected route input configuration"
            );
        })?;
        if route_ids.is_empty() {
            debug!(
                source_id = %context.source_id,
                broadcaster_id = %broadcaster.id,
                broadcaster_screen_id = %broadcaster.screen_id,
                movie_id = %movie.id,
                configured_routes = context.route_inputs.len(),
                "TwitCasting webhook broadcaster did not match any configured route"
            );
            warn!(
                source_id = %context.source_id,
                broadcaster_id = %broadcaster.id,
                broadcaster_screen_id = %broadcaster.screen_id,
                movie_id = %movie.id,
                configured_routes = context.route_inputs.len(),
                "ignored TwitCasting webhook for unconfigured broadcaster"
            );
            return Ok(WebhookOutcome::Ignored);
        }
        if movie.user_id != broadcaster.id {
            debug!(
                source_id = %context.source_id,
                broadcaster_id = %broadcaster.id,
                broadcaster_screen_id = %broadcaster.screen_id,
                movie_id = %movie.id,
                movie_user_id = %movie.user_id,
                "ignored TwitCasting webhook because movie owner did not match broadcaster"
            );
            return Ok(WebhookOutcome::Ignored);
        }

        let movie_id = movie.id.to_string();
        let broadcaster_id = broadcaster.id.to_string();
        let dedupe_key = dedupe_sha256("livestart", &[&broadcaster_id, &movie_id]);
        let context_json = json!({
            "event": {
                "id": dedupe_key,
                "kind": "livestart",
            },
            "broadcaster": {
                "id": broadcaster_id,
                "screen_id": broadcaster.screen_id,
                "name": broadcaster.name,
            },
            "movie": {
                "id": movie_id,
                "title": movie.title,
                "subtitle": movie.subtitle.unwrap_or_default(),
                "comment": movie.last_owner_comment.unwrap_or_default(),
                "url": movie.link,
            }
        });
        let delivery_count = context
            .sink
            .ingest(&context.source_id, &route_ids, &dedupe_key, &context_json)
            .inspect_err(|error| {
                debug!(
                    source_id = %context.source_id,
                    broadcaster_id = %broadcaster.id,
                    broadcaster_screen_id = %broadcaster.screen_id,
                    movie_id,
                    route_count = route_ids.len(),
                    dedupe_key,
                    error = %error,
                    "failed to ingest TwitCasting webhook delivery"
                );
            })?;
        info!(
            source_id = %context.source_id,
            broadcaster_id = %broadcaster.id,
            broadcaster_screen_id = %broadcaster.screen_id,
            movie_id,
            route_count = route_ids.len(),
            delivery_count,
            dedupe_key,
            "accepted TwitCasting livestart webhook"
        );
        Ok(WebhookOutcome::Accepted)
    }

    async fn reconcile(&self, spec: &Self::Spec, context: &SourceContext) -> Result<()> {
        debug!(
            source_id = %context.source_id,
            api_base_url = %spec.api_base_url,
            "listing TwitCasting webhooks"
        );
        let hooks = list_webhooks(spec).await.inspect_err(|error| {
            debug!(
                source_id = %context.source_id,
                api_base_url = %spec.api_base_url,
                error = %error,
                "failed to list TwitCasting webhooks during reconciliation"
            );
        })?;
        let broadcasters =
            configured_broadcasters(&context.route_inputs, &BroadcasterValidator::none())
                .inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        route_count = context.route_inputs.len(),
                        error = %error,
                        "TwitCasting reconciliation rejected route input configuration"
                    );
                })?;
        for broadcaster in broadcasters {
            let user_id = resolve_user(spec, &broadcaster)
                .await
                .inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        broadcaster,
                        api_base_url = %spec.api_base_url,
                        error = %error,
                        "failed to resolve TwitCasting broadcaster during reconciliation"
                    );
                })?;
            let exists = hooks
                .webhooks
                .iter()
                .any(|hook| hook.user_id == user_id && hook.event == WebhookEvent::LiveStart);
            if !exists {
                debug!(
                    source_id = %context.source_id,
                    broadcaster,
                    user_id = %user_id,
                    "creating TwitCasting livestart webhook"
                );
                create_webhook(spec, &user_id).await.inspect_err(|error| {
                    debug!(
                        source_id = %context.source_id,
                        broadcaster,
                        user_id = %user_id,
                        api_base_url = %spec.api_base_url,
                        error = %error,
                        "failed to create TwitCasting livestart webhook during reconciliation"
                    );
                })?;
            } else {
                debug!(
                    source_id = %context.source_id,
                    broadcaster,
                    user_id = %user_id,
                    "TwitCasting livestart webhook already exists"
                );
            }
        }
        Ok(())
    }
}

fn payload_signature(payload: &WebhookPayload) -> Option<&SecretString> {
    match payload {
        WebhookPayload::LiveStart { signature, .. }
        | WebhookPayload::LiveEnd { signature, .. }
        | WebhookPayload::LiveScheduleCreate { signature, .. }
        | WebhookPayload::LiveScheduleUpdate { signature, .. }
        | WebhookPayload::LiveScheduleDelete { signature, .. } => Some(signature),
        WebhookPayload::Unknown { signature, .. } => signature.as_ref(),
    }
}

fn verify_signature(
    spec: &TwitCastingSpec,
    payload: &WebhookPayload,
    body: &Bytes,
) -> Result<(), WebhookError> {
    let received = payload_signature(payload).map(SecretString::expose_secret);
    if received == Some(spec.webhook_signature.as_str()) {
        debug!("TwitCasting webhook signature verified");
        return Ok(());
    }

    if spec.enforce_signature_verification {
        error!(
            body_bytes = body.len(),
            has_signature = received.is_some(),
            "rejected TwitCasting webhook with invalid signature"
        );
        return Err(WebhookError::Unauthorized);
    }

    debug!(
        body_bytes = body.len(),
        body = %String::from_utf8_lossy(body),
        "TwitCasting webhook signature mismatch request body"
    );
    warn!(
        body_bytes = body.len(),
        has_signature = received.is_some(),
        "TwitCasting webhook signature mismatch (processing anyway)"
    );
    Ok(())
}

fn client(spec: &TwitCastingSpec) -> Result<Client<AppAuth>> {
    let auth = AppAuth::new(
        spec.common.client_id.clone(),
        spec.common.client_secret.clone(),
    );
    Ok(Client::builder(auth)?
        .base_url(Url::parse(&spec.api_base_url)?)
        .build()?)
}

async fn resolve_user(spec: &TwitCastingSpec, broadcaster: &str) -> Result<twitcasting::UserId> {
    let response = client(spec)?
        .users()
        .get(&UserRef::from(ScreenId::new(broadcaster)))
        .await
        .with_context(|| format!("failed to resolve TwitCasting broadcaster {broadcaster:?}"))?;
    Ok(response.value.user.id)
}

async fn list_webhooks(spec: &TwitCastingSpec) -> Result<twitcasting::WebhookList> {
    Ok(client(spec)?
        .webhooks()
        .list(&WebhookListRequest::default())
        .await
        .context("failed to list TwitCasting webhooks")?
        .value)
}

async fn create_webhook(spec: &TwitCastingSpec, user_id: &twitcasting::UserId) -> Result<()> {
    client(spec)?
        .webhooks()
        .register(user_id, &WebhookEvents::new([WebhookEvent::LiveStart]))
        .await
        .context("failed to create TwitCasting livestart webhook")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_value() -> Value {
        serde_json::json!({
            "webhook_path": "/hooks/twitcasting",
            "client_id": "client",
            "client_secret": "secret",
            "webhook_signature": "expected-signature"
        })
    }

    #[test]
    fn requires_nonempty_webhook_signature() {
        let mut missing = spec_value();
        missing.as_object_mut().unwrap().remove("webhook_signature");
        let error = TwitCastingSource::new()
            .validate_spec(&missing, &[])
            .unwrap_err();
        assert!(format!("{error:#}").contains("webhook_signature"));

        let mut empty = spec_value();
        empty["webhook_signature"] = "  ".into();
        let error = TwitCastingSource::new()
            .validate_spec(&empty, &[])
            .unwrap_err();
        assert!(format!("{error:#}").contains("webhook_signature cannot be empty"));
    }

    #[tokio::test]
    async fn signature_mismatch_is_allowed_when_enforcement_is_disabled() {
        let mut value = spec_value();
        value["enforce_signature_verification"] = false.into();
        let spec = TwitCastingProvider.parse_spec(&value).unwrap();
        let body = Bytes::from_static(br#"{"event":"future","signature":"unexpected-signature"}"#);

        assert!(
            TwitCastingProvider
                .parse(&spec, &HeaderMap::new(), &body)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn signature_mismatch_is_rejected_by_default() {
        let spec = TwitCastingProvider.parse_spec(&spec_value()).unwrap();
        let body = Bytes::from_static(br#"{"event":"future","signature":"unexpected-signature"}"#);

        let result = TwitCastingProvider
            .parse(&spec, &HeaderMap::new(), &body)
            .await;
        assert!(matches!(result, Err(WebhookError::Unauthorized)));
    }

    #[test]
    fn validates_configured_webhook_path() {
        let validated = TwitCastingSource::new()
            .validate_spec(
                &spec_value(),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: serde_json::json!({"broadcasters": ["example", "another"]}),
                }],
            )
            .unwrap();
        assert_eq!(validated.http_paths, ["/hooks/twitcasting"]);
    }

    #[test]
    fn rejects_duplicate_route_broadcasters_case_insensitively() {
        let error = TwitCastingSource::new()
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
            &BroadcasterValidator::none(),
        )
        .unwrap();

        assert_eq!(route_ids, ["one"]);
    }

    #[test]
    fn unexpected_event_broadcaster_is_ignored() {
        // Sanity: matching_route_ids returns empty for unmatched broadcasters. The
        // dispatcher treats that empty vector as Ignored (HTTP 204).
        let route_ids = matching_route_ids(
            &[RoutePluginInput {
                route_id: "only".into(),
                input: serde_json::json!({"broadcasters": ["known"]}),
            }],
            "unknown",
            &BroadcasterValidator::none(),
        )
        .unwrap();
        assert!(route_ids.is_empty());
    }

    #[test]
    fn allows_permissive_broadcaster_characters() {
        TwitCastingSource::new()
            .validate_spec(
                &spec_value(),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: serde_json::json!({"broadcasters": ["name-dash", "日本"]}),
                }],
            )
            .unwrap();
    }
}
