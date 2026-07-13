use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use notifier_runtime::{
    PluginMetadata, RoutePluginInput, SourceContext, SourcePlugin, ValidatedSource, schema_value,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tracing::{debug, info, warn};
use twitcasting::{
    AppAuth, Client, ScreenId, UserRef, WebhookEvent, WebhookEvents, WebhookListRequest,
    WebhookPayload, decode_webhook,
};
use url::Url;

#[derive(Clone)]
pub struct TwitCastingSource {}

impl TwitCastingSource {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for TwitCastingSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    webhook_path: String,
    client_id: String,
    client_secret: String,
    #[serde(default = "default_api_base_url")]
    api_base_url: String,
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

#[derive(Clone)]
struct WebhookState {
    context: SourceContext,
}

fn default_api_base_url() -> String {
    "https://apiv2.twitcasting.tv".into()
}

fn parse_spec(value: &Value) -> Result<Spec> {
    let spec: Spec = serde_json::from_value(value.clone()).context("invalid TwitCasting spec")?;
    for (name, value) in [
        ("client_id", &spec.client_id),
        ("client_secret", &spec.client_secret),
    ] {
        if value.trim().is_empty() {
            bail!("{name} cannot be empty");
        }
    }
    Ok(spec)
}

fn parse_input(value: &Value) -> Result<Input> {
    let input: Input =
        serde_json::from_value(value.clone()).context("invalid TwitCasting route input")?;
    validate_broadcasters(&input.broadcasters)?;
    Ok(input)
}

fn validate_broadcasters(values: &[String]) -> Result<()> {
    if values.is_empty() {
        bail!("at least one broadcaster must be configured");
    }
    for value in values {
        if value.trim().is_empty() {
            bail!("broadcasters cannot contain empty values");
        }
    }
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_ascii_lowercase()) {
            bail!("duplicate broadcaster {value:?}");
        }
    }
    Ok(())
}

fn configured_broadcasters(inputs: &[RoutePluginInput]) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    for route in inputs {
        let input = parse_input(&route.input)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        for broadcaster in input.broadcasters {
            if seen.insert(broadcaster.to_ascii_lowercase()) {
                values.push(broadcaster);
            }
        }
    }
    Ok(values)
}

fn matching_route_ids(inputs: &[RoutePluginInput], broadcaster: &str) -> Result<Vec<String>> {
    let mut route_ids = Vec::new();
    for route in inputs {
        let input = parse_input(&route.input)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        if input
            .broadcasters
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(broadcaster))
        {
            route_ids.push(route.route_id.clone());
        }
    }
    Ok(route_ids)
}

#[async_trait]
impl SourcePlugin for TwitCastingSource {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "twitcasting",
            description: "Receives TwitCasting livestart webhooks.",
            spec_schema: schema_value::<Spec>(),
            input_schema: schema_value::<Input>(),
        }
    }

    fn template_context_schema(&self) -> Value {
        schema_value::<TwitCastingTemplateContext>()
    }

    fn template_variables(&self) -> Vec<String> {
        vec!["event".into(), "broadcaster".into(), "movie".into()]
    }

    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<ValidatedSource> {
        let spec = parse_spec(spec).inspect_err(|error| {
            debug!(
                route_count = inputs.len(),
                error = %error,
                "rejected TwitCasting source configuration"
            );
        })?;
        configured_broadcasters(inputs).inspect_err(|error| {
            debug!(
                route_count = inputs.len(),
                webhook_path = %spec.webhook_path,
                error = %error,
                "rejected TwitCasting route input configuration"
            );
        })?;
        Ok(ValidatedSource {
            allowed_template_variables: self.template_variables(),
            http_paths: vec![spec.webhook_path],
        })
    }

    fn router(&self, context: SourceContext) -> Router {
        let spec = parse_spec(&context.spec).expect("validated TwitCasting spec");
        Router::new()
            .route(&spec.webhook_path, post(webhook))
            .with_state(WebhookState { context })
    }

    async fn reconcile(&self, context: &SourceContext) -> Result<()> {
        let spec = parse_spec(&context.spec).inspect_err(|error| {
            debug!(
                source_id = %context.source_id,
                error = %error,
                "TwitCasting reconciliation rejected source configuration"
            );
        })?;
        debug!(
            source_id = %context.source_id,
            api_base_url = %spec.api_base_url,
            "listing TwitCasting webhooks"
        );
        let hooks = list_webhooks(&spec).await.inspect_err(|error| {
            debug!(
                source_id = %context.source_id,
                api_base_url = %spec.api_base_url,
                error = %error,
                "failed to list TwitCasting webhooks during reconciliation"
            );
        })?;
        let broadcasters = configured_broadcasters(&context.route_inputs).inspect_err(|error| {
            debug!(
                source_id = %context.source_id,
                route_count = context.route_inputs.len(),
                error = %error,
                "TwitCasting reconciliation rejected route input configuration"
            );
        })?;
        for broadcaster in broadcasters {
            let user_id = resolve_user(&spec, &broadcaster)
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
                create_webhook(&spec, &user_id).await.inspect_err(|error| {
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

async fn webhook(State(state): State<WebhookState>, body: Bytes) -> Response {
    let body_bytes = body.len();
    debug!(
        source_id = %state.context.source_id,
        body_bytes,
        "received TwitCasting webhook"
    );
    match decode_webhook(&body)
        .context("invalid TwitCasting webhook")
        .and_then(|payload| handle_webhook(&state, payload))
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            debug!(
                source_id = %state.context.source_id,
                body_bytes,
                body = %String::from_utf8_lossy(&body),
                error = %error,
                "TwitCasting webhook rejection details"
            );
            warn!(
                source_id = %state.context.source_id,
                body_bytes,
                error = %error,
                "rejected TwitCasting webhook"
            );
            (StatusCode::BAD_REQUEST, error.to_string()).into_response()
        }
    }
}

fn handle_webhook(state: &WebhookState, body: WebhookPayload) -> Result<()> {
    let (movie, broadcaster) = match body {
        WebhookPayload::LiveStart {
            movie,
            broadcaster,
            ..
        } => (movie, broadcaster),
        WebhookPayload::LiveEnd {
            movie, broadcaster, ..
        } => {
            debug!(
                source_id = %state.context.source_id,
                event_kind = "liveend",
                broadcaster_id = %broadcaster.id,
                broadcaster_screen_id = %broadcaster.screen_id,
                movie_id = %movie.id,
                "ignored unsupported TwitCasting webhook event"
            );
            return Ok(());
        }
        WebhookPayload::LiveScheduleCreate { live_schedule, .. } => {
            debug!(
                source_id = %state.context.source_id,
                event_kind = "liveschedulecreate",
                live_schedule_id = %live_schedule.id,
                "ignored unsupported TwitCasting webhook event"
            );
            return Ok(());
        }
        WebhookPayload::LiveScheduleUpdate { live_schedule, .. } => {
            debug!(
                source_id = %state.context.source_id,
                event_kind = "livescheduleupdate",
                live_schedule_id = %live_schedule.id,
                "ignored unsupported TwitCasting webhook event"
            );
            return Ok(());
        }
        WebhookPayload::LiveScheduleDelete {
            live_schedule_id, ..
        } => {
            debug!(
                source_id = %state.context.source_id,
                event_kind = "livescheduledelete",
                live_schedule_id = %live_schedule_id,
                "ignored unsupported TwitCasting webhook event"
            );
            return Ok(());
        }
        WebhookPayload::Unknown {
            event, signature, ..
        } => {
            debug!(
                source_id = %state.context.source_id,
                event_kind = %event,
                has_signature = signature.is_some(),
                "ignored unknown TwitCasting webhook event"
            );
            return Ok(());
        }
    };
    let route_ids = matching_route_ids(&state.context.route_inputs, broadcaster.screen_id.as_str())
        .inspect_err(|error| {
            debug!(
                source_id = %state.context.source_id,
                broadcaster_id = %broadcaster.id,
                broadcaster_screen_id = %broadcaster.screen_id,
                movie_id = %movie.id,
                configured_routes = state.context.route_inputs.len(),
                error = %error,
                "TwitCasting webhook rejected route input configuration"
            );
        })?;
    if route_ids.is_empty() {
        debug!(
            source_id = %state.context.source_id,
            broadcaster_id = %broadcaster.id,
            broadcaster_screen_id = %broadcaster.screen_id,
            movie_id = %movie.id,
            configured_routes = state.context.route_inputs.len(),
            "TwitCasting webhook broadcaster did not match any configured route"
        );
        warn!(
            source_id = %state.context.source_id,
            broadcaster_id = %broadcaster.id,
            broadcaster_screen_id = %broadcaster.screen_id,
            movie_id = %movie.id,
            configured_routes = state.context.route_inputs.len(),
            "rejected TwitCasting webhook for unconfigured broadcaster"
        );
        bail!("broadcaster did not match");
    }
    if movie.user_id != broadcaster.id {
        debug!(
            source_id = %state.context.source_id,
            broadcaster_id = %broadcaster.id,
            broadcaster_screen_id = %broadcaster.screen_id,
            movie_id = %movie.id,
            movie_user_id = %movie.user_id,
            "ignored TwitCasting webhook because movie owner did not match broadcaster"
        );
        return Ok(());
    }

    let movie_id = movie.id.to_string();
    let dedupe_key = format!(
        "{:x}",
        Sha256::digest(format!("livestart\0{}\0{}", broadcaster.id, movie_id).as_bytes())
    );
    let context = json!({
        "event": {
            "id": dedupe_key,
            "kind": "livestart",
        },
        "broadcaster": {
            "id": broadcaster.id,
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
    let delivery_count = state
        .context
        .sink
        .ingest(&state.context.source_id, &route_ids, &dedupe_key, &context)
        .inspect_err(|error| {
            debug!(
                source_id = %state.context.source_id,
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
        source_id = %state.context.source_id,
        broadcaster_id = %broadcaster.id,
        broadcaster_screen_id = %broadcaster.screen_id,
        movie_id,
        route_count = route_ids.len(),
        delivery_count,
        dedupe_key,
        "accepted TwitCasting livestart webhook"
    );
    Ok(())
}

fn client(spec: &Spec) -> Result<Client<AppAuth>> {
    let auth = AppAuth::new(spec.client_id.clone(), spec.client_secret.clone());
    Ok(Client::builder(auth)?
        .base_url(Url::parse(&spec.api_base_url)?)
        .build()?)
}

async fn resolve_user(spec: &Spec, broadcaster: &str) -> Result<twitcasting::UserId> {
    let response = client(spec)?
        .users()
        .get(&UserRef::from(ScreenId::new(broadcaster)))
        .await
        .with_context(|| format!("failed to resolve TwitCasting broadcaster {broadcaster:?}"))?;
    Ok(response.value.user.id)
}

async fn list_webhooks(spec: &Spec) -> Result<twitcasting::WebhookList> {
    Ok(client(spec)?
        .webhooks()
        .list(&WebhookListRequest::default())
        .await
        .context("failed to list TwitCasting webhooks")?
        .value)
}

async fn create_webhook(spec: &Spec, user_id: &twitcasting::UserId) -> Result<()> {
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

    #[test]
    fn validates_configured_webhook_path() {
        let spec = Spec {
            webhook_path: "/hooks/twitcasting".into(),
            client_id: "client".into(),
            client_secret: "secret".into(),
            api_base_url: default_api_base_url(),
        };
        let validated = TwitCastingSource::new()
            .validate_spec(
                &serde_json::to_value(spec).unwrap(),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: json!({"broadcasters": ["example", "another"]}),
                }],
            )
            .unwrap();
        assert_eq!(validated.http_paths, ["/hooks/twitcasting"]);
    }

    #[test]
    fn rejects_duplicate_route_broadcasters_case_insensitively() {
        let spec = serde_json::json!({
            "webhook_path": "/hooks/twitcasting",
            "client_id": "client",
            "client_secret": "secret"
        });
        let error = TwitCastingSource::new()
            .validate_spec(
                &spec,
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: json!({"broadcasters": ["Example", "example"]}),
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
                    input: json!({"broadcasters": ["hanon", "kotoha"]}),
                },
                RoutePluginInput {
                    route_id: "two".into(),
                    input: json!({"broadcasters": ["other"]}),
                },
            ],
            "KOTOHA",
        )
        .unwrap();

        assert_eq!(route_ids, ["one"]);
    }
}
