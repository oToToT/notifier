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
    PluginMetadata, SourceContext, SourcePlugin, ValidatedSource, schema_value,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
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
    webhook_signature: String,
    broadcasters: Vec<String>,
    #[serde(default = "default_api_base_url")]
    api_base_url: String,
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
        ("webhook_signature", &spec.webhook_signature),
    ] {
        if value.trim().is_empty() {
            bail!("{name} cannot be empty");
        }
    }
    let broadcasters = broadcasters(&spec)?;
    if broadcasters.is_empty() {
        bail!("at least one broadcaster must be configured");
    }
    reject_duplicate_broadcasters(&broadcasters)?;
    Ok(spec)
}

fn broadcasters(spec: &Spec) -> Result<Vec<String>> {
    let values = spec.broadcasters.clone();
    for value in &values {
        if value.trim().is_empty() {
            bail!("broadcasters cannot contain empty values");
        }
    }
    Ok(values)
}

fn reject_duplicate_broadcasters(values: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_ascii_lowercase()) {
            bail!("duplicate broadcaster {value:?}");
        }
    }
    Ok(())
}

#[async_trait]
impl SourcePlugin for TwitCastingSource {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "twitcasting",
            description: "Receives TwitCasting livestart webhooks.",
            spec_schema: schema_value::<Spec>(),
        }
    }

    fn template_context_schema(&self) -> Value {
        schema_value::<TwitCastingTemplateContext>()
    }

    fn template_variables(&self) -> Vec<String> {
        vec!["event".into(), "broadcaster".into(), "movie".into()]
    }

    fn validate_spec(&self, spec: &Value) -> Result<ValidatedSource> {
        let spec = parse_spec(spec)?;
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
        let spec = parse_spec(&context.spec)?;
        let hooks = list_webhooks(&spec).await?;
        for broadcaster in broadcasters(&spec)? {
            let user_id = resolve_user(&spec, &broadcaster).await?;
            let exists = hooks
                .webhooks
                .iter()
                .any(|hook| hook.user_id == user_id && hook.event == WebhookEvent::LiveStart);
            if !exists {
                create_webhook(&spec, &user_id).await?;
            }
        }
        Ok(())
    }
}

async fn webhook(State(state): State<WebhookState>, body: Bytes) -> Response {
    match decode_webhook(&body)
        .context("invalid TwitCasting webhook")
        .and_then(|payload| handle_webhook(&state, payload))
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

fn handle_webhook(state: &WebhookState, body: WebhookPayload) -> Result<()> {
    let WebhookPayload::LiveStart {
        signature,
        movie,
        broadcaster,
    } = body
    else {
        return Ok(());
    };
    let spec = parse_spec(&state.context.spec)?;
    let broadcasters = broadcasters(&spec)?;
    if spec.webhook_signature != signature.expose_secret()
        || !broadcasters
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(broadcaster.screen_id.as_str()))
    {
        bail!("signature or broadcaster did not match");
    }
    if movie.user_id != broadcaster.id {
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
    state.context.sink.ingest(
        &state.context.source_id,
        &state.context.route_ids,
        &dedupe_key,
        &context,
    )?;
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
            webhook_signature: "signature".into(),
            broadcasters: vec!["example".into(), "another".into()],
            api_base_url: default_api_base_url(),
        };
        let validated = TwitCastingSource::new()
            .validate_spec(&serde_json::to_value(spec).unwrap())
            .unwrap();
        assert_eq!(validated.http_paths, ["/hooks/twitcasting"]);
    }
}
