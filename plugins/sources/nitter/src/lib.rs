use std::{collections::HashSet, io::Cursor, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::Router;
use notifier_runtime::{
    PluginMetadata, RoutePluginInput, SourceContext, SourcePlugin, ValidatedSource, schema_value,
};
use rand::Rng;
use rss::Channel;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing::{info, warn};
use url::Url;

const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 300;
const MIN_POLL_INTERVAL_SECONDS: u64 = 60;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 20;
const MIN_REQUEST_TIMEOUT_SECONDS: u64 = 5;
const DEFAULT_RETRY_INITIAL_SECONDS: u64 = 30;
const DEFAULT_RETRY_MAX_SECONDS: u64 = 1_800;

#[derive(Clone)]
pub struct NitterSource {
    client: reqwest::Client,
}

impl NitterSource {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for NitterSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    instance_url: Url,
    #[serde(default)]
    tweet_url_base: Option<Url>,
    #[serde(default)]
    first_fetch: FirstFetch,
    #[serde(default = "default_poll_interval_seconds")]
    poll_interval_seconds: u64,
    #[serde(default = "default_request_timeout_seconds")]
    request_timeout_seconds: u64,
    #[serde(default = "default_retry_initial_seconds")]
    retry_initial_seconds: u64,
    #[serde(default = "default_retry_max_seconds")]
    retry_max_seconds: u64,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FirstFetch {
    #[default]
    MarkSeen,
    NotifyExisting,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct Input {
    users: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct NitterTemplateContext {
    event: EventContext,
    user: UserContext,
    tweet: TweetContext,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct EventContext {
    id: String,
    kind: String,
    published_at: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct UserContext {
    username: String,
    rss_url: String,
    profile_url: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct TweetContext {
    id: String,
    title: String,
    description: String,
    url: String,
    published_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TweetItem {
    key: String,
    id: String,
    title: String,
    description: String,
    url: String,
    published_at: String,
}

fn default_poll_interval_seconds() -> u64 {
    DEFAULT_POLL_INTERVAL_SECONDS
}

fn default_request_timeout_seconds() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECONDS
}

fn default_retry_initial_seconds() -> u64 {
    DEFAULT_RETRY_INITIAL_SECONDS
}

fn default_retry_max_seconds() -> u64 {
    DEFAULT_RETRY_MAX_SECONDS
}

fn parse_spec(value: &Value) -> Result<Spec> {
    let spec: Spec = serde_json::from_value(value.clone()).context("invalid Nitter spec")?;
    validate_url_root("instance_url", &spec.instance_url)?;
    if let Some(tweet_url_base) = &spec.tweet_url_base {
        validate_url_root("tweet_url_base", tweet_url_base)?;
    }
    if spec.poll_interval_seconds < MIN_POLL_INTERVAL_SECONDS {
        bail!("poll_interval_seconds must be at least {MIN_POLL_INTERVAL_SECONDS}");
    }
    if spec.request_timeout_seconds < MIN_REQUEST_TIMEOUT_SECONDS {
        bail!("request_timeout_seconds must be at least {MIN_REQUEST_TIMEOUT_SECONDS}");
    }
    if spec.retry_initial_seconds == 0 {
        bail!("retry_initial_seconds must be greater than zero");
    }
    if spec.retry_max_seconds < spec.retry_initial_seconds {
        bail!("retry_max_seconds must be greater than or equal to retry_initial_seconds");
    }
    Ok(spec)
}

fn validate_url_root(name: &str, url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("{name} must use http or https");
    }
    if url.cannot_be_a_base() || url.host_str().is_none() {
        bail!("{name} must be an absolute URL");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("{name} must not contain a query or fragment");
    }
    Ok(())
}

fn parse_input(value: &Value) -> Result<Input> {
    let input: Input =
        serde_json::from_value(value.clone()).context("invalid Nitter route input")?;
    validate_users(&input.users)?;
    Ok(input)
}

fn validate_users(values: &[String]) -> Result<()> {
    if values.is_empty() {
        bail!("at least one user must be configured");
    }
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            bail!("users cannot contain empty values");
        }
        if value.len() > 15 {
            bail!("user {value:?} is longer than 15 characters");
        }
        if !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            bail!("user {value:?} contains invalid characters");
        }
        if !seen.insert(value.to_ascii_lowercase()) {
            bail!("duplicate user {value:?}");
        }
    }
    Ok(())
}

fn configured_users(inputs: &[RoutePluginInput]) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    for route in inputs {
        let input = parse_input(&route.input)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        for user in input.users {
            if seen.insert(user.to_ascii_lowercase()) {
                values.push(user);
            }
        }
    }
    Ok(values)
}

fn matching_route_ids(inputs: &[RoutePluginInput], user: &str) -> Result<Vec<String>> {
    let mut route_ids = Vec::new();
    for route in inputs {
        let input = parse_input(&route.input)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        if input
            .users
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(user))
        {
            route_ids.push(route.route_id.clone());
        }
    }
    Ok(route_ids)
}

#[async_trait]
impl SourcePlugin for NitterSource {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "nitter",
            description: "Polls Nitter RSS feeds for new tweets.",
            spec_schema: schema_value::<Spec>(),
            input_schema: schema_value::<Input>(),
        }
    }

    fn template_context_schema(&self) -> Value {
        schema_value::<NitterTemplateContext>()
    }

    fn template_variables(&self) -> Vec<String> {
        vec!["event".into(), "user".into(), "tweet".into()]
    }

    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<ValidatedSource> {
        parse_spec(spec)?;
        configured_users(inputs)?;
        Ok(ValidatedSource {
            allowed_template_variables: self.template_variables(),
            http_paths: Vec::new(),
        })
    }

    fn router(&self, _context: SourceContext) -> Router {
        Router::new()
    }

    async fn reconcile(&self, _context: &SourceContext) -> Result<()> {
        Ok(())
    }

    async fn run(&self, context: SourceContext) -> Result<()> {
        let spec = parse_spec(&context.spec)?;
        let users = configured_users(&context.route_inputs)?;
        let mut retry_seconds = spec.retry_initial_seconds;

        loop {
            let mut successes = 0usize;
            for user in &users {
                match poll_user(&self.client, &context, &spec, user).await {
                    Ok(()) => {
                        successes += 1;
                    }
                    Err(error) => {
                        warn!(
                            source_id = %context.source_id,
                            user,
                            %error,
                            retry_seconds,
                            "Nitter fetch failed"
                        );
                    }
                }
            }

            let delay = if successes == 0 {
                let delay = jittered_delay(retry_seconds, spec.retry_max_seconds);
                retry_seconds = retry_seconds.saturating_mul(2).min(spec.retry_max_seconds);
                delay
            } else {
                retry_seconds = spec.retry_initial_seconds;
                spec.poll_interval_seconds
            };
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    }
}

async fn poll_user(
    client: &reqwest::Client,
    context: &SourceContext,
    spec: &Spec,
    user: &str,
) -> Result<()> {
    let rss_url = rss_url(&spec.instance_url, user)?;
    let body = client
        .get(rss_url.clone())
        .timeout(Duration::from_secs(spec.request_timeout_seconds))
        .send()
        .await
        .with_context(|| format!("failed to fetch {rss_url}"))?
        .error_for_status()
        .with_context(|| format!("Nitter returned an error for {rss_url}"))?
        .bytes()
        .await
        .with_context(|| format!("failed to read {rss_url} response"))?;
    let items = parse_feed(&body, spec, user).context("failed to parse Nitter RSS")?;
    process_items(context, spec, user, &rss_url, &items)?;
    Ok(())
}

fn process_items(
    context: &SourceContext,
    spec: &Spec,
    user: &str,
    rss_url: &Url,
    items: &[TweetItem],
) -> Result<usize> {
    let scope = user.to_ascii_lowercase();
    let route_ids = matching_route_ids(&context.route_inputs, user)?;
    if route_ids.is_empty() {
        return Ok(0);
    }

    let baseline_completed = context
        .storage
        .source_baseline_completed(&context.source_id, &scope)?;
    let ordered_items = items.iter().rev().collect::<Vec<_>>();
    if !baseline_completed && spec.first_fetch == FirstFetch::MarkSeen {
        let keys = ordered_items
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        context
            .storage
            .mark_source_items_seen(&context.source_id, &scope, &keys)?;
        context
            .storage
            .complete_source_baseline(&context.source_id, &scope)?;
        info!(source_id = %context.source_id, user, count = keys.len(), "Nitter baseline marked seen");
        return Ok(0);
    }

    let items_to_notify = if baseline_completed {
        let keys = ordered_items
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        let unseen = context
            .storage
            .unseen_source_items(&context.source_id, &scope, &keys)?
            .into_iter()
            .collect::<HashSet<_>>();
        ordered_items
            .into_iter()
            .filter(|item| unseen.contains(&item.key))
            .collect::<Vec<_>>()
    } else {
        ordered_items
    };

    for item in &items_to_notify {
        let event_id = stable_event_id(&context.source_id, user, &item.key);
        let context_json = json!({
            "event": {
                "id": event_id,
                "kind": "tweet",
                "published_at": item.published_at,
            },
            "user": {
                "username": user,
                "rss_url": rss_url.as_str(),
                "profile_url": profile_url(&spec.instance_url, user)?.to_string(),
            },
            "tweet": {
                "id": item.id,
                "title": item.title,
                "description": item.description,
                "url": item.url,
                "published_at": item.published_at,
            }
        });
        context
            .sink
            .ingest(&context.source_id, &route_ids, &event_id, &context_json)?;
    }

    let seen_keys = if baseline_completed {
        items_to_notify
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>()
    } else {
        items
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>()
    };
    context
        .storage
        .mark_source_items_seen(&context.source_id, &scope, &seen_keys)?;
    if !baseline_completed {
        context
            .storage
            .complete_source_baseline(&context.source_id, &scope)?;
    }
    Ok(items_to_notify.len())
}

fn parse_feed(bytes: &[u8], spec: &Spec, user: &str) -> Result<Vec<TweetItem>> {
    let channel = Channel::read_from(Cursor::new(bytes)).context("invalid RSS document")?;
    let mut items = Vec::new();
    for item in channel.items() {
        let key = item
            .guid()
            .map(|guid| guid.value().trim())
            .filter(|value| !value.is_empty())
            .or_else(|| item.link().map(str::trim).filter(|value| !value.is_empty()));
        let Some(key) = key else {
            continue;
        };
        let original_url = item.link().unwrap_or(key).to_owned();
        let (id, url) = tweet_id_and_url(spec, user, &original_url);
        items.push(TweetItem {
            key: key.to_owned(),
            id,
            title: item.title().unwrap_or_default().to_owned(),
            description: item.description().unwrap_or_default().to_owned(),
            url,
            published_at: item.pub_date().unwrap_or_default().to_owned(),
        });
    }
    Ok(items)
}

fn tweet_id_and_url(spec: &Spec, user: &str, original_url: &str) -> (String, String) {
    let parsed_id = Url::parse(original_url)
        .ok()
        .and_then(|url| tweet_id_from_url_path(url.path()));
    match parsed_id {
        Some(id) => {
            let base = spec.tweet_url_base.as_ref().unwrap_or(&spec.instance_url);
            let url = profile_url(base, user)
                .and_then(|profile| {
                    profile
                        .join(&format!("status/{id}"))
                        .context("invalid tweet URL")
                })
                .map(|url| url.to_string())
                .unwrap_or_else(|_| original_url.to_owned());
            (id, url)
        }
        None => (String::new(), original_url.to_owned()),
    }
}

fn tweet_id_from_url_path(path: &str) -> Option<String> {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    while let Some(part) = parts.next() {
        if part == "status" {
            let id = parts.next()?;
            if id.chars().all(|character| character.is_ascii_digit()) {
                return Some(id.to_owned());
            }
        }
    }
    None
}

fn rss_url(instance_url: &Url, user: &str) -> Result<Url> {
    instance_url
        .join(&format!("{user}/rss"))
        .context("invalid RSS URL")
}

fn profile_url(base_url: &Url, user: &str) -> Result<Url> {
    base_url
        .join(&format!("{user}/"))
        .context("invalid profile URL")
}

fn stable_event_id(source_id: &str, user: &str, key: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("nitter\0{source_id}\0{user}\0{key}").as_bytes())
    )
}

fn jittered_delay(base_seconds: u64, max_seconds: u64) -> u64 {
    let capped = base_seconds.min(max_seconds);
    let jitter = rand::rng().random_range(0..=capped / 4);
    capped.saturating_add(jitter).min(max_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn route(input: Value) -> RoutePluginInput {
        RoutePluginInput {
            route_id: "route".into(),
            input,
        }
    }

    fn spec_json() -> Value {
        json!({
            "instance_url": "https://nitter.example",
            "tweet_url_base": "https://fxtwitter.com"
        })
    }

    #[test]
    fn validates_spec_and_users() {
        let validated = NitterSource::new()
            .validate_spec(
                &spec_json(),
                &[route(json!({"users": ["Example", "other"]}))],
            )
            .unwrap();
        assert!(validated.http_paths.is_empty());
        assert_eq!(
            validated.allowed_template_variables,
            ["event", "user", "tweet"]
        );

        let error = NitterSource::new()
            .validate_spec(
                &spec_json(),
                &[route(json!({"users": ["Example", "example"]}))],
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("duplicate user"));
    }

    #[test]
    fn rejects_too_short_intervals() {
        let error = parse_spec(&json!({
            "instance_url": "https://nitter.example",
            "poll_interval_seconds": 10
        }))
        .unwrap_err();
        assert!(format!("{error:#}").contains("poll_interval_seconds"));
    }

    #[test]
    fn constructs_urls_and_matches_routes() {
        let base = Url::parse("https://nitter.example").unwrap();
        assert_eq!(
            rss_url(&base, "User_Name").unwrap().as_str(),
            "https://nitter.example/User_Name/rss"
        );
        assert_eq!(
            profile_url(&base, "User_Name").unwrap().as_str(),
            "https://nitter.example/User_Name/"
        );
        let routes = matching_route_ids(
            &[
                route(json!({"users": ["one", "Two"]})),
                RoutePluginInput {
                    route_id: "miss".into(),
                    input: json!({"users": ["other"]}),
                },
            ],
            "two",
        )
        .unwrap();
        assert_eq!(routes, ["route"]);
    }

    #[test]
    fn parses_feed_and_rewrites_status_urls() {
        let spec = parse_spec(&spec_json()).unwrap();
        let feed = br#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>User</title>
<item>
  <title>new</title>
  <description>body</description>
  <link>https://nitter.example/example/status/12345#m</link>
  <guid>https://nitter.example/example/status/12345#m</guid>
  <pubDate>Wed, 01 Jul 2026 00:00:00 GMT</pubDate>
</item>
</channel></rss>"#;
        let items = parse_feed(feed, &spec, "example").unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "12345");
        assert_eq!(items[0].url, "https://fxtwitter.com/example/status/12345");
        assert_eq!(items[0].published_at, "Wed, 01 Jul 2026 00:00:00 GMT");
    }

    #[test]
    fn uses_link_when_guid_is_missing() {
        let spec = parse_spec(&json!({"instance_url": "https://nitter.example"})).unwrap();
        let feed = br#"<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>a</title><link>https://nitter.example/u/status/9</link></item>
</channel></rss>"#;
        let items = parse_feed(feed, &spec, "u").unwrap();
        assert_eq!(items[0].key, "https://nitter.example/u/status/9");
    }
}
