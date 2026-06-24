mod config;
mod storage;
mod template;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{Json, Router, http::StatusCode, routing::get};
use rand::Rng;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

pub use config::{Config, DeliveryConfig, PluginConfig, RouteConfig, ServerConfig, StorageConfig};
pub use storage::{Delivery, Storage};
pub use template::{render_template, validate_template};

#[derive(Clone, Debug, Serialize)]
pub struct PluginMetadata {
    pub name: &'static str,
    pub description: &'static str,
    pub spec_schema: Value,
}

#[derive(Clone, Debug)]
pub struct ValidatedSource {
    pub watch_key: String,
    pub allowed_template_variables: Vec<String>,
}

#[derive(Clone)]
pub struct SourceRoute {
    pub route_id: String,
    pub spec: Value,
    pub watch_key: String,
}

#[derive(Clone)]
pub struct SourceContext {
    pub public_base_url: url::Url,
    pub routes: Vec<SourceRoute>,
    pub sink: EventSink,
}

#[derive(Clone, Debug)]
pub enum DeliveryError {
    Transient(String),
    Permanent(String),
}

impl DeliveryError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self::Transient(message.into())
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent(message.into())
    }

    fn message(&self) -> &str {
        match self {
            Self::Transient(message) | Self::Permanent(message) => message,
        }
    }
}

#[async_trait]
pub trait SourcePlugin: Send + Sync {
    fn metadata(&self) -> PluginMetadata;
    fn template_context_schema(&self) -> Value;
    fn template_variables(&self) -> Vec<String>;
    fn validate_spec(&self, spec: &Value) -> Result<ValidatedSource>;
    fn router(&self, context: SourceContext) -> Router;
    async fn reconcile(&self, context: &SourceContext) -> Result<()>;
}

#[async_trait]
pub trait DestinationPlugin: Send + Sync {
    fn metadata(&self) -> PluginMetadata;
    fn validate_spec(&self, spec: &Value) -> Result<()>;
    fn message_template<'a>(&self, spec: &'a Value) -> Result<&'a str>;
    async fn deliver(&self, spec: &Value, message: &str) -> Result<(), DeliveryError>;
}

#[derive(Clone)]
struct PreparedRoute {
    source_plugin: String,
    source_spec: Value,
    source_watch_key: String,
    destination_plugin: String,
    destination_spec: Value,
    message_template: String,
}

#[derive(Clone)]
pub struct EventSink {
    storage: Storage,
    routes: Arc<HashMap<String, PreparedRoute>>,
}

impl EventSink {
    pub fn ingest(
        &self,
        source_plugin: &str,
        route_ids: &[String],
        dedupe_key: &str,
        context: &Value,
    ) -> Result<usize> {
        let mut deliveries = Vec::with_capacity(route_ids.len());
        for route_id in route_ids {
            let route = self
                .routes
                .get(route_id)
                .with_context(|| format!("source emitted unknown route {route_id:?}"))?;
            if route.source_plugin != source_plugin {
                bail!("source plugin mismatch for route {route_id:?}");
            }
            let message = render_template(&route.message_template, context)
                .with_context(|| format!("failed to render route {route_id:?}"))?;
            deliveries.push((route_id.clone(), message));
        }
        self.storage
            .enqueue_batch(source_plugin, dedupe_key, &deliveries)
    }
}

pub struct RuntimeBuilder {
    sources: HashMap<String, Arc<dyn SourcePlugin>>,
    destinations: HashMap<String, Arc<dyn DestinationPlugin>>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            destinations: HashMap::new(),
        }
    }

    pub fn source(mut self, plugin: impl SourcePlugin + 'static) -> Self {
        let name = plugin.metadata().name.to_owned();
        assert!(
            self.sources
                .insert(name.clone(), Arc::new(plugin))
                .is_none(),
            "duplicate source plugin {name}"
        );
        self
    }

    pub fn destination(mut self, plugin: impl DestinationPlugin + 'static) -> Self {
        let name = plugin.metadata().name.to_owned();
        assert!(
            self.destinations
                .insert(name.clone(), Arc::new(plugin))
                .is_none(),
            "duplicate destination plugin {name}"
        );
        self
    }

    pub fn check_config(&self, config: Config) -> Result<Runtime> {
        config.validate_base()?;
        let mut routes = HashMap::new();
        for route in &config.routes {
            let source = self
                .sources
                .get(&route.src.plugin)
                .with_context(|| format!("unknown source plugin {:?}", route.src.plugin))?;
            let destination = self
                .destinations
                .get(&route.dst.plugin)
                .with_context(|| format!("unknown destination plugin {:?}", route.dst.plugin))?;
            let validated = source
                .validate_spec(&route.src.spec)
                .with_context(|| format!("invalid source on route {:?}", route.id))?;
            destination
                .validate_spec(&route.dst.spec)
                .with_context(|| format!("invalid destination on route {:?}", route.id))?;
            let message = destination
                .message_template(&route.dst.spec)
                .with_context(|| format!("missing message template on route {:?}", route.id))?;
            validate_template(message, &validated.allowed_template_variables)
                .with_context(|| format!("invalid message template on route {:?}", route.id))?;
            routes.insert(
                route.id.clone(),
                PreparedRoute {
                    source_plugin: route.src.plugin.clone(),
                    source_spec: route.src.spec.clone(),
                    source_watch_key: validated.watch_key,
                    destination_plugin: route.dst.plugin.clone(),
                    destination_spec: route.dst.spec.clone(),
                    message_template: message.to_owned(),
                },
            );
        }
        Ok(Runtime {
            config,
            sources: self.sources.clone(),
            destinations: self.destinations.clone(),
            routes: Arc::new(routes),
            ready: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn schema(&self) -> Value {
        let sources: Vec<_> = self
            .sources
            .values()
            .map(|plugin| {
                json!({
                    "metadata": plugin.metadata(),
                    "template_context_schema": plugin.template_context_schema(),
                    "template_variables": plugin.template_variables(),
                })
            })
            .collect();
        let destinations: Vec<_> = self
            .destinations
            .values()
            .map(|plugin| plugin.metadata())
            .collect();
        json!({
            "configuration_schema": schemars::schema_for!(Config),
            "source_plugins": sources,
            "destination_plugins": destinations,
        })
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Runtime {
    config: Config,
    sources: HashMap<String, Arc<dyn SourcePlugin>>,
    destinations: HashMap<String, Arc<dyn DestinationPlugin>>,
    routes: Arc<HashMap<String, PreparedRoute>>,
    ready: Arc<AtomicBool>,
}

impl Runtime {
    pub async fn serve(self) -> Result<()> {
        let storage = Storage::open(&self.config.storage.sqlite_path)?;
        storage.recover(&self.routes.keys().cloned().collect::<HashSet<_>>())?;
        let sink = EventSink {
            storage: storage.clone(),
            routes: self.routes.clone(),
        };

        let mut app = Router::new();
        for (name, plugin) in &self.sources {
            let routes = self
                .routes
                .iter()
                .filter(|(_, route)| route.source_plugin == *name)
                .map(|(route_id, route)| SourceRoute {
                    route_id: route_id.clone(),
                    spec: route.source_spec.clone(),
                    watch_key: route.source_watch_key.clone(),
                })
                .collect();
            let context = SourceContext {
                public_base_url: self.config.server.public_base_url.clone(),
                routes,
                sink: sink.clone(),
            };
            plugin
                .reconcile(&context)
                .await
                .with_context(|| format!("source plugin {name:?} reconciliation failed"))?;
            app = app.merge(plugin.router(context));
        }

        self.ready.store(true, Ordering::Release);
        let ready = self.ready.clone();
        app = app
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/ready",
                get(move || {
                    let ready = ready.clone();
                    async move {
                        if ready.load(Ordering::Acquire) {
                            (StatusCode::OK, Json(json!({"ready": true})))
                        } else {
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(json!({"ready": false})),
                            )
                        }
                    }
                }),
            );

        let mut workers = JoinSet::new();
        for worker_id in 0..self.config.delivery.workers {
            workers.spawn(delivery_worker(
                worker_id,
                storage.clone(),
                self.routes.clone(),
                self.destinations.clone(),
                self.config.delivery.max_attempts,
            ));
        }

        let listener = tokio::net::TcpListener::bind(self.config.server.bind).await?;
        info!(bind = %self.config.server.bind, "notifier is listening");
        let result = axum::serve(listener, app)
            .await
            .context("HTTP server failed");
        workers.abort_all();
        result
    }
}

async fn delivery_worker(
    worker_id: usize,
    storage: Storage,
    routes: Arc<HashMap<String, PreparedRoute>>,
    destinations: HashMap<String, Arc<dyn DestinationPlugin>>,
    max_attempts: u32,
) {
    loop {
        let delivery = match storage.claim() {
            Ok(Some(delivery)) => delivery,
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(error) => {
                error!(worker_id, %error, "failed to claim delivery");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let Some(route) = routes.get(&delivery.route_id) else {
            let _ = storage.dead_letter(delivery.id, delivery.attempts, "route no longer exists");
            continue;
        };
        let Some(plugin) = destinations.get(&route.destination_plugin) else {
            let _ = storage.dead_letter(
                delivery.id,
                delivery.attempts,
                "destination plugin is unavailable",
            );
            continue;
        };

        match plugin
            .deliver(&route.destination_spec, &delivery.message)
            .await
        {
            Ok(()) => {
                if let Err(error) = storage.complete(delivery.id) {
                    error!(worker_id, %error, "failed to complete delivery");
                }
            }
            Err(error) => {
                let attempts = delivery.attempts + 1;
                let permanent = matches!(error, DeliveryError::Permanent(_));
                if permanent || attempts >= max_attempts {
                    warn!(
                        worker_id,
                        route_id = %delivery.route_id,
                        attempts,
                        error = error.message(),
                        "delivery moved to dead letter"
                    );
                    let _ = storage.dead_letter(delivery.id, attempts, error.message());
                } else {
                    let delay = retry_delay(attempts);
                    warn!(
                        worker_id,
                        route_id = %delivery.route_id,
                        attempts,
                        delay,
                        error = error.message(),
                        "delivery will be retried"
                    );
                    let _ = storage.retry(delivery.id, attempts, delay, error.message());
                }
            }
        }
    }
}

fn retry_delay(attempt: u32) -> u64 {
    let base = 2_u64.saturating_pow(attempt.min(11)).min(3_600);
    let jitter = rand::rng().random_range(0..=base / 4);
    (base + jitter).min(3_600)
}

pub fn schema_value<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("JSON Schema must serialize")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TestSource;

    #[async_trait]
    impl SourcePlugin for TestSource {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata {
                name: "test-source",
                description: "test",
                spec_schema: json!({}),
            }
        }

        fn template_context_schema(&self) -> Value {
            json!({"type": "object"})
        }

        fn template_variables(&self) -> Vec<String> {
            vec!["event".into()]
        }

        fn validate_spec(&self, _spec: &Value) -> Result<ValidatedSource> {
            Ok(ValidatedSource {
                watch_key: "watch".into(),
                allowed_template_variables: self.template_variables(),
            })
        }

        fn router(&self, _context: SourceContext) -> Router {
            Router::new()
        }

        async fn reconcile(&self, _context: &SourceContext) -> Result<()> {
            Ok(())
        }
    }

    struct TestDestination;

    #[async_trait]
    impl DestinationPlugin for TestDestination {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata {
                name: "test-destination",
                description: "test",
                spec_schema: json!({}),
            }
        }

        fn validate_spec(&self, _spec: &Value) -> Result<()> {
            Ok(())
        }

        fn message_template<'a>(&self, spec: &'a Value) -> Result<&'a str> {
            spec["message"].as_str().context("message")
        }

        async fn deliver(&self, _spec: &Value, _message: &str) -> Result<(), DeliveryError> {
            Ok(())
        }
    }

    fn test_config(source: &str) -> Config {
        Config {
            server: ServerConfig {
                bind: "127.0.0.1:8080".parse().unwrap(),
                public_base_url: "https://example.test".parse().unwrap(),
            },
            storage: StorageConfig {
                sqlite_path: ":memory:".into(),
            },
            delivery: DeliveryConfig::default(),
            routes: vec![RouteConfig {
                id: "route".into(),
                src: PluginConfig {
                    plugin: source.into(),
                    spec: json!({}),
                },
                dst: PluginConfig {
                    plugin: "test-destination".into(),
                    spec: json!({"message": "{{ event.id }}"}),
                },
            }],
        }
    }

    #[test]
    fn retry_is_capped_at_one_hour() {
        for attempt in 0..32 {
            assert!(retry_delay(attempt) <= 3_600);
        }
    }

    #[test]
    fn validates_plugin_lookup_and_generates_schema() {
        let builder = RuntimeBuilder::new()
            .source(TestSource)
            .destination(TestDestination);
        builder.check_config(test_config("test-source")).unwrap();
        assert!(builder.schema()["configuration_schema"].is_object());

        let error = builder
            .check_config(test_config("missing"))
            .err()
            .expect("unknown plugin must fail");
        assert!(error.to_string().contains("unknown source plugin"));
    }
}
