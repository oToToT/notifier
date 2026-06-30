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
use tracing::{debug, error, info, warn};

pub use config::{
    Config, DeliveryConfig, PluginConfig, RouteConfig, RouteEndpointConfig, ServerConfig,
    StorageConfig,
};
pub use storage::{Delivery, Storage};
pub use template::{render_template, validate_template};

#[derive(Clone, Debug, Serialize)]
pub struct PluginMetadata {
    pub name: &'static str,
    pub description: &'static str,
    pub spec_schema: Value,
    pub input_schema: Value,
}

#[derive(Clone, Debug)]
pub struct ValidatedSource {
    pub allowed_template_variables: Vec<String>,
    pub http_paths: Vec<String>,
}

#[derive(Clone)]
pub struct SourceContext {
    pub source_id: String,
    pub public_base_url: url::Url,
    pub spec: Value,
    pub route_inputs: Vec<RoutePluginInput>,
    pub storage: Storage,
    pub sink: EventSink,
}

#[derive(Clone, Debug)]
pub struct RoutePluginInput {
    pub route_id: String,
    pub input: Value,
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

    pub fn message(&self) -> &str {
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
    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<ValidatedSource>;
    fn router(&self, context: SourceContext) -> Router;
    async fn reconcile(&self, context: &SourceContext) -> Result<()>;
    async fn run(&self, _context: SourceContext) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait DestinationPlugin: Send + Sync {
    fn metadata(&self) -> PluginMetadata;
    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<()>;
    async fn deliver(
        &self,
        spec: &Value,
        input: &Value,
        message: &str,
    ) -> Result<(), DeliveryError>;
}

#[derive(Clone)]
struct PreparedRoute {
    source_id: String,
    source_plugin: String,
    destination_id: String,
    destination_input: Value,
    message_template: String,
}

#[derive(Clone)]
struct PreparedSource {
    plugin: String,
    spec: Value,
    route_inputs: Vec<RoutePluginInput>,
}

#[derive(Clone)]
struct PreparedDestination {
    plugin: String,
    spec: Value,
}

#[derive(Clone)]
pub struct EventSink {
    storage: Storage,
    routes: Arc<HashMap<String, PreparedRoute>>,
}

impl EventSink {
    pub fn ingest(
        &self,
        source_id: &str,
        route_ids: &[String],
        dedupe_key: &str,
        context: &Value,
    ) -> Result<usize> {
        let mut deliveries = Vec::with_capacity(route_ids.len());
        let mut source_plugin = None;
        for route_id in route_ids {
            let route = self
                .routes
                .get(route_id)
                .with_context(|| format!("source emitted unknown route {route_id:?}"))?;
            if route.source_id != source_id {
                bail!("source ID mismatch for route {route_id:?}");
            }
            source_plugin = Some(route.source_plugin.as_str());
            let message = render_template(&route.message_template, context)
                .with_context(|| format!("failed to render route {route_id:?}"))?;
            deliveries.push((route_id.clone(), message));
        }
        let source_plugin = source_plugin.context("source has no routes")?;
        let queued = self
            .storage
            .enqueue_batch(source_plugin, dedupe_key, &deliveries)?;
        info!(
            source_id,
            source_plugin,
            route_count = route_ids.len(),
            queued,
            dedupe_key,
            "event ingested"
        );
        Ok(queued)
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
        debug!(
            sources = config.srcs.len(),
            destinations = config.dsts.len(),
            routes = config.routes.len(),
            "checking configuration"
        );
        config.validate_base()?;
        let mut prepared_sources = HashMap::new();
        for (id, definition) in &config.srcs {
            self.sources
                .get(&definition.plugin)
                .with_context(|| format!("unknown source plugin {:?}", definition.plugin))?;
            prepared_sources.insert(
                id.clone(),
                PreparedSource {
                    plugin: definition.plugin.clone(),
                    spec: definition.spec.clone(),
                    route_inputs: Vec::new(),
                },
            );
        }
        let mut prepared_destinations = HashMap::new();
        for (id, definition) in &config.dsts {
            self.destinations
                .get(&definition.plugin)
                .with_context(|| format!("unknown destination plugin {:?}", definition.plugin))?;
            prepared_destinations.insert(
                id.clone(),
                (
                    PreparedDestination {
                        plugin: definition.plugin.clone(),
                        spec: definition.spec.clone(),
                    },
                    Vec::<RoutePluginInput>::new(),
                ),
            );
        }

        let mut routes = HashMap::new();
        for route in &config.routes {
            let source = prepared_sources
                .get_mut(&route.src.id)
                .expect("base validation checked source references");
            source.route_inputs.push(RoutePluginInput {
                route_id: route.id.clone(),
                input: route.src.input.clone(),
            });
            let (_, destination_inputs) = prepared_destinations
                .get_mut(&route.dst.id)
                .expect("base validation checked destination references");
            destination_inputs.push(RoutePluginInput {
                route_id: route.id.clone(),
                input: route.dst.input.clone(),
            });
            routes.insert(
                route.id.clone(),
                PreparedRoute {
                    source_id: route.src.id.clone(),
                    source_plugin: source.plugin.clone(),
                    destination_id: route.dst.id.clone(),
                    destination_input: route.dst.input.clone(),
                    message_template: route.message.clone(),
                },
            );
        }

        let mut validated_sources = HashMap::new();
        for (id, source) in &prepared_sources {
            let plugin = self
                .sources
                .get(&source.plugin)
                .expect("prepared source plugin must be registered");
            let validated = plugin
                .validate_spec(&source.spec, &source.route_inputs)
                .with_context(|| format!("invalid source definition {id:?}"))?;
            debug!(
                source_id = %id,
                plugin = %source.plugin,
                route_count = source.route_inputs.len(),
                http_path_count = validated.http_paths.len(),
                "source configuration validated"
            );
            for path in &validated.http_paths {
                validate_http_path(path)
                    .with_context(|| format!("invalid webhook path on source {id:?}"))?;
            }
            validated_sources.insert(id.clone(), validated);
        }

        for (id, (destination, inputs)) in &prepared_destinations {
            let plugin = self
                .destinations
                .get(&destination.plugin)
                .expect("prepared destination plugin must be registered");
            plugin
                .validate_spec(&destination.spec, inputs)
                .with_context(|| format!("invalid destination definition {id:?}"))?;
            debug!(
                destination_id = %id,
                plugin = %destination.plugin,
                route_count = inputs.len(),
                "destination configuration validated"
            );
        }

        for route in &config.routes {
            let validated = validated_sources
                .get(&route.src.id)
                .expect("source validation must exist");
            validate_template(&route.message, &validated.allowed_template_variables)
                .with_context(|| format!("invalid message template on route {:?}", route.id))?;
        }

        let mut active_paths = HashMap::new();
        for (source_id, source) in &prepared_sources {
            if source.route_inputs.is_empty() {
                continue;
            }
            let validated = validated_sources
                .get(source_id)
                .expect("source validation must exist");
            for path in &validated.http_paths {
                if let Some(existing) = active_paths.insert(path.clone(), source_id.clone()) {
                    bail!(
                        "duplicate webhook path {path:?} on sources {existing:?} and {source_id:?}"
                    );
                }
            }
        }
        Ok(Runtime {
            config,
            sources: self.sources.clone(),
            destinations: self.destinations.clone(),
            prepared_sources: prepared_sources
                .into_iter()
                .filter_map(|(id, source)| {
                    (!source.route_inputs.is_empty()).then_some((id, source))
                })
                .collect(),
            prepared_destinations: prepared_destinations
                .into_iter()
                .map(|(id, (destination, _))| (id, destination))
                .collect(),
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
    prepared_sources: HashMap<String, PreparedSource>,
    prepared_destinations: HashMap<String, PreparedDestination>,
    routes: Arc<HashMap<String, PreparedRoute>>,
    ready: Arc<AtomicBool>,
}

impl Runtime {
    pub async fn serve(self) -> Result<()> {
        info!(
            bind = %self.config.server.bind,
            public_base_url = %self.config.server.public_base_url,
            source_count = self.prepared_sources.len(),
            destination_count = self.prepared_destinations.len(),
            route_count = self.routes.len(),
            "runtime starting"
        );
        let storage = Storage::open(&self.config.storage.sqlite_path)?;
        info!(
            sqlite_path = %self.config.storage.sqlite_path,
            "storage opened"
        );
        storage.recover(&self.routes.keys().cloned().collect::<HashSet<_>>())?;
        info!("storage recovery completed");
        let sink = EventSink {
            storage: storage.clone(),
            routes: self.routes.clone(),
        };

        let mut app = Router::new();
        for (source_id, source) in &self.prepared_sources {
            let plugin = self
                .sources
                .get(&source.plugin)
                .expect("prepared source plugin must be registered");
            let context = SourceContext {
                source_id: source_id.clone(),
                public_base_url: self.config.server.public_base_url.clone(),
                spec: source.spec.clone(),
                route_inputs: source.route_inputs.clone(),
                storage: storage.clone(),
                sink: sink.clone(),
            };
            info!(
                source_id,
                plugin = %source.plugin,
                route_count = source.route_inputs.len(),
                "reconciling source"
            );
            plugin
                .reconcile(&context)
                .await
                .with_context(|| format!("source {source_id:?} reconciliation failed"))?;
            info!(source_id, plugin = %source.plugin, "source reconciled");
            app = app.merge(plugin.router(context.clone()));
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
            debug!(worker_id, "starting delivery worker");
            workers.spawn(delivery_worker(
                worker_id,
                storage.clone(),
                self.routes.clone(),
                self.destinations.clone(),
                self.prepared_destinations.clone(),
                self.config.delivery.max_attempts,
            ));
        }
        for (source_id, source) in &self.prepared_sources {
            let plugin = self
                .sources
                .get(&source.plugin)
                .expect("prepared source plugin must be registered")
                .clone();
            let source_id = source_id.clone();
            let context = SourceContext {
                source_id: source_id.clone(),
                public_base_url: self.config.server.public_base_url.clone(),
                spec: source.spec.clone(),
                route_inputs: source.route_inputs.clone(),
                storage: storage.clone(),
                sink: sink.clone(),
            };
            debug!(
                source_id,
                plugin = %source.plugin,
                "starting source background task"
            );
            workers.spawn(async move {
                if let Err(error) = plugin.run(context).await {
                    error!(source_id, %error, "source background task exited");
                }
            });
        }

        let listener = tokio::net::TcpListener::bind(self.config.server.bind).await?;
        info!(bind = %self.config.server.bind, "notifier is listening");
        let result = axum::serve(listener, app)
            .await
            .context("HTTP server failed");
        warn!("HTTP server stopped; aborting background workers");
        workers.abort_all();
        result
    }
}

async fn delivery_worker(
    worker_id: usize,
    storage: Storage,
    routes: Arc<HashMap<String, PreparedRoute>>,
    destinations: HashMap<String, Arc<dyn DestinationPlugin>>,
    prepared_destinations: HashMap<String, PreparedDestination>,
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
        debug!(
            worker_id,
            delivery_id = delivery.id,
            route_id = %delivery.route_id,
            attempts = delivery.attempts,
            "claimed delivery"
        );
        let Some(route) = routes.get(&delivery.route_id) else {
            warn!(
                worker_id,
                delivery_id = delivery.id,
                route_id = %delivery.route_id,
                "delivery route no longer exists"
            );
            let _ = storage.dead_letter(delivery.id, delivery.attempts, "route no longer exists");
            continue;
        };
        let Some(destination) = prepared_destinations.get(&route.destination_id) else {
            warn!(
                worker_id,
                delivery_id = delivery.id,
                route_id = %delivery.route_id,
                destination_id = %route.destination_id,
                "delivery destination no longer exists"
            );
            let _ = storage.dead_letter(
                delivery.id,
                delivery.attempts,
                "destination no longer exists",
            );
            continue;
        };
        let Some(plugin) = destinations.get(&destination.plugin) else {
            warn!(
                worker_id,
                delivery_id = delivery.id,
                route_id = %delivery.route_id,
                destination_id = %route.destination_id,
                plugin = %destination.plugin,
                "delivery destination plugin is unavailable"
            );
            let _ = storage.dead_letter(
                delivery.id,
                delivery.attempts,
                "destination plugin is unavailable",
            );
            continue;
        };

        match plugin
            .deliver(
                &destination.spec,
                &route.destination_input,
                &delivery.message,
            )
            .await
        {
            Ok(()) => {
                if let Err(error) = storage.complete(delivery.id) {
                    error!(worker_id, %error, "failed to complete delivery");
                } else {
                    info!(
                        worker_id,
                        delivery_id = delivery.id,
                        route_id = %delivery.route_id,
                        destination_id = %route.destination_id,
                        plugin = %destination.plugin,
                        "delivery completed"
                    );
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

fn validate_http_path(path: &str) -> Result<()> {
    if !path.starts_with('/') || path == "/" {
        bail!("path must be an absolute non-root path beginning with '/'");
    }
    path.parse::<axum::http::uri::PathAndQuery>()
        .context("path is not a valid HTTP path")?;
    if path.ends_with('/') {
        bail!("path must not have a trailing slash");
    }
    if path.contains(['?', '#', '{', '}', '*', ':']) {
        bail!("path must be static and contain no query, fragment, capture, or wildcard");
    }
    if matches!(path, "/health" | "/ready") {
        bail!("path {path:?} is reserved");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestSource;

    #[async_trait]
    impl SourcePlugin for TestSource {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata {
                name: "test-source",
                description: "test",
                spec_schema: json!({}),
                input_schema: json!({}),
            }
        }

        fn template_context_schema(&self) -> Value {
            json!({"type": "object"})
        }

        fn template_variables(&self) -> Vec<String> {
            vec!["event".into()]
        }

        fn validate_spec(
            &self,
            _spec: &Value,
            _inputs: &[RoutePluginInput],
        ) -> Result<ValidatedSource> {
            Ok(ValidatedSource {
                allowed_template_variables: self.template_variables(),
                http_paths: vec!["/hooks/test".into()],
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
                input_schema: json!({}),
            }
        }

        fn validate_spec(&self, _spec: &Value, _inputs: &[RoutePluginInput]) -> Result<()> {
            Ok(())
        }

        async fn deliver(
            &self,
            _spec: &Value,
            _input: &Value,
            _message: &str,
        ) -> Result<(), DeliveryError> {
            Ok(())
        }
    }

    fn test_config(source: &str) -> Config {
        Config {
            server: ServerConfig {
                bind: "127.0.0.1:8080".parse().unwrap(),
                public_base_url: "https://example.test".parse().unwrap(),
                log_level: None,
            },
            storage: StorageConfig {
                sqlite_path: ":memory:".into(),
            },
            delivery: DeliveryConfig::default(),
            srcs: HashMap::from([(
                "source".into(),
                PluginConfig {
                    plugin: source.into(),
                    spec: json!({}),
                },
            )]),
            dsts: HashMap::from([(
                "destination".into(),
                PluginConfig {
                    plugin: "test-destination".into(),
                    spec: json!({}),
                },
            )]),
            routes: vec![RouteConfig {
                id: "route".into(),
                src: RouteEndpointConfig {
                    id: "source".into(),
                    input: json!({}),
                },
                dst: RouteEndpointConfig {
                    id: "destination".into(),
                    input: json!({}),
                },
                message: "{{ event.id }}".into(),
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

    #[test]
    fn rejects_invalid_reserved_and_duplicate_active_webhook_paths() {
        struct PathSource(&'static str);

        #[async_trait]
        impl SourcePlugin for PathSource {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata {
                    name: self.0,
                    description: "test",
                    spec_schema: json!({}),
                    input_schema: json!({}),
                }
            }

            fn template_context_schema(&self) -> Value {
                json!({})
            }

            fn template_variables(&self) -> Vec<String> {
                vec!["event".into()]
            }

            fn validate_spec(
                &self,
                spec: &Value,
                _inputs: &[RoutePluginInput],
            ) -> Result<ValidatedSource> {
                Ok(ValidatedSource {
                    allowed_template_variables: self.template_variables(),
                    http_paths: vec![spec["path"].as_str().context("path")?.into()],
                })
            }

            fn router(&self, _context: SourceContext) -> Router {
                Router::new()
            }

            async fn reconcile(&self, _context: &SourceContext) -> Result<()> {
                Ok(())
            }
        }

        let builder = RuntimeBuilder::new()
            .source(PathSource("one"))
            .source(PathSource("two"))
            .destination(TestDestination);
        let mut config = test_config("one");
        config.srcs.get_mut("source").unwrap().spec = json!({"path": "/health"});
        let error = builder
            .check_config(config)
            .err()
            .expect("reserved path must fail");
        assert!(format!("{error:#}").contains("reserved"));

        let mut config = test_config("one");
        config.srcs.get_mut("source").unwrap().spec = json!({"path": "/hooks/shared"});
        config.srcs.insert(
            "other".into(),
            PluginConfig {
                plugin: "two".into(),
                spec: json!({"path": "/hooks/shared"}),
            },
        );
        config.routes.push(RouteConfig {
            id: "other-route".into(),
            src: RouteEndpointConfig {
                id: "other".into(),
                input: json!({}),
            },
            dst: RouteEndpointConfig {
                id: "destination".into(),
                input: json!({}),
            },
            message: "{{ event.id }}".into(),
        });
        assert!(
            builder
                .check_config(config)
                .err()
                .expect("duplicate path must fail")
                .to_string()
                .contains("duplicate webhook path")
        );
    }

    #[test]
    fn validates_unreferenced_definitions_but_only_activates_referenced_sources() {
        let builder = RuntimeBuilder::new()
            .source(TestSource)
            .destination(TestDestination);
        let mut config = test_config("test-source");
        config.srcs.insert(
            "unused".into(),
            PluginConfig {
                plugin: "missing".into(),
                spec: json!({}),
            },
        );
        assert!(
            builder
                .check_config(config)
                .err()
                .expect("unknown unreferenced plugin must fail")
                .to_string()
                .contains("unknown source plugin")
        );
    }

    #[test]
    fn validates_reused_definitions_once_and_supports_route_local_templates() {
        static SOURCE_VALIDATIONS: AtomicUsize = AtomicUsize::new(0);
        static DESTINATION_VALIDATIONS: AtomicUsize = AtomicUsize::new(0);

        struct CountingSource;

        #[async_trait]
        impl SourcePlugin for CountingSource {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata {
                    name: "counting-source",
                    description: "test",
                    spec_schema: json!({}),
                    input_schema: json!({}),
                }
            }

            fn template_context_schema(&self) -> Value {
                json!({})
            }

            fn template_variables(&self) -> Vec<String> {
                vec!["event".into()]
            }

            fn validate_spec(
                &self,
                _spec: &Value,
                _inputs: &[RoutePluginInput],
            ) -> Result<ValidatedSource> {
                SOURCE_VALIDATIONS.fetch_add(1, Ordering::Relaxed);
                Ok(ValidatedSource {
                    allowed_template_variables: self.template_variables(),
                    http_paths: vec!["/hooks/counting".into()],
                })
            }

            fn router(&self, _context: SourceContext) -> Router {
                Router::new()
            }

            async fn reconcile(&self, _context: &SourceContext) -> Result<()> {
                Ok(())
            }
        }

        struct CountingDestination;

        #[async_trait]
        impl DestinationPlugin for CountingDestination {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata {
                    name: "counting-destination",
                    description: "test",
                    spec_schema: json!({}),
                    input_schema: json!({}),
                }
            }

            fn validate_spec(&self, _spec: &Value, _inputs: &[RoutePluginInput]) -> Result<()> {
                DESTINATION_VALIDATIONS.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }

            async fn deliver(
                &self,
                _spec: &Value,
                _input: &Value,
                _message: &str,
            ) -> Result<(), DeliveryError> {
                Ok(())
            }
        }

        SOURCE_VALIDATIONS.store(0, Ordering::Relaxed);
        DESTINATION_VALIDATIONS.store(0, Ordering::Relaxed);
        let mut config = test_config("counting-source");
        config.dsts.get_mut("destination").unwrap().plugin = "counting-destination".into();
        config.routes.push(RouteConfig {
            id: "second".into(),
            src: RouteEndpointConfig {
                id: "source".into(),
                input: json!({"route": "second"}),
            },
            dst: RouteEndpointConfig {
                id: "destination".into(),
                input: json!({"route": "second"}),
            },
            message: "second: {{ event.id }}".into(),
        });
        let runtime = RuntimeBuilder::new()
            .source(CountingSource)
            .destination(CountingDestination)
            .check_config(config)
            .unwrap();

        assert_eq!(SOURCE_VALIDATIONS.load(Ordering::Relaxed), 1);
        assert_eq!(DESTINATION_VALIDATIONS.load(Ordering::Relaxed), 1);
        assert_eq!(runtime.prepared_sources["source"].route_inputs.len(), 2);
        assert_ne!(
            runtime.routes["route"].message_template,
            runtime.routes["second"].message_template
        );
    }

    #[test]
    fn rejects_malformed_http_paths() {
        for path in [
            "",
            "relative",
            "/",
            "/trailing/",
            "/query?x=1",
            "/fragment#x",
            "/capture/{id}",
            "/wildcard/*rest",
            "/invalid path",
        ] {
            assert!(validate_http_path(path).is_err(), "{path:?} must fail");
        }
        validate_http_path("/hooks/static-path").unwrap();
    }
}
