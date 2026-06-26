# notifier-runtime

`notifier-runtime` is the shared runtime crate for Notifier. It defines the plugin traits,
configuration model, template validation, Axum integration, SQLite delivery queue, retry
workers, health endpoints, and JSON Schema generation used by the workspace binaries and
plugins.

The crate is designed for compile-time plugin registration. Plugins are ordinary Rust
libraries implementing runtime traits; dynamic loading and a stable binary plugin ABI are
outside the crate's scope.

## Main API

Register plugins with `RuntimeBuilder`:

```rust
use notifier_runtime::RuntimeBuilder;

let builder = RuntimeBuilder::new()
    .source(my_source_plugin)
    .destination(my_destination_plugin);
```

The builder validates a `Config` with registered plugin implementations:

```rust
let config = notifier_runtime::Config::load("config.json")?;
let runtime = builder.check_config(config)?;
runtime.serve().await?;
```

`RuntimeBuilder::schema()` returns a JSON value containing the base configuration schema,
source plugin metadata and template schemas, and destination plugin metadata.

## Source plugins

Source plugins implement `SourcePlugin`:

```rust
#[async_trait::async_trait]
pub trait SourcePlugin: Send + Sync {
    fn metadata(&self) -> PluginMetadata;
    fn template_context_schema(&self) -> serde_json::Value;
    fn template_variables(&self) -> Vec<String>;
    fn validate_spec(
        &self,
        spec: &serde_json::Value,
        inputs: &[RoutePluginInput],
    ) -> anyhow::Result<ValidatedSource>;
    fn router(&self, context: SourceContext) -> axum::Router;
    async fn reconcile(&self, context: &SourceContext) -> anyhow::Result<()>;
}
```

`PluginMetadata` includes both `spec_schema` for the reusable source instance and
`input_schema` for each route's source input. `validate_spec` receives the shared spec plus
all active route inputs for that source instance. It returns the top-level template
variables allowed for routes using that source and any HTTP paths the source wants to expose.
The runtime validates those paths, ensures active paths are unique, and rejects reserved
paths.

`router` receives a `SourceContext` containing the source ID, public base URL, raw plugin
spec, route inputs keyed by route ID, and an `EventSink`. Source webhooks select matching
route IDs from those inputs and call `EventSink::ingest` after they authenticate and
normalize an event.

`reconcile` runs during startup before readiness is enabled. Webhook sources use it to
create missing external subscriptions.

## Destination plugins

Destination plugins implement `DestinationPlugin`:

```rust
#[async_trait::async_trait]
pub trait DestinationPlugin: Send + Sync {
    fn metadata(&self) -> PluginMetadata;
    fn validate_spec(
        &self,
        spec: &serde_json::Value,
        inputs: &[RoutePluginInput],
    ) -> anyhow::Result<()>;
    async fn deliver(
        &self,
        spec: &serde_json::Value,
        input: &serde_json::Value,
        message: &str,
    )
        -> Result<(), DeliveryError>;
}
```

`PluginMetadata` includes both `spec_schema` for the reusable destination instance and
`input_schema` for each route's destination input. Destination implementations classify
failures as `DeliveryError::Transient` or `DeliveryError::Permanent`. Transient failures are
retried until `delivery.max_attempts` is reached. Permanent failures are moved directly to
dead-letter state.

## Configuration model

`Config` is loaded from JSON and contains:

- `server.bind`: socket address used by Axum.
- `server.public_base_url`: HTTP or HTTPS base URL used by source reconciliation.
- `storage.sqlite_path`: SQLite database path.
- `delivery.workers`: number of delivery workers, default `4`.
- `delivery.max_attempts`: retry attempt limit, default `8`.
- `srcs`: reusable source plugin instances.
- `dsts`: reusable destination plugin instances.
- `routes`: source-to-destination routes with `src.id`, `src.input`, `dst.id`, `dst.input`,
  and MiniJinja message templates.

Base validation checks route references, unique route IDs, non-empty IDs, non-empty
messages, valid delivery settings, and public base URL scheme. Plugin validation is handled
by `RuntimeBuilder::check_config`.

## Templates

Route messages use MiniJinja. The runtime validates syntax and detectable unknown
top-level variables at startup:

```rust
notifier_runtime::validate_template("{{ stream.title }}", &["stream".to_owned()])?;
```

Rendering is lenient for missing values, so absent nested event fields render as empty
strings:

```rust
let message = notifier_runtime::render_template("{{ stream.title }}", &context)?;
```

Messages are rendered during event ingestion and the rendered text is persisted. Retries
therefore remain deterministic even if configuration changes later.

## Storage and delivery

`Storage` wraps SQLite and creates a `deliveries` table with a uniqueness constraint on:

```text
(source_plugin, dedupe_key, route_id)
```

The queue states are `queued`, `processing`, `delivered`, and `dead`. On startup, the
runtime requeues interrupted `processing` rows and moves queued rows for removed routes to
dead-letter state.

Delivery workers claim queued rows, call the destination plugin, complete successful rows,
retry transient failures with exponential backoff and jitter capped at one hour, and retain
permanent failures as dead-letter rows.

## HTTP integration

`Runtime::serve` builds one Axum application containing active source routers plus:

- `GET /health`
- `GET /ready`

Readiness is set only after storage recovery and source reconciliation succeed.

## Development

```sh
cargo test -p notifier-runtime
cargo clippy -p notifier-runtime --all-targets -- -D warnings
```
