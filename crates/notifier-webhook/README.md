# notifier-webhook

Common building blocks for notifier source plugins that receive webhooks.

## Overview

`notifier-webhook` provides three layers that a plugin can opt into
independently:

1. Pure helpers for working with broadcaster-based route inputs and shared
   spec fields:
   - `BroadcasterInput`, `parse_broadcaster_input`, `validate_broadcasters`,
     `configured_broadcasters`, `matching_route_ids`.
   - `CommonSpec { webhook_path, client_id, client_secret }` and
     `validate_common_spec`.
2. Cryptographic utilities: `verify_hmac_sha256_parts` /
   `expected_hmac_sha256_parts` (Twitch EventSub HMAC) and
   `dedupe_sha256` (TwitCasting-style NUL-joined digest).
3. A unified dispatcher: implement the [`WebhookProvider`] trait, wrap it in
   [`WebhookSource<P>`], and the resulting `SourcePlugin` wires together the
   Axum router, signature verification, dispatch, and HTTP status codes.

## `WebhookProvider` contract

```text
async fn parse(spec, headers, body) -> Result<Event, WebhookError>
async fn dispatch(spec, event, context) -> Result<WebhookOutcome>
async fn reconcile(spec, context) -> Result<()>
fn metadata(), template_context_schema(), template_variables(),
fn webhook_path(spec), parse_spec(value), broadcaster_validator()
```

`parse` is responsible for verifying the inbound request (signature, shape,
headers) and returning either `WebhookError::Unauthorized` (HTTP 401),
`WebhookError::BadRequest` (HTTP 400), or a typed event. `dispatch` then
performs broadcaster matching, enrichment, ingestion via `context.sink`, and
provider-specific logging before returning one of:

- `WebhookOutcome::Challenge(String)` — HTTP 200 with the challenge body.
- `WebhookOutcome::Revoked` — HTTP 200 empty.
- `WebhookOutcome::Accepted` — HTTP 204.
- `WebhookOutcome::Ignored` — HTTP 204.

Anyhow errors from `dispatch` are surfaced by the dispatcher as HTTP 400
rejections.

## Example

```rust
use async_trait::async_trait;
use axum::{body::Bytes, http::HeaderMap};
use notifier_runtime::{PluginMetadata, SourceContext, schema_value};
use notifier_webhook::{
    BroadcasterValidator, CommonSpec, WebhookError, WebhookOutcome, WebhookProvider,
    WebhookSource, configured_broadcasters, validate_common_spec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct MySpec {
    #[serde(flatten)]
    common: CommonSpec,
    webhook_secret: String,
}

struct MyProvider;

#[async_trait::impl_webhook_provider] // pseudo: see WebhookProvider trait
impl WebhookProvider for MyProvider {
    type Spec = MySpec;
    type Event = ();
    fn metadata(&self) -> PluginMetadata { /* ... */ unimplemented!() }
    fn template_context_schema(&self) -> Value { unimplemented!() }
    fn template_variables(&self) -> Vec<String> { unimplemented!() }
    fn webhook_path(&self, spec: &Self::Spec) -> String { spec.common.webhook_path.clone() }
    fn parse_spec(&self, value: &Value) -> anyhow::Result<Self::Spec> {
        let spec: Self::Spec = serde_json::from_value(value.clone())?;
        validate_common_spec(&spec.common)?;
        Ok(spec)
    }
    async fn parse(&self, _: &Self::Spec, _: &HeaderMap, _: &Bytes)
        -> Result<Self::Event, WebhookError> { Ok(()) }
    async fn dispatch(&self, _: &Self::Spec, _: Self::Event, _: &SourceContext)
        -> anyhow::Result<WebhookOutcome> { Ok(WebhookOutcome::Ignored) }
    async fn reconcile(&self, _: &Self::Spec, _: &SourceContext) -> anyhow::Result<()> { Ok(()) }
}

pub type MySource = WebhookSource<MyProvider>;
```

## Behavior

The unified dispatcher establishes these conventions:

- Unmatched broadcaster events return `WebhookOutcome::Ignored` (HTTP 204),
  treating them as graceful no-ops.
- Signature verification failures (`WebhookError::Unauthorized`) return HTTP
  401. Other parse failures (`WebhookError::BadRequest`) return HTTP 400.
- `dispatch` performs ingestion via `context.sink.ingest(...)` itself and
  emits the provider-specific acceptance log line before returning
  `WebhookOutcome::Accepted`.