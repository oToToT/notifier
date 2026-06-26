# Notifier Design

## Goals

Notifier is a Rust Cargo workspace for routing livestream source events to text-message
destinations. Plugins are linked at compile time, configuration is loaded once at startup,
and accepted deliveries are persisted before webhook handlers return success.

V1 supports:

- Twitch `stream.online`
- TwitCasting `livestart`
- Discord channel text messages
- Telegram Bot API text messages
- SQLite persistence
- MiniJinja message templates

The administration UI and mutable subscription API from the original prototype are not part
of this design.

## Workspace

The workspace contains:

- `crates/notifier-runtime`: plugin traits, configuration, templates, HTTP integration, SQLite
  persistence, workers, retries, health checks, and schema generation.
- `plugins/sources/twitch`: Twitch EventSub handling using `twitch_api`.
- `plugins/sources/twitcasting`: TwitCasting API and webhook handling using `twitcasting`.
- `plugins/destinations/discord`: Discord delivery using `serenity`.
- `plugins/destinations/telegram`: Telegram delivery using `teloxide`.
- `apps/notifier-server`: executable that registers the four plugins and exposes the CLI.

Plugins are ordinary Rust libraries. Dynamic loading and a stable binary ABI are explicitly
out of scope.

## Runtime API

`RuntimeBuilder` registers source and destination plugins before configuration is loaded.
Plugin names must be unique within their category.

`SourcePlugin` provides:

- Plugin metadata and a JSON Schema for its reusable source specification.
- A schema and documented top-level variables for its template context.
- Startup validation, including the HTTP paths it exposes.
- An Axum webhook router for one source definition.
- Startup reconciliation of external provider subscriptions.

`DestinationPlugin` provides:

- Plugin metadata and a JSON Schema for its reusable destination specification.
- Startup validation.
- Asynchronous delivery with transient or permanent error classification.

`EventSink` is passed to source plugins. It renders destination templates and transactionally
persists one delivery per matching route.

The source abstraction is intended to support polling or WebSocket plugins in addition to
HTTP handlers without changing routing or persistence semantics.

## Configuration

Notifier reads one JSON file at startup:

```json
{
  "server": {
    "bind": "127.0.0.1:8080",
    "public_base_url": "https://notify.example.com"
  },
  "storage": {
    "sqlite_path": "notifier.db"
  },
  "delivery": {
    "workers": 4,
    "max_attempts": 8
  },
  "srcs": {
    "twitch-example": {
      "plugin": "twitch",
      "spec": {
        "webhook_path": "/hooks/twitch-example",
        "client_id": "...",
        "client_secret": "...",
        "webhook_secret": "...",
        "broadcaster": "example"
      }
    }
  },
  "dsts": {
    "discord-main": {
      "plugin": "discord",
      "spec": {
        "bot_token": "...",
        "channel_id": "123"
      }
    }
  },
  "routes": [
    {
      "id": "twitch-example-to-discord",
      "src": "twitch-example",
      "dst": "discord-main",
      "message": "{{ broadcaster.name }} is live: {{ stream.title }}\n{{ stream.url }}"
    }
  ]
}
```

`srcs` and `dsts` are keyed reusable definitions. Each route references one of each and owns
its template. A source ID can fan out to many routes and is reconciled once. A destination ID
can be reused with different route templates. All definitions are validated at startup;
unreferenced definitions remain inactive.

Route IDs must be unique and stable. Configuration changes require a restart. Credentials
remain literal values in definitions and are never included in application logs.

## CLI

`notifier-server` provides:

- `serve`: validate configuration, open SQLite, recover deliveries, reconcile source
  subscriptions, start workers, and serve HTTP.
- `check-config`: validate configuration, plugin lookup, specifications, and templates without
  contacting providers.
- `schema`: print the combined runtime and plugin JSON Schemas plus source template-variable
  documentation.

## Templates

Routes own the `message` field. Templates use a restricted MiniJinja environment
with interpolation, conditionals, loops, and built-in filters.

Startup validation rejects:

- Malformed template syntax.
- Detectable unknown top-level variables.

Missing event values render as empty strings. Messages are rendered during event ingestion
and the rendered text is persisted, so retries remain deterministic even if configuration
changes later.

V1 supports plain text only. Destination length limits are checked before sending and produce
permanent failures rather than truncation.

Twitch exposes:

- `event`: message ID, event kind, and occurrence timestamp.
- `broadcaster`: ID, login, and display name.
- `stream`: title and Twitch URL.

TwitCasting exposes:

- `event`: stable event ID and event kind.
- `broadcaster`: ID, screen ID, and display name.
- `movie`: ID, title, subtitle, latest owner comment, and URL.

## HTTP Endpoints

The runtime exposes:

- One configured `POST` webhook path for every active HTTP source ID
- `GET /health`
- `GET /ready`

Webhook paths must be unique static absolute paths. Queries, fragments, captures, wildcards,
trailing slashes, `/health`, and `/ready` are rejected before routers are constructed.

`/health` reports that the process and HTTP server are alive. `/ready` becomes successful
only after SQLite recovery and all source reconciliation work succeeds.

Provider reconciliation failures prevent the server from becoming ready and abort startup
with contextual errors.

## Twitch Source

The Twitch plugin uses `twitch_api` for:

- App access token acquisition.
- Broadcaster lookup.
- EventSub subscription listing and creation.
- Stream enrichment.
- EventSub HMAC verification.

At startup, each active source ID resolves the broadcaster ID and creates a missing
`stream.online` webhook subscription for `public_base_url + webhook_path`. Existing external
subscriptions not represented in configuration are not deleted.

Webhook handling:

1. Read Twitch message headers and the raw body.
2. Verify the HMAC using that source definition's secret.
3. Return the raw challenge for callback verification.
4. Acknowledge revocations.
5. Match the notification broadcaster to that source definition.
6. Query current stream data for title and URL.
7. Persist rendered deliveries using the Twitch message ID as the deduplication key.
8. Return a successful response only after persistence completes.

Twitch may redeliver events, so the message ID is treated as opaque and unique.

## TwitCasting Source

The TwitCasting plugin uses `twitcasting` for:

- Broadcaster lookup.
- Webhook listing and registration.
- Typed webhook decoding.

At startup, each active source ID resolves the broadcaster ID and registers a missing
`livestart` hook. The application-level callback URL must already be configured to the full
URL corresponding to that source's `webhook_path`. Unconfigured external hooks are not
removed.

TwitCasting provides an opaque signature in the webhook body rather than a documented HMAC
algorithm. The plugin compares it with the configured application signature and matches the
broadcaster screen ID.

The deduplication key is a SHA-256 digest derived from the event type, broadcaster ID, and
movie ID.

## Destinations

### Discord

The Discord plugin uses `serenity` to send a channel message using a bot token and channel
ID. Allowed mentions are empty by default, preventing user, role, and everyone mentions from
being expanded.

Messages over 2,000 Unicode code points are permanent failures.

### Telegram

The Telegram plugin uses `teloxide` to call `sendMessage` with a bot token and numeric chat
ID. It does not receive commands or updates.

Messages over 4,096 Unicode code points are permanent failures.

## Persistence

SQLite stores one delivery row for each source event and route pair. The uniqueness constraint
is:

```text
(source_plugin, dedupe_key, route_id)
```

Stored fields include:

- Source plugin and deduplication key.
- Route ID.
- Rendered message.
- State.
- Attempt count.
- Next available time.
- Last error.
- Creation time.

The principal states are:

- `queued`
- `processing`
- `delivered`
- `dead`

Ingestion renders all matching route messages before starting a transaction. The transaction
inserts every non-duplicate delivery, then commits before the source webhook returns success.

On restart:

- `processing` rows return to `queued`.
- Queued rows referencing missing routes move to `dead`.
- Delivered and dead-letter records are retained.

## Delivery and Retries

Configured workers claim queued deliveries and invoke the destination plugin associated with
the persisted route ID.

Transient failures include:

- Network and transport failures.
- Provider rate limiting.
- HTTP 5xx-equivalent provider failures.

Permanent failures include:

- Invalid destination configuration encountered during delivery.
- Provider message-length rejection.
- Other non-retryable provider API errors.
- Routes or destination plugins unavailable after restart.

Retries use exponential backoff with jitter and are capped at one hour. The default maximum is
eight attempts. Permanent failures and exhausted retries are retained as dead-letter rows.

The system guarantees at-least-once delivery. If a destination accepts a request but its
response is lost, retrying can produce a duplicate.

## Logging and Security

Complete source or destination specifications and credentials must never be logged. Errors identify plugins,
routes, attempts, and provider classifications without including tokens or secrets.

Configuration files and SQLite databases contain sensitive material and must be protected
with operating-system permissions.

## Testing and Quality Gates

Coverage includes:

- Keyed definition, reference, plugin, specification, route ID, and template validation.
- Required, malformed, reserved, and duplicate webhook path validation.
- Plugin lookup and schema generation.
- Template syntax, unknown variables, missing values, and loop-local variables.
- SQLite deduplication, claiming, recovery, and dead-letter transitions.
- Twitch HMAC verification and configurable callback routing.
- TwitCasting configurable callback routing and source-scoped matching.
- Discord and Telegram error classification.
- CLI example configuration validation and schema output.

Required quality gates:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Non-Goals

V1 does not include:

- An administration UI.
- Runtime configuration mutation.
- Manual subscribe or unsubscribe endpoints.
- Dynamic plugin loading.
- A persistence backend other than SQLite.
- Rich destination messages, embeds, media, or attachments.
- Inbound Telegram commands.
- Events other than livestream start notifications.
