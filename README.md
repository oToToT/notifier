# Notifier

Notifier is a compile-time plugin-based Rust service for routing livestream webhooks to
text-message destinations. Configuration is loaded once at startup, accepted events are
stored in SQLite before webhook success is returned, and delivery happens asynchronously.

## Included plugins

Sources:

- `twitch`: Twitch EventSub `stream.online`
- `twitcasting`: TwitCasting `livestart`

Destinations:

- `discord`: channel messages with all mentions disabled
- `telegram`: Bot API `sendMessage`

Provider protocol bindings are supplied by `twitch_api`, `twitcasting`, `serenity`, and
`teloxide`; the plugin crates contain routing and policy logic rather than independent API
clients.

Sources and destinations are keyed reusable definitions. Routes reference those IDs and own
their message templates. Multiple routes can reuse either side; each referenced source is
reconciled once.

## Commands

```sh
cargo run -p notifier-server -- check-config --config config.json
cargo run -p notifier-server -- schema
cargo run -p notifier-server -- serve --config config.json
```

Start from [`config.example.json`](config.example.json). Configuration changes require a
restart. Route IDs must be unique and should remain stable across restarts.

The server exposes:

- One configured `POST` path per active Twitch or TwitCasting source
- `GET /health`
- `GET /ready`

Readiness is enabled only after all configured source subscriptions reconcile successfully.
Reconciliation creates missing subscriptions; it does not delete provider subscriptions that
are absent from the file. Twitch callback URLs are built from `public_base_url` and the
source's `webhook_path`. The TwitCasting application's callback URL must be configured
separately to the same full URL.

## Templates

Route `message` fields are MiniJinja templates. Interpolation, conditionals, loops,
and built-in filters are available. Missing event values render as empty strings. Syntax and
detectable unknown top-level variables are rejected at startup. Run the `schema` command for
the complete plugin schemas and template-variable documentation.

Twitch top-level variables are `event`, `broadcaster`, and `stream`. TwitCasting variables
are `event`, `broadcaster`, and `movie`.

Messages are rendered during webhook ingestion and the rendered text is stored, making
retries deterministic. V1 sends plain text only. Provider length limits are permanent
failures; content is never silently truncated.

## Delivery semantics

SQLite-backed workers retry network failures, HTTP 429, and HTTP 5xx with exponential
backoff and jitter capped at one hour. Other HTTP 4xx responses and exhausted retries are
retained as dead-letter rows. Processing rows are recovered after restart. Queued rows whose
route has been removed are moved to dead-letter state.

Delivery is at least once. If a destination accepts a request but its response is lost, a
retry can produce a duplicate. Webhook deduplication uses Twitch message IDs and a stable
TwitCasting key derived from the broadcaster and movie.

Credentials are literal values in reusable source and destination definitions. The service
never logs specs or credentials; protect the configuration file and SQLite database using
operating-system permissions.

## Development

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
