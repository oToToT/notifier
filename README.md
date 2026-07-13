# Notifier

Notifier is a compile-time plugin-based Rust service for routing events from various
sources to text-message destinations. Configuration is loaded once at startup, accepted
events are stored in SQLite before the source handler reports success, and delivery
happens asynchronously.

## Included plugins

Sources:

- `nitter`: Nitter RSS polling for new tweets
- `twitch`: Twitch EventSub `stream.online`
- `twitcasting`: TwitCasting `livestart`

Destinations:

- `discord`: channel messages with all mentions disabled
- `telegram`: Bot API `sendMessage`

Provider protocol bindings are supplied by `twitch_api`, `twitcasting`, `serenity`, and
`teloxide`; the plugin crates contain routing and policy logic rather than independent API
clients.

Sources and destinations are keyed reusable plugin instances. Routes reference those IDs and
provide plugin-defined route-local inputs, such as broadcaster lists or destination channel
IDs, alongside their message templates. Multiple routes can reuse either side; each
referenced source is reconciled once.

The runtime is generic: sources can be webhook-based, polling-based, or long-running
background tasks, and destinations can be any message channel. New plugins are added at
compile time by implementing `SourcePlugin` or `DestinationPlugin` and registering them in
the `RuntimeBuilder`.

For webhook sources, `crates/notifier-webhook` offers optional helpers such as a unified
Axum dispatcher, HMAC verification, SHA-256 deduplication utilities, and common spec
fragments. It is not required; you may build a webhook source from scratch or use your own
libraries. We welcome contributions of additional generic helper crates, new plugins, and
any other improvements.

## Commands

```sh
cargo run -p notifier-server -- check-config --config config.json
cargo run -p notifier-server -- schema
cargo run -p notifier-server -- serve --config config.json
```

Start from [`config.example.json`](config.example.json). Configuration changes require a
restart. Route IDs must be unique and should remain stable across restarts.

The server exposes:

- One configured `POST` path per active webhook-based source
- `GET /health`
- `GET /ready`

Readiness is enabled only after all configured source subscriptions reconcile successfully.
Reconciliation creates missing subscriptions; it does not delete provider subscriptions that
are absent from the file. Twitch callback URLs are built from `public_base_url` and the
source's `webhook_path`. The TwitCasting application's callback URL must be configured
separately to the same full URL. Polling sources such as Nitter do not expose webhook
paths; they run background tasks that are started after reconciliation and do not block
readiness on fetch availability.

## Templates

Route `message` fields are MiniJinja templates. Interpolation, conditionals, loops,
and built-in filters are available. Missing event values render as empty strings. Syntax and
detectable unknown top-level variables are rejected at startup. Run the `schema` command for
the complete plugin schemas and template-variable documentation.

Nitter top-level variables are `event`, `user`, and `tweet`. Twitch top-level variables are
`event`, `broadcaster`, and `stream`. TwitCasting variables are `event`, `broadcaster`, and
`movie`.

Nitter source specs use `instance_url` for fetching RSS from `{instance_url}/{user}/rss`.
Optional `tweet_url_base` rewrites notification links while leaving fetches on
`instance_url`; useful values include `https://fxtwitter.com`, `https://vxtwitter.com`, and
`https://x.com`. The default `first_fetch` mode is `mark_seen`; setting
`first_fetch` to `notify_existing` may send every item currently present in the RSS feed.

Messages are rendered during event ingestion and the rendered text is stored, making
retries deterministic. V1 sends plain text only. Provider length limits are permanent
failures; content is never silently truncated.

## Delivery semantics

SQLite-backed workers retry network failures, HTTP 429, and HTTP 5xx with exponential
backoff and jitter capped at one hour. Other HTTP 4xx responses and exhausted retries are
retained as dead-letter rows. Processing rows are recovered after restart. Queued rows whose
route has been removed are moved to dead-letter state.

Delivery is at least once. If a destination accepts a request but its response is lost, a
retry can produce a duplicate. Webhook deduplication uses Twitch message IDs, a stable
TwitCasting key derived from the broadcaster and movie, and Nitter RSS item GUIDs falling
back to item links.

Credentials are literal values in reusable source and destination definitions. The service
never logs specs or credentials; protect the configuration file and SQLite database using
operating-system permissions.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
for manifest in $(find crates plugins -name Cargo.toml | sort); do cargo fmt --manifest-path "$manifest" -- --check; done
for manifest in $(find crates plugins -name Cargo.toml | sort); do cargo clippy --manifest-path "$manifest" --all-targets -- -D warnings; done
for manifest in $(find crates plugins -name Cargo.toml | sort); do cargo test --manifest-path "$manifest"; done
```
