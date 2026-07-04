# notifier-server

`notifier-server` is the executable for Notifier. It links the bundled
source and destination plugins at compile time, validates a JSON configuration file,
reconciles external webhook subscriptions, exposes the webhook HTTP server, and runs
durable delivery workers.

This binary currently registers:

- Source plugins: `twitch`, `twitcasting`
- Destination plugins: `discord`, `telegram`

## Commands

Run commands from the repository root:

```sh
cargo run -p notifier-server -- check-config --config config.json
cargo run -p notifier-server -- schema
cargo run -p notifier-server -- serve --config config.json
```

`check-config` validates the file, plugin names, shared plugin specs, route-local plugin
inputs, and route templates without opening SQLite or contacting Twitch, TwitCasting,
Discord, or Telegram.

`schema` prints the combined runtime configuration schema, every registered plugin spec
schema, and each source plugin's template context documentation.

`serve` validates the configuration, opens SQLite, recovers interrupted deliveries,
reconciles active source subscriptions, starts delivery workers, and serves HTTP.

## Configuration

The default configuration path is `config.json`. Start from the repository-level
`config.example.json`.

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
        "webhook_secret": "..."
      }
    }
  },
  "dsts": {
    "discord-main": {
      "plugin": "discord",
      "spec": {
        "bot_token": "..."
      }
    }
  },
  "routes": [
    {
      "id": "twitch-example-to-discord",
      "src": {
        "id": "twitch-example",
        "input": {
          "broadcasters": ["example"]
        }
      },
      "dst": {
        "id": "discord-main",
        "input": {
          "channel_id": "1234567890"
        }
      },
      "message": "{{ broadcaster.name }} is live: {{ stream.title }}\n{{ stream.url }}"
    }
  ]
}
```

Source and destination definitions are reusable maps. Routes reference those IDs, provide
plugin-defined route-local inputs, and own their MiniJinja message templates. Route IDs must
be unique and should remain stable across restarts because they are part of delivery state.

## HTTP endpoints

When serving, the binary exposes:

- One configured `POST` webhook path for each active source definition.
- `GET /health`, which returns success when the HTTP server is alive.
- `GET /ready`, which returns success after storage recovery and source reconciliation.

Webhook paths are validated by `notifier-runtime`; they must be unique static absolute paths
and cannot be `/health` or `/ready`.

## Runtime behavior

Accepted events are rendered into route messages and stored in SQLite before webhook success
is returned. Delivery workers retry transient provider and network failures with exponential
backoff and jitter. Permanent failures and exhausted retries remain in SQLite as dead-letter
rows.

Delivery is at least once. If a destination accepts a request but the response is lost, a
retry can produce a duplicate destination message.

## Logging

Logging uses `tracing-subscriber` and defaults to `info`. Set `server.log_level` in
`config.json` to values such as `debug`, `info`, `warn`, or a full tracing filter directive.
`RUST_LOG` takes precedence when set:

```sh
RUST_LOG=notifier_runtime=debug,notifier_server=debug cargo run -p notifier-server -- serve
```

Credentials are read from the configuration file and plugin specs are not logged. Protect
the configuration file and SQLite database with operating-system permissions.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
