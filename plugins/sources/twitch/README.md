# notifier-source-twitch

`notifier-source-twitch` is a Notifier source plugin for Twitch EventSub
`stream.online` webhooks. It uses `twitch_api` for Twitch API calls, EventSub subscription
management, and webhook HMAC verification.

The plugin name used in Notifier configuration is `twitch`.

## What it does

At startup, the plugin:

- Requests a Twitch app access token from `client_id` and `client_secret`.
- Resolves the configured broadcaster login to a broadcaster ID.
- Lists existing `stream.online` EventSub subscriptions.
- Creates a missing webhook subscription for `public_base_url + webhook_path`.

At webhook time, the plugin:

- Verifies Twitch EventSub headers and HMAC signature with `webhook_secret`.
- Responds to Twitch callback verification challenges.
- Acknowledges revocation messages.
- Ignores non-`stream.online` notifications and other broadcasters.
- Fetches current stream data for the notification.
- Enqueues one rendered delivery per matching route through `notifier-runtime`.

Existing Twitch subscriptions that are not represented in the Notifier configuration are not
deleted.

## Configuration

Use this plugin in the `srcs` map:

```json
{
  "srcs": {
    "twitch-example": {
      "plugin": "twitch",
      "spec": {
        "webhook_path": "/hooks/twitch-example",
        "client_id": "your-twitch-client-id",
        "client_secret": "your-twitch-client-secret",
        "webhook_secret": "a-shared-secret",
        "broadcaster": "example_login"
      }
    }
  }
}
```

Spec fields:

- `webhook_path`: static absolute HTTP path served by Notifier.
- `client_id`: Twitch application client ID.
- `client_secret`: Twitch application client secret.
- `webhook_secret`: EventSub webhook secret, 10 to 100 characters.
- `broadcaster`: Twitch broadcaster login; ASCII letters, numbers, and `_`.

The runtime validates the webhook path. It must be a non-root absolute path, have no
trailing slash, contain no query, fragment, captures, or wildcards, and not be `/health` or
`/ready`.

## Template variables

Routes using a Twitch source may reference these top-level MiniJinja variables:

- `event`
- `broadcaster`
- `stream`

Example route message:

```jinja
{{ broadcaster.name }} is live: {{ stream.title }}
{{ stream.url }}
```

Template context shape:

```json
{
  "event": {
    "id": "twitch-eventsub-message-id",
    "kind": "stream.online",
    "occurred_at": "event-or-message-timestamp"
  },
  "broadcaster": {
    "id": "123456",
    "login": "example_login",
    "name": "Example"
  },
  "stream": {
    "title": "Stream title",
    "url": "https://www.twitch.tv/example_login"
  }
}
```

## Deduplication

The Twitch EventSub message ID is used as the event deduplication key. SQLite enforces one
delivery row per `(source_plugin, dedupe_key, route_id)`, so Twitch redeliveries do not
create duplicate queued rows for the same route.

## Registration

Register the plugin with the runtime:

```rust
use notifier_runtime::RuntimeBuilder;
use notifier_source_twitch::TwitchSource;

let builder = RuntimeBuilder::new().source(TwitchSource::new());
```

The bundled `notifier-server` binary already registers this plugin.

## Development

```sh
cargo test -p notifier-source-twitch
cargo clippy -p notifier-source-twitch --all-targets -- -D warnings
```
