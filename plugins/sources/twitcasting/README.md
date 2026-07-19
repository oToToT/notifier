# notifier-source-twitcasting

`notifier-source-twitcasting` is a Notifier source plugin for TwitCasting `livestart`
webhooks. It uses the `twitcasting` crate for API access, webhook listing and registration,
and typed webhook decoding.

The plugin name used in Notifier configuration is `twitcasting`.

## What it does

At startup, the plugin:

- Resolves each unique broadcaster screen ID from active route inputs.
- Lists TwitCasting webhooks for the application.
- Registers a missing `livestart` webhook for each broadcaster.

At webhook time, the plugin:

- Decodes the TwitCasting webhook payload.
- Accepts only `livestart` events.
- Compares the payload broadcaster with route inputs.
- Verifies the payload signature against the configured `webhook_signature`.
  Mismatches are rejected when `enforce_signature_verification` is enabled;
  otherwise they are logged and the webhook is processed.
- Ensures the movie belongs to that broadcaster.
- Enqueues one rendered delivery per matching route through `notifier-runtime`.

TwitCasting stores the callback URL at the application level. Configure the TwitCasting
application callback URL separately to the same full URL as `public_base_url + webhook_path`.
Notifier registers the event hook but does not set that callback URL.

Existing TwitCasting hooks that are not represented in the Notifier configuration are not
deleted.

## Configuration

Use this plugin in the `srcs` map, then provide route-local broadcaster inputs from routes:

```json
{
  "srcs": {
    "twitcasting-example": {
      "plugin": "twitcasting",
      "spec": {
        "webhook_path": "/hooks/twitcasting-example",
        "client_id": "your-twitcasting-client-id",
        "client_secret": "your-twitcasting-client-secret",
        "webhook_signature": "your-twitcasting-webhook-signature",
        "enforce_signature_verification": true
      }
    }
  },
  "routes": [
    {
      "id": "twitcasting-example-to-destination",
      "src": {
        "id": "twitcasting-example",
        "input": {
          "broadcasters": ["example_screen_id", "another_screen_id"]
        }
      },
      "dst": {
        "id": "destination-id",
        "input": {}
      },
      "message": "{{ broadcaster.name }} started"
    }
  ]
}
```

Spec fields:

- `webhook_path`: static absolute HTTP path served by Notifier.
- `client_id`: TwitCasting application client ID.
- `client_secret`: TwitCasting application client secret.
- `webhook_signature`: required non-empty string value from the TwitCasting developer
  dashboard (under "WebHook Signature").
- `enforce_signature_verification`: optional boolean, default `true`. A signature
  mismatch is logged as a warning, with the full request body at debug level, and
  is accepted when `false`. When `true`, a mismatch is logged as an error and
  rejected with HTTP 401.
- `api_base_url`: optional API base URL, default `https://apiv2.twitcasting.tv`.

Route input fields:

- `broadcasters`: one or more TwitCasting broadcaster screen IDs. Duplicate names in one
  route input are rejected case-insensitively.

The runtime validates the webhook path. It must be a non-root absolute path, have no
trailing slash, contain no query, fragment, captures, or wildcards, and not be `/health` or
`/ready`.

## Template variables

Routes using a TwitCasting source may reference these top-level MiniJinja variables:

- `event`
- `broadcaster`
- `movie`

Example route message:

```jinja
{{ broadcaster.name }} started a TwitCasting live: {{ movie.title }}
{{ movie.url }}
```

Template context shape:

```json
{
  "event": {
    "id": "stable-dedupe-key",
    "kind": "livestart"
  },
  "broadcaster": {
    "id": "123456",
    "screen_id": "example_screen_id",
    "name": "Example"
  },
  "movie": {
    "id": "987654",
    "title": "Live title",
    "subtitle": "Live subtitle",
    "comment": "latest owner comment",
    "url": "https://twitcasting.tv/example_screen_id/movie/987654"
  }
}
```

`movie.subtitle` and `movie.comment` render as empty strings when TwitCasting does not
provide those values.

## Deduplication

TwitCasting webhooks do not provide the same opaque event ID shape used by Twitch. This
plugin derives a SHA-256 deduplication key from:

```text
livestart\0{broadcaster_id}\0{movie_id}
```

SQLite enforces one delivery row per `(source_plugin, dedupe_key, route_id)`, so repeated
webhooks do not create duplicate queued rows for the same route.

## Registration

Register the plugin with the runtime:

```rust
use notifier_runtime::RuntimeBuilder;
use notifier_source_twitcasting::TwitCastingSource;

let builder = RuntimeBuilder::new().source(TwitCastingSource::new());
```

The bundled `notifier-server` binary already registers this plugin.

## Development

```sh
cargo test --manifest-path plugins/sources/twitcasting/Cargo.toml
cargo clippy --manifest-path plugins/sources/twitcasting/Cargo.toml --all-targets -- -D warnings
```
