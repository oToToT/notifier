# notifier-destination-discord

`notifier-destination-discord` is a Notifier destination plugin that sends plain-text
messages to a Discord channel using `serenity`.

The plugin name used in Notifier configuration is `discord`.

## What it does

For each delivery, the plugin:

- Validates the configured bot token and channel ID shape.
- Rejects messages over Discord's 2,000-character limit as permanent failures.
- Sends the message to the configured channel.
- Disables all allowed mentions so users, roles, and `@everyone` are not expanded.
- Classifies rate limits and server errors as transient failures for retry.

The plugin only sends channel messages. It does not manage guilds, channels, slash commands,
interactions, or inbound Discord events.

## Configuration

Use this plugin in the `dsts` map, then provide route-local channel inputs from routes:

```json
{
  "dsts": {
    "discord-main": {
      "plugin": "discord",
      "spec": {
        "bot_token": "your-discord-bot-token"
      }
    }
  },
  "routes": [
    {
      "id": "source-to-discord",
      "src": {
        "id": "source-id",
        "input": {}
      },
      "dst": {
        "id": "discord-main",
        "input": {
          "channel_id": "123456789012345678"
        }
      },
      "message": "message text"
    }
  ]
}
```

Spec fields:

- `bot_token`: Discord bot token used by `serenity::http::Http`.

Route input fields:

- `channel_id`: target Discord channel ID as an unsigned integer string.

Both fields are required in their respective locations and must be non-empty. `channel_id`
must parse as `u64`.

## Failure handling

The plugin returns `DeliveryError::Transient` for:

- HTTP `429`
- HTTP `5xx`
- lower-level HTTP request failures

The plugin returns `DeliveryError::Permanent` for:

- invalid plugin specs or route inputs
- messages over 2,000 Unicode scalar values
- other Discord HTTP `4xx` responses
- unexpected Serenity errors

`notifier-runtime` retries transient failures with exponential backoff and moves permanent
failures to dead-letter state.

## Registration

Register the plugin with the runtime:

```rust
use notifier_destination_discord::DiscordDestination;
use notifier_runtime::RuntimeBuilder;

let builder = RuntimeBuilder::new().destination(DiscordDestination::new());
```

The bundled `notifier-server` binary already registers this plugin.

## Development

```sh
cargo test --manifest-path plugins/destinations/discord/Cargo.toml
cargo clippy --manifest-path plugins/destinations/discord/Cargo.toml --all-targets -- -D warnings
```
