# notifier-destination-telegram

`notifier-destination-telegram` is a Notifier destination plugin that sends plain-text
messages with the Telegram Bot API `sendMessage` method using `teloxide`.

The plugin name used in Notifier configuration is `telegram`.

## What it does

For each delivery, the plugin:

- Validates the configured bot token and chat ID shape.
- Rejects messages over Telegram's 4,096-character limit as permanent failures.
- Calls `sendMessage` for the configured chat.
- Classifies network failures, retry-after responses, and Telegram internal server errors
  as transient failures for retry.

The plugin only sends outbound messages. It does not receive updates, process commands, or
run a Telegram bot dispatcher.

## Configuration

Use this plugin in the `dsts` map, then provide route-local chat inputs from routes:

```json
{
  "dsts": {
    "telegram-main": {
      "plugin": "telegram",
      "spec": {
        "bot_token": "your-telegram-bot-token"
      }
    }
  },
  "routes": [
    {
      "id": "source-to-telegram",
      "src": {
        "id": "source-id",
        "input": {}
      },
      "dst": {
        "id": "telegram-main",
        "input": {
          "chat_id": "@channelusername"
        }
      },
      "message": "message text"
    }
  ]
}
```

Spec fields:

- `bot_token`: Telegram bot token.

Route input fields:

- `chat_id`: target Telegram chat ID as a signed integer string, or a public channel
  username such as `@channelusername`.

Both fields are required in their respective locations and must be non-empty. Numeric
`chat_id` values must parse as `i64`. Channel usernames must include the leading `@` and
use 5-32 ASCII letters, digits, or underscores after it.

## Failure handling

The plugin returns `DeliveryError::Transient` for:

- network failures
- Telegram `RetryAfter`
- Telegram API errors containing `Internal Server Error`

The plugin returns `DeliveryError::Permanent` for:

- invalid plugin specs or route inputs
- messages over 4,096 Unicode scalar values
- other Telegram API errors

`notifier-runtime` retries transient failures with exponential backoff and moves permanent
failures to dead-letter state.

## Registration

Register the plugin with the runtime:

```rust
use notifier_destination_telegram::TelegramDestination;
use notifier_runtime::RuntimeBuilder;

let builder = RuntimeBuilder::new().destination(TelegramDestination::new());
```

The bundled `notifier-server` binary already registers this plugin.

## Development

```sh
cargo test --manifest-path plugins/destinations/telegram/Cargo.toml
cargo clippy --manifest-path plugins/destinations/telegram/Cargo.toml --all-targets -- -D warnings
```
