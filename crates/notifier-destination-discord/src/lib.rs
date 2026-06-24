use anyhow::{Context, Result};
use async_trait::async_trait;
use notifier_runtime::{DeliveryError, DestinationPlugin, PluginMetadata, schema_value};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;
use serenity::{
    Error as SerenityError,
    builder::{CreateAllowedMentions, CreateMessage},
    http::{Http, HttpError},
    model::id::ChannelId,
};

#[derive(Clone)]
pub struct DiscordDestination {}

impl DiscordDestination {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for DiscordDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Spec {
    bot_token: String,
    channel_id: String,
    message: String,
}

fn parse_spec(value: &Value) -> Result<Spec> {
    let spec: Spec = serde_json::from_value(value.clone()).context("invalid Discord spec")?;
    if spec.bot_token.trim().is_empty() {
        anyhow::bail!("bot_token cannot be empty");
    }
    if spec.channel_id.trim().is_empty() {
        anyhow::bail!("channel_id cannot be empty");
    }
    spec.channel_id
        .parse::<u64>()
        .context("channel_id must be an unsigned integer")?;
    if spec.message.trim().is_empty() {
        anyhow::bail!("message cannot be empty");
    }
    Ok(spec)
}

#[async_trait]
impl DestinationPlugin for DiscordDestination {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "discord",
            description: "Sends plain-text Discord channel messages with mentions disabled.",
            spec_schema: schema_value::<Spec>(),
        }
    }

    fn validate_spec(&self, spec: &Value) -> Result<()> {
        parse_spec(spec).map(|_| ())
    }

    fn message_template<'a>(&self, spec: &'a Value) -> Result<&'a str> {
        spec.get("message")
            .and_then(Value::as_str)
            .context("message must be a string")
    }

    async fn deliver(&self, spec: &Value, message: &str) -> Result<(), DeliveryError> {
        let spec = parse_spec(spec).map_err(|error| DeliveryError::permanent(error.to_string()))?;
        if message.chars().count() > 2_000 {
            return Err(DeliveryError::permanent(
                "Discord message exceeds 2,000 characters",
            ));
        }
        let channel_id =
            ChannelId::new(spec.channel_id.parse().map_err(|error| {
                DeliveryError::permanent(format!("invalid channel ID: {error}"))
            })?);
        let http = Http::new(&spec.bot_token);
        channel_id
            .send_message(
                &http,
                CreateMessage::new()
                    .content(message)
                    .allowed_mentions(CreateAllowedMentions::new()),
            )
            .await
            .map(|_| ())
            .map_err(classify_error)
    }
}

fn classify_status(status: u16) -> DeliveryError {
    match status {
        429 | 500..=599 => DeliveryError::transient(format!("Discord returned HTTP {status}")),
        _ => DeliveryError::permanent(format!("Discord returned HTTP {status}")),
    }
}

fn classify_error(error: SerenityError) -> DeliveryError {
    match error {
        SerenityError::Http(HttpError::UnsuccessfulRequest(response)) => {
            classify_status(response.status_code.as_u16())
        }
        SerenityError::Http(HttpError::Request(error)) => {
            DeliveryError::transient(error.to_string())
        }
        error => DeliveryError::permanent(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_provider_errors() {
        assert!(matches!(classify_status(429), DeliveryError::Transient(_)));
        assert!(matches!(classify_status(503), DeliveryError::Transient(_)));
        assert!(matches!(classify_status(400), DeliveryError::Permanent(_)));
    }

    #[test]
    fn schema_and_message_are_available() {
        let plugin = DiscordDestination::new();
        assert_eq!(plugin.metadata().name, "discord");
        assert_eq!(
            plugin
                .message_template(&json!({
                    "bot_token": "x",
                    "channel_id": "1",
                    "message": "hello"
                }))
                .unwrap(),
            "hello"
        );
    }
}
