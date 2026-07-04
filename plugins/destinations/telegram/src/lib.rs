use anyhow::{Context, Result};
use async_trait::async_trait;
use notifier_runtime::{
    DeliveryError, DestinationPlugin, PluginMetadata, RoutePluginInput, schema_value,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;
use teloxide::{
    ApiError, Bot, RequestError,
    prelude::Requester,
    types::{ChatId, Recipient},
};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct TelegramDestination {}

impl TelegramDestination {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for TelegramDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Spec {
    bot_token: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Input {
    chat_id: String,
}

fn parse_spec(value: &Value) -> Result<Spec> {
    let spec: Spec = serde_json::from_value(value.clone()).context("invalid Telegram spec")?;
    if spec.bot_token.trim().is_empty() {
        anyhow::bail!("bot_token cannot be empty");
    }
    Ok(spec)
}

fn parse_input(value: &Value) -> Result<Input> {
    let input: Input = serde_json::from_value(value.clone()).context("invalid Telegram input")?;
    parse_chat_id(&input.chat_id)?;
    Ok(input)
}

fn parse_chat_id(chat_id: &str) -> Result<Recipient> {
    let chat_id = chat_id.trim();
    if chat_id.is_empty() {
        anyhow::bail!("chat_id cannot be empty");
    }

    if let Ok(id) = chat_id.parse::<i64>() {
        return Ok(Recipient::Id(ChatId(id)));
    }

    let username = chat_id.strip_prefix('@').with_context(
        || "chat_id must be a signed integer or a Telegram channel username like @channelusername",
    )?;
    if username.len() < 5
        || username.len() > 32
        || !username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        anyhow::bail!(
            "chat_id channel username must be 5-32 ASCII letters, digits, or underscores after @"
        );
    }

    Ok(Recipient::ChannelUsername(chat_id.to_owned()))
}

#[async_trait]
impl DestinationPlugin for TelegramDestination {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            name: "telegram",
            description: "Sends plain-text messages with the Telegram Bot API sendMessage method.",
            spec_schema: schema_value::<Spec>(),
            input_schema: schema_value::<Input>(),
        }
    }

    fn validate_spec(&self, spec: &Value, inputs: &[RoutePluginInput]) -> Result<()> {
        parse_spec(spec).inspect_err(|error| {
            debug!(
                route_count = inputs.len(),
                error = %error,
                "rejected Telegram destination configuration"
            );
        })?;
        for route in inputs {
            parse_input(&route.input)
                .with_context(|| format!("invalid destination input on route {:?}", route.route_id))
                .inspect_err(|error| {
                    debug!(
                        route_id = %route.route_id,
                        error = %error,
                        "rejected Telegram destination route input"
                    );
                })?;
        }
        debug!(
            route_count = inputs.len(),
            "validated Telegram destination configuration"
        );
        Ok(())
    }

    async fn deliver(
        &self,
        spec: &Value,
        input: &Value,
        message: &str,
    ) -> Result<(), DeliveryError> {
        let spec = parse_spec(spec).map_err(|error| {
            debug!(
                error = %error,
                "rejected Telegram delivery because destination spec is invalid"
            );
            DeliveryError::permanent(error.to_string())
        })?;
        let input = parse_input(input).map_err(|error| {
            debug!(
                error = %error,
                "rejected Telegram delivery because destination input is invalid"
            );
            DeliveryError::permanent(error.to_string())
        })?;
        if message.chars().count() > 4_096 {
            debug!(
                chat_id = %input.chat_id,
                message_chars = message.chars().count(),
                max_chars = 4_096,
                "rejected Telegram delivery because message is too long"
            );
            warn!(
                message_chars = message.chars().count(),
                "rejected Telegram delivery because message is too long"
            );
            return Err(DeliveryError::permanent(
                "Telegram message exceeds 4,096 characters",
            ));
        }
        let chat_id = parse_chat_id(&input.chat_id).map_err(|error| {
            debug!(
                chat_id = %input.chat_id,
                error = %error,
                "rejected Telegram delivery because chat ID could not be parsed"
            );
            DeliveryError::permanent(format!("invalid chat ID: {error}"))
        })?;
        debug!(
            chat_id = %input.chat_id,
            message_chars = message.chars().count(),
            "sending Telegram message"
        );
        Bot::new(spec.bot_token)
            .send_message(chat_id, message)
            .await
            .map(|_| {
                info!(chat_id = %input.chat_id, "Telegram message sent");
            })
            .map_err(|error| {
                debug!(
                    chat_id = %input.chat_id,
                    raw_error = ?error,
                    "Telegram provider request failed"
                );
                let classified = classify_error(error);
                debug!(
                    chat_id = %input.chat_id,
                    classified_error = classified.message(),
                    permanent = matches!(classified, DeliveryError::Permanent(_)),
                    "classified Telegram delivery failure"
                );
                warn!(
                    chat_id = %input.chat_id,
                    error = classified.message(),
                    "Telegram delivery failed"
                );
                classified
            })
    }
}

fn classify_error(error: RequestError) -> DeliveryError {
    match error {
        RequestError::Network(error) => DeliveryError::transient(error.to_string()),
        RequestError::RetryAfter(duration) => {
            DeliveryError::transient(format!("Telegram requested retry after {duration:?}"))
        }
        RequestError::Api(ApiError::Unknown(error)) if error.contains("Internal Server Error") => {
            DeliveryError::transient(error)
        }
        error => DeliveryError::permanent(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use teloxide::types::Seconds;

    #[test]
    fn classifies_retry_after_as_transient() {
        assert!(matches!(
            classify_error(RequestError::RetryAfter(Seconds::from_seconds(1))),
            DeliveryError::Transient(_)
        ));
    }

    #[test]
    fn validates_shared_token_and_route_input() {
        let plugin = TelegramDestination::new();
        plugin
            .validate_spec(
                &json!({"bot_token": "x"}),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: json!({"chat_id": "-1001"}),
                }],
            )
            .unwrap();

        plugin
            .validate_spec(
                &json!({"bot_token": "x"}),
                &[RoutePluginInput {
                    route_id: "route".into(),
                    input: json!({"chat_id": "@nanabunnonijyuuni_tweet"}),
                }],
            )
            .unwrap();

        assert!(
            plugin
                .validate_spec(
                    &json!({"bot_token": "x"}),
                    &[RoutePluginInput {
                        route_id: "route".into(),
                        input: json!({"chat_id": "@bad"}),
                    }],
                )
                .is_err()
        );

        assert!(
            plugin
                .validate_spec(
                    &json!({"bot_token": "x"}),
                    &[RoutePluginInput {
                        route_id: "route".into(),
                        input: json!({"chat_id": "not-an-int"}),
                    }],
                )
                .is_err()
        );
    }
}
