use anyhow::{Context, Result};
use async_trait::async_trait;
use notifier_runtime::{
    DeliveryError, DestinationPlugin, PluginMetadata, RoutePluginInput, schema_value,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;
use teloxide::{ApiError, Bot, RequestError, prelude::Requester, types::ChatId};

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
    if input.chat_id.trim().is_empty() {
        anyhow::bail!("chat_id cannot be empty");
    }
    input
        .chat_id
        .parse::<i64>()
        .context("chat_id must be a signed integer")?;
    Ok(input)
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
        parse_spec(spec)?;
        for route in inputs {
            parse_input(&route.input).with_context(|| {
                format!("invalid destination input on route {:?}", route.route_id)
            })?;
        }
        Ok(())
    }

    async fn deliver(
        &self,
        spec: &Value,
        input: &Value,
        message: &str,
    ) -> Result<(), DeliveryError> {
        let spec = parse_spec(spec).map_err(|error| DeliveryError::permanent(error.to_string()))?;
        let input =
            parse_input(input).map_err(|error| DeliveryError::permanent(error.to_string()))?;
        if message.chars().count() > 4_096 {
            return Err(DeliveryError::permanent(
                "Telegram message exceeds 4,096 characters",
            ));
        }
        let chat_id = input
            .chat_id
            .parse::<i64>()
            .map(ChatId)
            .map_err(|error| DeliveryError::permanent(format!("invalid chat ID: {error}")))?;
        Bot::new(spec.bot_token)
            .send_message(chat_id, message)
            .await
            .map(|_| ())
            .map_err(classify_error)
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
