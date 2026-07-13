use anyhow::{Context, Result, bail};
use notifier_runtime::RoutePluginInput;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

/// Standard webhook source route input: a list of broadcaster identifiers.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BroadcasterInput {
    pub broadcasters: Vec<String>,
}

/// Customize per-plugin broadcaster validation when [`configured_broadcasters`]
/// and [`matching_route_ids`] parse route inputs. The default allows any
/// non-empty, case-insensitively-unique string.
#[derive(Clone, Copy, Debug)]
pub struct BroadcasterValidator {
    allowed_chars: Option<fn(char) -> bool>,
}

impl Default for BroadcasterValidator {
    fn default() -> Self {
        Self::none()
    }
}

impl BroadcasterValidator {
    /// No per-character restriction; only non-empty and case-insensitive
    /// uniqueness are enforced.
    pub const fn none() -> Self {
        Self {
            allowed_chars: None,
        }
    }

    /// Restrict broadcasters to ASCII alphanumeric characters and underscores
    /// (the Twitch EventSub login alphabet).
    pub const fn ascii_alphanumeric_or_underscore() -> Self {
        Self {
            allowed_chars: Some(is_ascii_alnum_or_underscore),
        }
    }

    fn check(&self, values: &[String]) -> Result<()> {
        if let Some(check) = self.allowed_chars {
            for value in values {
                if !value.chars().all(check) {
                    bail!("broadcaster {value:?} contains invalid characters");
                }
            }
        }
        Ok(())
    }
}

fn is_ascii_alnum_or_underscore(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Deserialize a route-local input into [`BroadcasterInput`].
pub fn parse_broadcaster_input(value: &Value) -> Result<BroadcasterInput> {
    serde_json::from_value::<BroadcasterInput>(value.clone())
        .context("invalid source input")
        .map_err(|error| anyhow::anyhow!(error))
}

/// Validate a per-route broadcaster list using the supplied validator.
pub fn validate_broadcasters(values: &[String], validator: &BroadcasterValidator) -> Result<()> {
    if values.is_empty() {
        bail!("at least one broadcaster must be configured");
    }
    for value in values {
        if value.trim().is_empty() {
            bail!("broadcasters cannot contain empty values");
        }
    }
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_ascii_lowercase()) {
            bail!("duplicate broadcaster {value:?}");
        }
    }
    validator.check(values)
}

/// Collect the de-duplicated, case-insensitive broadcaster union across routes.
pub fn configured_broadcasters(
    inputs: &[RoutePluginInput],
    validator: &BroadcasterValidator,
) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    for route in inputs {
        let input = parse_broadcaster_input(&route.input)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        validate_broadcasters(&input.broadcasters, validator)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        for broadcaster in input.broadcasters {
            if seen.insert(broadcaster.to_ascii_lowercase()) {
                values.push(broadcaster);
            }
        }
    }
    Ok(values)
}

/// Return the route IDs whose broadcaster list contains `broadcaster`
/// (case-insensitive). Invalid route inputs surface as errors so malformed
/// configuration is rejected at request time rather than silently ignored.
pub fn matching_route_ids(
    inputs: &[RoutePluginInput],
    broadcaster: &str,
    validator: &BroadcasterValidator,
) -> Result<Vec<String>> {
    let mut route_ids = Vec::new();
    for route in inputs {
        let input = parse_broadcaster_input(&route.input)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        validate_broadcasters(&input.broadcasters, validator)
            .with_context(|| format!("invalid source input on route {:?}", route.route_id))?;
        if input
            .broadcasters
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(broadcaster))
        {
            route_ids.push(route.route_id.clone());
        }
    }
    Ok(route_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(values: &[&[&str]]) -> Vec<RoutePluginInput> {
        values
            .iter()
            .enumerate()
            .map(|(idx, broadcasters)| RoutePluginInput {
                route_id: format!("route-{idx}"),
                input: serde_json::json!({ "broadcasters": broadcasters }),
            })
            .collect()
    }

    #[test]
    fn parses_broadcaster_input() {
        let input = parse_broadcaster_input(&serde_json::json!({
            "broadcasters": ["a", "B"]
        }))
        .unwrap();
        assert_eq!(input.broadcasters, ["a", "B"]);
    }

    #[test]
    fn rejects_empty_and_duplicate_and_whitespace_broadcasters() {
        let validator = BroadcasterValidator::none();
        let empty: [String; 0] = [];
        assert!(validate_broadcasters(&empty, &validator).is_err());
        assert!(validate_broadcasters(&["".into()], &validator).is_err());
        assert!(validate_broadcasters(&["  ".into()], &validator).is_err());
        assert!(validate_broadcasters(&["a".into(), "A".into()], &validator).is_err());
        assert!(validate_broadcasters(&["a".into(), "b".into()], &validator).is_ok());
    }

    #[test]
    fn ascii_alphanumeric_validator_rejects_other_characters() {
        let validator = BroadcasterValidator::ascii_alphanumeric_or_underscore();
        assert!(validate_broadcasters(&["a_b1".into()], &validator).is_ok());
        assert!(validate_broadcasters(&["a-b".into()], &validator).is_err());
        assert!(validate_broadcasters(&["日本".into()], &validator).is_err());
    }

    #[test]
    fn configured_broadcasters_unions_case_insensitively() {
        let inputs = inputs(&[&["Hanon", "kotoha"], &["KOTOHA", "other"]]);
        let union = configured_broadcasters(&inputs, &BroadcasterValidator::none()).unwrap();
        assert_eq!(union, ["Hanon", "kotoha", "other"]);
    }

    #[test]
    fn matching_route_ids_is_case_insensitive() {
        let inputs = inputs(&[&["hanon", "kotoha"], &["other"]]);
        let route_ids =
            matching_route_ids(&inputs, "KOTOHA", &BroadcasterValidator::none()).unwrap();
        assert_eq!(route_ids, ["route-0"]);
    }

    #[test]
    fn matching_route_ids_rejects_invalid_input() {
        let inputs = vec![RoutePluginInput {
            route_id: "r".into(),
            input: serde_json::json!({ "broadcasters": ["a", "a"] }),
        }];
        assert!(matching_route_ids(&inputs, "a", &BroadcasterValidator::none()).is_err());
    }
}
