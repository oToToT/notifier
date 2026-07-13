use anyhow::{Result, bail};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration fields shared by webhook source plugins. Provider spec
/// structs typically embed this via `#[serde(flatten)]` and add their own
/// provider-specific fields.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CommonSpec {
    pub webhook_path: String,
    pub client_id: String,
    pub client_secret: String,
}

/// Validate the shared spec fields. Provider-specific spec validation remains
/// the responsibility of the provider's `parse_spec` implementation.
pub fn validate_common_spec(spec: &CommonSpec) -> Result<()> {
    for (name, value) in [
        ("client_id", &spec.client_id),
        ("client_secret", &spec.client_secret),
    ] {
        if value.trim().is_empty() {
            bail!("{name} cannot be empty");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> CommonSpec {
        CommonSpec {
            webhook_path: "/hooks/x".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
        }
    }

    #[test]
    fn accepts_populated_shared_spec() {
        assert!(validate_common_spec(&valid()).is_ok());
    }

    #[test]
    fn rejects_empty_client_id_or_secret() {
        let mut spec = valid();
        spec.client_id = "  ".into();
        assert!(validate_common_spec(&spec).is_err());
        let mut spec = valid();
        spec.client_secret = "".into();
        assert!(validate_common_spec(&spec).is_err());
    }
}
