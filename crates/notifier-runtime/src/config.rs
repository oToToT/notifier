use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    path::Path,
};

use anyhow::{Context, Result, bail};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    pub srcs: HashMap<String, PluginConfig>,
    pub dsts: HashMap<String, PluginConfig>,
    pub routes: Vec<RouteConfig>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub public_base_url: Url,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub sqlite_path: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeliveryConfig {
    pub workers: usize,
    pub max_attempts: u32,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            max_attempts: 8,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub id: String,
    pub src: RouteEndpointConfig,
    pub dst: RouteEndpointConfig,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteEndpointConfig {
    pub id: String,
    pub input: Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub plugin: String,
    pub spec: Value,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let config: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid JSON configuration {}", path.display()))?;
        config.validate_base()?;
        Ok(config)
    }

    pub fn validate_base(&self) -> Result<()> {
        if self.routes.is_empty() {
            bail!("configuration must contain at least one route");
        }
        if self.delivery.workers == 0 {
            bail!("delivery.workers must be greater than zero");
        }
        if self.delivery.max_attempts == 0 {
            bail!("delivery.max_attempts must be greater than zero");
        }
        if self.server.public_base_url.scheme() != "http"
            && self.server.public_base_url.scheme() != "https"
        {
            bail!("server.public_base_url must use http or https");
        }

        let mut ids = HashSet::new();
        for route in &self.routes {
            if route.id.trim().is_empty() {
                bail!("route IDs cannot be empty");
            }
            if !ids.insert(&route.id) {
                bail!("duplicate route ID {:?}", route.id);
            }
            if route.src.id.trim().is_empty() {
                bail!("route {:?} source reference cannot be empty", route.id);
            }
            if route.dst.id.trim().is_empty() {
                bail!("route {:?} destination reference cannot be empty", route.id);
            }
            if !self.srcs.contains_key(&route.src.id) {
                bail!(
                    "route {:?} references missing source {:?}",
                    route.id,
                    route.src.id
                );
            }
            if !self.dsts.contains_key(&route.dst.id) {
                bail!(
                    "route {:?} references missing destination {:?}",
                    route.id,
                    route.dst.id
                );
            }
            if route.message.trim().is_empty() {
                bail!("route {:?} message cannot be empty", route.id);
            }
        }
        for id in self.srcs.keys() {
            if id.trim().is_empty() {
                bail!("source IDs cannot be empty");
            }
        }
        for id in self.dsts.keys() {
            if id.trim().is_empty() {
                bail!("destination IDs cannot be empty");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> Config {
        let plugin = PluginConfig {
            plugin: "x".into(),
            spec: Value::Null,
        };
        Config {
            server: ServerConfig {
                bind: "127.0.0.1:8080".parse().unwrap(),
                public_base_url: "https://example.test".parse().unwrap(),
            },
            storage: StorageConfig {
                sqlite_path: ":memory:".into(),
            },
            delivery: DeliveryConfig::default(),
            srcs: HashMap::from([("source".into(), plugin.clone())]),
            dsts: HashMap::from([("destination".into(), plugin)]),
            routes: vec![RouteConfig {
                id: "route".into(),
                src: RouteEndpointConfig {
                    id: "source".into(),
                    input: Value::Null,
                },
                dst: RouteEndpointConfig {
                    id: "destination".into(),
                    input: Value::Null,
                },
                message: "test".into(),
            }],
        }
    }

    #[test]
    fn rejects_duplicate_route_ids() {
        let mut config = config();
        config.routes.push(RouteConfig {
            id: "route".into(),
            src: RouteEndpointConfig {
                id: "source".into(),
                input: Value::Null,
            },
            dst: RouteEndpointConfig {
                id: "destination".into(),
                input: Value::Null,
            },
            message: "test".into(),
        });
        assert!(
            config
                .validate_base()
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[test]
    fn rejects_empty_ids_and_missing_references() {
        let mut value = serde_json::to_value(config()).unwrap();
        value["srcs"] = json!({"": {"plugin": "x", "spec": null}});
        value["routes"][0]["src"] = json!({"id": "", "input": null});
        let invalid: Config = serde_json::from_value(value).unwrap();
        assert!(invalid.validate_base().is_err());

        let mut config = config();
        config.routes[0].dst.id = "missing".into();
        assert!(
            config
                .validate_base()
                .unwrap_err()
                .to_string()
                .contains("missing destination")
        );
    }
}
