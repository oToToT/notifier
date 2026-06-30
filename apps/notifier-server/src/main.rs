use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use notifier_destination_discord::DiscordDestination;
use notifier_destination_telegram::TelegramDestination;
use notifier_runtime::{Config, RuntimeBuilder};
use notifier_source_nitter::NitterSource;
use notifier_source_twitcasting::TwitCastingSource;
use notifier_source_twitch::TwitchSource;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Durable, plugin-based livestream notifier")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate configuration without contacting providers or opening SQLite.
    CheckConfig {
        #[arg(short, long, default_value = "config.json")]
        config: PathBuf,
    },
    /// Print combined runtime/plugin JSON Schema and template documentation.
    Schema,
    /// Reconcile subscriptions and run the HTTP and delivery services.
    Serve {
        #[arg(short, long, default_value = "config.json")]
        config: PathBuf,
    },
}

fn builder() -> RuntimeBuilder {
    RuntimeBuilder::new()
        .source(NitterSource::new())
        .source(TwitchSource::new())
        .source(TwitCastingSource::new())
        .destination(DiscordDestination::new())
        .destination(TelegramDestination::new())
}

fn init_logging(log_level: Option<&str>) -> Result<()> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => {
            let level = log_level.unwrap_or("info");
            EnvFilter::try_new(level)
                .with_context(|| format!("invalid server.log_level directive {level:?}"))?
        }
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let builder = builder();
    match cli.command {
        Command::CheckConfig { config } => {
            let config = Config::load(&config)?;
            init_logging(config.server.log_level.as_deref())?;
            debug!(path = %config.server.public_base_url, "checking notifier configuration");
            builder.check_config(config)?;
            info!("configuration is valid");
            println!("configuration is valid");
            Ok(())
        }
        Command::Schema => {
            init_logging(None)?;
            debug!("printing notifier schema");
            println!(
                "{}",
                serde_json::to_string_pretty(&builder.schema())
                    .context("failed to serialize schema")?
            );
            Ok(())
        }
        Command::Serve { config } => {
            let config = Config::load(&config)?;
            init_logging(config.server.log_level.as_deref())?;
            info!(
                bind = %config.server.bind,
                public_base_url = %config.server.public_base_url,
                "starting notifier server"
            );
            builder.check_config(config)?.serve().await
        }
    }
}
