use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use notifier_destination_discord::DiscordDestination;
use notifier_destination_telegram::TelegramDestination;
use notifier_runtime::{Config, RuntimeBuilder};
use notifier_source_twitcasting::TwitCastingSource;
use notifier_source_twitch::TwitchSource;
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
        .source(TwitchSource::new())
        .source(TwitCastingSource::new())
        .destination(DiscordDestination::new())
        .destination(TelegramDestination::new())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let builder = builder();
    match cli.command {
        Command::CheckConfig { config } => {
            let config = Config::load(&config)?;
            builder.check_config(config)?;
            println!("configuration is valid");
            Ok(())
        }
        Command::Schema => {
            println!(
                "{}",
                serde_json::to_string_pretty(&builder.schema())
                    .context("failed to serialize schema")?
            );
            Ok(())
        }
        Command::Serve { config } => {
            let config = Config::load(&config)?;
            builder.check_config(config)?.serve().await
        }
    }
}
