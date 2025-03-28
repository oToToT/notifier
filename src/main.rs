use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
use clap::Parser;
use env_logger::Env;
use figment::{
    Figment,
    providers::{Format, Json},
};
use log_once::info_once;
use serde::Deserialize;

mod twitcasting;
mod twitch;

#[derive(Deserialize, Clone)]
struct Config {
    base_url: url::Url,
    twitch: Option<twitch::TwitchConfig>,
    twitcasting: Option<twitcasting::TwitcastingConfig>,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.json")]
    config: String,

    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 80)]
    port: u16,
}

macro_rules! add_service {
    ($app:ident, $config:ident, $config_name:ident, $service:literal) => {
        if let Some(c) = $config.$config_name {
            info_once!("Adding service: {}", $service);
            $app.service(
                web::scope($service)
                    .app_data(web::Data::new(
                        $config
                            .base_url
                            .join($service)
                            .expect("Failed to setup twitch URL"),
                    ))
                    .app_data(web::Data::new(c))
                    .service(twitch::get_services()),
            );
        }
    };
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let config: Config = Figment::new()
        .join(Json::file(args.config))
        .extract()
        .expect("Failed to load config");

    env_logger::init_from_env(Env::default().default_filter_or("info"));
    HttpServer::new(move || {
        let config = config.clone();
        App::new().wrap(Logger::default()).configure(|app| {
            add_service!(app, config, twitch, "./twitch/");
            add_service!(app, config, twitcasting, "./twitcasting/");
        })
    })
    .bind((args.host, args.port))?
    .run()
    .await
}
