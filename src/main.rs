use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
use clap::Parser;
use env_logger::Env;
use figment::{
    Figment,
    providers::{Format, Json},
};
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

    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 8787)]
    port: u16,
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
            if let Some(twitch_config) = config.twitch {
                println!("Twitch loaded");
                app.service(
                    web::scope("/twitch")
                        .app_data(
                            config
                                .base_url
                                .join("./twitch/")
                                .expect("Failed to setup twitch URL"),
                        )
                        .app_data(twitch_config)
                        .service(twitch::get_services()),
                );
            }
            if let Some(twitcasting_config) = config.twitcasting {
                println!("Twitcasting loaded");
                app.service(
                    web::scope("/twitcasting")
                        .app_data(
                            config
                                .base_url
                                .join("./twitcasting/")
                                .expect("Failed to setup twitcasting URL"),
                        )
                        .app_data(twitcasting_config)
                        .service(twitcasting::get_services()),
                );
            }
        })
    })
    .bind((args.host, args.port))?
    .run()
    .await
}
