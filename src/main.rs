use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config: Config = Figment::new()
        .join(Json::file("config.json"))
        .extract()
        .expect("Failed to load config");

    env_logger::init_from_env(Env::default().default_filter_or("info"));
    HttpServer::new(move || {
        let config = config.clone();
        App::new().wrap(Logger::default()).configure(|app| {
            if let Some(twitch_config) = config.twitch {
                app.service(
                    web::scope("/twitch")
                        .app_data(
                            config
                                .base_url
                                .join("./twitch")
                                .expect("Failed to setup twitch URL"),
                        )
                        .app_data(twitch_config)
                        .service(twitch::get_services()),
                );
            }
            if let Some(twitcasting_config) = config.twitcasting {
                app.service(
                    web::scope("/twitcasting")
                        .app_data(
                            config
                                .base_url
                                .join("./twitcasting")
                                .expect("Failed to setup twitcasting URL"),
                        )
                        .app_data(twitcasting_config)
                        .service(twitcasting::get_services()),
                );
            }
        })
    })
    .bind(("127.0.0.1", 8787))?
    .run()
    .await
}
