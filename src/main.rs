use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
use clap::Parser;
use env_logger::Env;
use figment::{
    Figment,
    providers::{Format, Json},
};
use log_once::info_once;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Deserialize;

mod controller;
mod db;
mod discord;
mod twitcasting;
mod twitch;

#[derive(Deserialize, Clone)]
struct Config {
    base_url: url::Url,
    db_path: String,
    discord_token: String,
    discord_channel_id: u64,
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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let config: Config = Figment::new()
        .join(Json::file(args.config))
        .extract()
        .expect("Failed to load config");

    let manager = SqliteConnectionManager::file(config.db_path.clone());
    let pool = db::Pool::new(manager).expect("Failed to connect to database");

    env_logger::init_from_env(Env::default().default_filter_or("info"));
    HttpServer::new(move || {
        let config = config.clone();
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(discord::Bot::new(
                &config.discord_token,
                config.discord_channel_id,
            )))
            .app_data(web::Data::new(pool.clone()))
            .configure(|app| {
                macro_rules! add_service {
                    ($service:literal, $config:ident, $config_name:ident) => {
                        if let Some(config) = $config.$config_name {
                            info_once!("Adding service: {}", $service);
                            $config_name::init_db(&pool);
                            app.service(
                                web::scope($service)
                                    .app_data(web::Data::new(
                                        $config.base_url.join(concat!($service, "/")).expect(
                                            format!("Failed to setup service url: {}", $service)
                                                .as_str(),
                                        ),
                                    ))
                                    .app_data(web::Data::new(config))
                                    .service($config_name::get_services()),
                            );
                        }
                    };
                    ($service:literal, $module:ident) => {
                        info_once!("Adding service: {}", $service);
                        if $service == "/" {
                            app.service($module::get_services());
                        } else {
                            app.service(web::scope($service).service($module::get_services()));
                        }
                    };
                }

                add_service!("/twitch", config, twitch);
                add_service!("/twitcasting", config, twitcasting);
                add_service!("/", controller);
            })
    })
    .bind((args.host, args.port))?
    .run()
    .await
}
