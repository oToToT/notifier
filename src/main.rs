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

mod frontend;
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
    ($app:ident, $service:literal, $config:ident, $config_name:ident) => {
        if let Some(config) = $config.$config_name {
            info_once!("Adding service: {}", $service);
            $app.service(
                web::scope($service)
                    .app_data(web::Data::new($config.base_url.join($service).expect(
                        format!("Failed to setup service url: {}", $service).as_str(),
                    )))
                    .app_data(web::Data::new(config))
                    .service($config_name::get_services()),
            );
        }
    };
    ($app:ident, $service:literal, $module:ident) => {
        info_once!("Adding service: {}", $service);
        if $service == "/" {
            $app.service($module::get_services());
        } else {
            $app.service(web::scope($service).service($module::get_services()));
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
            add_service!(app, "/twitch/", config, twitch);
            add_service!(app, "/twitcasting/", config, twitcasting);
            add_service!(app, "/", frontend);
        })
    })
    .bind((args.host, args.port))?
    .workers(1)
    .run()
    .await
}
