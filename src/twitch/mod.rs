use crate::db;
use actix_web::web;
use reqwest::header::{HeaderMap, HeaderValue};
use rusqlite::params;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct TwitchConfig {
    client_id: String,
    client_secret: String,
    twitch_webhook_secret: String,
}

mod list;
mod subscribe;
mod unsubscribe;
mod webhook;

pub fn init_db(pool: &db::Pool) {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute(
            "CREATE TABLE IF NOT EXISTS twitch (id TEXT PRIMARY KEY, username TEXT)",
            params![],
        )
        .expect("Failed to init twitch db");
}

pub fn get_services() -> Vec<impl actix_web::dev::HttpServiceFactory> {
    vec![
        web::resource("/subscribe").route(web::put().to(subscribe::subscribe)),
        web::resource("/unsubscribe").route(web::delete().to(unsubscribe::unsubscribe)),
        web::resource("/webhook").route(web::post().to(webhook::webhook)),
        web::resource("/list").route(web::get().to(list::list)),
    ]
}

async fn get_token(
    client_id: &str,
    client_secret: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
    }

    let client = reqwest::Client::new();
    let res = client.post(format!("https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type=client_credentials", client_id, client_secret))
    .send()
    .await?
    .json::<TokenResponse>()
    .await?;

    Ok(res.access_token)
}

fn get_auth_headers(client_id: &str, token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Client-ID",
        HeaderValue::from_str(client_id).expect("Invalid client ID"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token)).expect("Invalid token"),
    );
    headers
}
