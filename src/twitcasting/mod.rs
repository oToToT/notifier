use crate::db;
use actix_web::web;
use base64::prelude::{BASE64_STANDARD, Engine};
use reqwest::header::{HeaderMap, HeaderValue};
use rusqlite::params;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct TwitcastingConfig {
    client_id: String,
    client_secret: String,
    webhook_signature: String,
}

mod list;
mod subscribe;
mod unsubscribe;
mod webhook;

pub fn init_db(pool: &db::Pool) {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute(
            "CREATE TABLE IF NOT EXISTS twitcasting (user_id TEXT PRIMARY KEY, username TEXT)",
            params![],
        )
        .expect("Failed to init twitcasting db");
}

pub fn get_services() -> Vec<impl actix_web::dev::HttpServiceFactory> {
    vec![
        web::resource("/subscribe").route(web::put().to(subscribe::subscribe)),
        web::resource("/unsubscribe").route(web::delete().to(unsubscribe::unsubscribe)),
        web::resource("/webhook").route(web::post().to(webhook::webhook)),
        web::resource("/list").route(web::get().to(list::list)),
    ]
}

fn get_token(client_id: &str, client_secret: &str) -> String {
    BASE64_STANDARD.encode(format!("{}:{}", client_id, client_secret).as_bytes())
}

fn get_auth_headers(client_id: &str, client_secret: &str) -> HeaderMap {
    let token = get_token(client_id, client_secret);

    let mut headers = HeaderMap::new();
    headers.insert("X-Api-Version", HeaderValue::from_static("2.0"));
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Basic {}", token)).expect("Invalid token"),
    );
    headers.insert("Accept", HeaderValue::from_static("application/json"));

    headers
}
