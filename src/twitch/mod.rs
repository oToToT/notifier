use crate::db;
use actix_web::web;
use rusqlite::params;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct TwitchConfig {
    client_id: String,
    client_secret: String,
    twitch_webhook_secret: String,
}

mod subscribe;
mod webhook;

pub fn init_db(pool: &db::Pool) {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute("CREATE TABLE IF NOT EXISTS twitch (id TEXT PRIMARY KEY, username TEXT)", params![])
        .expect("Failed to init twitch db");
}

pub fn get_services() -> Vec<impl actix_web::dev::HttpServiceFactory> {
    vec![
        web::resource("/subscribe").route(web::get().to(subscribe::subscribe)),
        web::resource("/webhook").route(web::post().to(webhook::webhook)),
    ]
}
