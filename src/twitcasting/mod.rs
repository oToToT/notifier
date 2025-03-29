use crate::db;
use actix_web::web;
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
        web::resource("/subscribe").route(web::get().to(subscribe::subscribe)),
        web::resource("/webhook").route(web::post().to(webhook::webhook)),
        web::resource("/list").route(web::get().to(list::list)),
    ]
}
