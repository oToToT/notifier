use actix_web::web;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct TwitcastingConfig {
    client_id: String,
    client_secret: String,
    webhook_secret: String,
}

mod webhook;

pub fn get_services() -> Vec<impl actix_web::dev::HttpServiceFactory> {
    vec![web::resource("/webhook").route(web::post().to(webhook::webhook))]
}
