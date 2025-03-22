use actix_web::web;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct TwitcastingConfig {
    client_id: String,
    client_secret: String,
    webhook_signature: String,
}

mod subscribe;
mod webhook;

pub fn get_services() -> Vec<impl actix_web::dev::HttpServiceFactory> {
    vec![
        web::resource("/subscribe").route(web::get().to(subscribe::subscribe)),
        web::resource("/webhook").route(web::post().to(webhook::webhook)),
    ]
}
