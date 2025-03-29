use super::unsubscribe::remove_from_db;
use crate::db;
use actix_web::http::header::HeaderMap;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::TwitchConfig;

#[derive(Deserialize, Serialize)]
struct TwitchRequestBody {
    challenge: Option<String>,
    subscription: Option<Subscription>,
    event: Option<Event>,
}

#[derive(Deserialize, Serialize)]
struct Subscription {
    #[serde(rename = "type")]
    subscription_type: String,
    id: String,
}

#[derive(Deserialize, Serialize)]
struct Event {
    broadcaster_user_name: String,
}

fn get_hmac(secret: &str, data: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify_request(config: &TwitchConfig, headers: &HeaderMap, raw_body: &str) -> bool {
    let signature = headers.get("twitch-eventsub-message-signature");
    let message_id = headers.get("twitch-eventsub-message-id");
    let message_timestamp = headers.get("twitch-eventsub-message-timestamp");

    if let (Some(signature), Some(message_id), Some(message_timestamp)) =
        (signature, message_id, message_timestamp)
    {
        let signature = signature.to_str().unwrap();
        let message_id = message_id.to_str().unwrap();
        let message_timestamp = message_timestamp.to_str().unwrap();

        let hmac_hex = get_hmac(
            &config.twitch_webhook_secret,
            &format!("{}{}{}", message_id, message_timestamp, raw_body),
        );
        format!("sha256={}", hmac_hex) == signature
    } else {
        false
    }
}

fn webhook_callback_verification(body: &TwitchRequestBody) -> HttpResponse {
    if let Some(challenge) = &body.challenge {
        HttpResponse::Ok()
            .content_type("text/plain")
            .body(challenge.clone())
    } else {
        HttpResponse::BadRequest().finish()
    }
}

fn webhook_notification(body: &TwitchRequestBody) -> HttpResponse {
    if let Some(subscription) = &body.subscription {
        if subscription.subscription_type == "stream.online" {
            if let Some(event) = &body.event {
                println!("{} just went live!", event.broadcaster_user_name);
            }
        }
    }

    HttpResponse::Ok().finish()
}

fn webhook_revocation(body: &TwitchRequestBody, pool: &db::Pool) -> HttpResponse {
    if let Some(subscription) = &body.subscription {
        remove_from_db(pool, &subscription.id).expect("Failed to remove from db");
        println!("Subscription {} revoked", subscription.id);
    }
    HttpResponse::Ok().finish()
}

pub async fn webhook(
    config: web::Data<TwitchConfig>,
    req: HttpRequest,
    raw_body: String,
    pool: web::Data<db::Pool>,
) -> impl Responder {
    let headers = req.headers();
    if !verify_request(&config, headers, &raw_body) {
        return HttpResponse::Unauthorized().finish();
    }

    if let Some(message) = headers.get("twitch-eventsub-message-type") {
        let body: TwitchRequestBody = serde_json::from_str(&raw_body).unwrap();

        if message == "webhook_callback_verification" {
            webhook_callback_verification(&body)
        } else if message == "notification" {
            webhook_notification(&body)
        } else if message == "revocation" {
            webhook_revocation(&body, &pool)
        } else {
            HttpResponse::BadRequest().finish()
        }
    } else {
        HttpResponse::BadRequest().finish()
    }
}
