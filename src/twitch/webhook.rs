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

fn webhook_callback_verification(body: &TwitchRequestBody) -> HttpResponse {
    if let Some(challenge) = &body.challenge {
        HttpResponse::Ok()
            .content_type("text/plain")
            .body(challenge.clone())
    } else {
        HttpResponse::BadRequest().finish()
    }
}

fn webhook_notification(
    config: &TwitchConfig,
    headers: &HeaderMap,
    body: &TwitchRequestBody,
    raw_body: &str,
) -> HttpResponse {
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
        if format!("sha256={}", hmac_hex) != signature {
            return HttpResponse::Forbidden().finish();
        }
    } else {
        return HttpResponse::BadRequest().finish();
    }

    if let Some(subscription) = &body.subscription {
        if subscription.subscription_type == "stream.online" {
            if let Some(event) = &body.event {
                println!("{} just went live!", event.broadcaster_user_name);
            }
        }
    }

    HttpResponse::Ok().finish()
}

pub async fn webhook(
    config: web::Data<TwitchConfig>,
    req: HttpRequest,
    raw_body: String,
) -> impl Responder {
    let body: TwitchRequestBody = serde_json::from_str(&raw_body).unwrap();
    let headers = req.headers();

    if let Some(message) = headers.get("twitch-eventsub-message-type") {
        if message == "webhook_callback_verification" {
            webhook_callback_verification(&body)
        } else if message == "notification" {
            webhook_notification(&config, headers, &body, &raw_body)
        } else {
            HttpResponse::BadRequest().finish()
        }
    } else {
        HttpResponse::BadRequest().finish()
    }
}
