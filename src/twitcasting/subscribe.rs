use super::TwitcastingConfig;
use actix_web::{HttpResponse, Responder, web};
use base64::prelude::{BASE64_STANDARD, Engine};
use log::debug;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct SubscriptionPayload {
    user_id: String,
    events: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct SubscriptionResponse {
    user_id: String,
    added_events: Vec<String>,
}

#[derive(Deserialize)]
pub struct SubscribeRequest {
    id: String,
}

fn get_twitcasting_token(client_id: &str, client_secret: &str) -> String {
    BASE64_STANDARD.encode(format!("{}:{}", client_id, client_secret).as_bytes())
}

pub async fn subscribe(
    info: web::Query<SubscribeRequest>,
    config: web::Data<TwitcastingConfig>,
) -> impl Responder {
    let token = get_twitcasting_token(&config.client_id, &config.client_secret);

    debug!("token: {}", token);

    let mut headers = HeaderMap::new();
    headers.insert("X-Api-Version", HeaderValue::from_static("2.0"));
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Basic {}", token)).expect("Invalid token"),
    );
    headers.insert("Accept", HeaderValue::from_static("application/json"));

    debug!("headers: {:?}", headers);

    let response = reqwest::Client::new()
        .post("https://apiv2.twitcasting.tv/webhooks")
        .headers(headers)
        .json(&SubscriptionPayload {
            user_id: info.id.clone(),
            events: vec!["livestart".to_string()],
        })
        .send()
        .await
        .expect("Failed to send subscription request");
    debug!("response: {:?}", response);
    if response.status().is_success() {
        let response_body: SubscriptionResponse =
            response.json().await.expect("Failed to parse response");
        println!(
            "Subscription successful: {:?}",
            serde_json::to_string(&response_body).expect("Failed to serialize response")
        );
    } else {
        let error_body = response.text().await.expect("Failed to get response body");
        eprintln!("Subscription failed: {:?}", error_body);
    }

    HttpResponse::Ok().finish()
}
