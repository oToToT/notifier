use actix_web::{HttpResponse, Responder, web};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use super::TwitchConfig;

#[derive(Serialize)]
struct SubscriptionPayload {
    #[serde(rename = "type")]
    subscription_type: String,
    version: String,
    condition: Condition,
    transport: Transport,
}

#[derive(Serialize, Deserialize)]
struct Condition {
    broadcaster_user_id: String,
}

#[derive(Serialize, Deserialize)]
struct Transport {
    method: String,
    callback: String,
    secret: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct SubscriptionResponse {
    data: Vec<SubscriptionData>,
}

#[derive(Serialize, Deserialize)]
struct SubscriptionData {
    id: String,
    #[serde(rename = "type")]
    subscription_type: String,
    version: String,
    status: String,
    cost: i32,
    condition: Condition,
    transport: Transport,
    created_at: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
pub struct SubscribeRequest {
    id: String,
}

async fn get_twitch_token(
    client_id: &str,
    client_secret: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let res = client.post(format!("https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type=client_credentials", client_id, client_secret))
    .send()
    .await?
    .json::<TokenResponse>()
    .await?;

    Ok(res.access_token)
}

pub async fn subscribe(
    info: web::Query<SubscribeRequest>,
    config: web::Data<TwitchConfig>,
    service_url: web::Data<url::Url>,
) -> impl Responder {
    let webhook_url = service_url
        .join("./webhook")
        .expect("Failed to setup webhook url");

    let token = get_twitch_token(&config.client_id, &config.client_secret)
        .await
        .expect("Failed to get token");

    let mut headers = HeaderMap::new();
    headers.insert(
        "Client-ID",
        HeaderValue::from_str(&config.client_id).expect("Invalid client ID"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token)).expect("Invalid token"),
    );
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));

    let response = reqwest::Client::new()
        .post("https://api.twitch.tv/helix/eventsub/subscriptions")
        .headers(headers)
        .json(&SubscriptionPayload {
            subscription_type: "stream.online".to_string(),
            version: "1".to_string(),
            condition: Condition {
                broadcaster_user_id: info.id.clone(),
            },
            transport: Transport {
                method: "webhook".to_string(),
                callback: webhook_url.to_string(),
                secret: Some(config.twitch_webhook_secret.clone()),
            },
        })
        .send()
        .await
        .expect("Failed to send subscription request");

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
