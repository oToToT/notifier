use actix_web::{Responder, web};
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

async fn subscribe_to_broadcaster_online(
    config: &TwitchConfig,
    webhook_url: &url::Url,
    broadcaster_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = get_twitch_token(&config.client_id, &config.client_secret).await?;

    let client = reqwest::Client::new();

    let url = "https://api.twitch.tv/helix/eventsub/subscriptions";

    let payload = SubscriptionPayload {
        subscription_type: "stream.online".to_string(),
        version: "1".to_string(),
        condition: Condition {
            broadcaster_user_id: broadcaster_id.to_string(),
        },
        transport: Transport {
            method: "webhook".to_string(),
            callback: webhook_url.to_string(),
            secret: Some(config.twitch_webhook_secret.clone()),
        },
    };

    let mut headers = HeaderMap::new();
    headers.insert("Client-ID", HeaderValue::from_str(&config.client_id)?);
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));

    let response = client
        .post(url)
        .headers(headers)
        .json(&payload)
        .send()
        .await?;

    if response.status().is_success() {
        let response_body: SubscriptionResponse = response.json().await?;
        println!(
            "Subscription successful: {:?}",
            serde_json::to_string(&response_body).unwrap()
        );
    } else {
        let error_body = response.text().await?;
        eprintln!("Subscription failed: {:?}", error_body);
    }

    Ok(())
}

pub async fn subscribe(
    info: web::Query<SubscribeRequest>,
    config: web::Data<TwitchConfig>,
    service_url: web::Data<url::Url>,
) -> impl Responder {
    subscribe_to_broadcaster_online(
        &config,
        &service_url
            .join("./webhook")
            .expect("Failed to setup webhook url"),
        &info.id,
    )
    .await
    .unwrap();
    "Ok!".to_string()
}
