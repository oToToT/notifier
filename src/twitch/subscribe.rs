use crate::db;
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
    username: String,
}

#[derive(Deserialize)]
struct User {
    id: String,
}

#[derive(Deserialize)]
struct UsersResponse {
    data: Vec<User>,
}

async fn get_token(
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

fn get_auth_headers(client_id: &str, token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Client-ID",
        HeaderValue::from_str(client_id).expect("Invalid client ID"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token)).expect("Invalid token"),
    );
    headers
}

async fn get_user_id_from_username(
    username: &str,
    client_id: &str,
    token: &str,
) -> Result<String, String> {
    let response = reqwest::Client::new()
        .get(format!(
            "https://api.twitch.tv/helix/users?login={}",
            username
        ))
        .headers(get_auth_headers(client_id, token))
        .send()
        .await
        .map_err(|_| "Failed to connect to twitcasting API")?;

    if response.status().is_success() {
        let user_info: UsersResponse = response
            .json()
            .await
            .map_err(|_| "Failed to parse response")?;
        if !user_info.data.is_empty() {
            Ok(user_info.data[0].id.clone())
        } else{
            Err("Failed to extract user id".to_string())
        }
    } else {
        Err("Failed to get user ID".to_string())
    }
}

pub fn record_hook(pool: &db::Pool, id: &str, username: &str) {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute(
            "INSERT OR IGNORE INTO twitch (id, username) VALUES (?, ?)",
            rusqlite::params![id, username],
        )
        .expect("Failed to insert or update user");
}

pub async fn subscribe(
    info: web::Query<SubscribeRequest>,
    config: web::Data<TwitchConfig>,
    service_url: web::Data<url::Url>,
    pool: web::Data<db::Pool>,
) -> impl Responder {
    let webhook_url = service_url
        .join("./webhook")
        .expect("Failed to setup webhook url");

    let token = get_token(&config.client_id, &config.client_secret)
        .await
        .expect("Failed to get token");

    let response = reqwest::Client::new()
        .post("https://api.twitch.tv/helix/eventsub/subscriptions")
        .headers(get_auth_headers(&config.client_id, &token))
        .json(&SubscriptionPayload {
            subscription_type: "stream.online".to_string(),
            version: "1".to_string(),
            condition: Condition {
                broadcaster_user_id: get_user_id_from_username(
                    &info.username,
                    &config.client_id,
                    &token,
                )
                .await
                .expect("Failed to get user ID"),
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
        if !response_body.data.is_empty() {
            let subscription = &response_body.data[0];
            record_hook(&pool, &subscription.id, &info.username);
            println!(
                "Subscription successful: {:?}",
                serde_json::to_string(&response_body).expect("Failed to serialize response")
            );
        }
    } else {
        let error_body = response.text().await.expect("Failed to get response body");
        eprintln!("Subscription failed: {:?}", error_body);
    }

    HttpResponse::Ok().finish()
}
