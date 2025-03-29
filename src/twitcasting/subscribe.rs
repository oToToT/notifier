use super::TwitcastingConfig;
use crate::db;
use actix_web::{HttpResponse, Responder, web};
use base64::prelude::{BASE64_STANDARD, Engine};
use log::debug;
use regex::Regex;
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
    username: String,
}

fn get_token(client_id: &str, client_secret: &str) -> String {
    BASE64_STANDARD.encode(format!("{}:{}", client_id, client_secret).as_bytes())
}

fn get_auth_headers(client_id: &str, client_secret: &str) -> HeaderMap {
    let token = get_token(client_id, client_secret);

    let mut headers = HeaderMap::new();
    headers.insert("X-Api-Version", HeaderValue::from_static("2.0"));
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Basic {}", token)).expect("Invalid token"),
    );
    headers.insert("Accept", HeaderValue::from_static("application/json"));

    headers
}

async fn get_user_id_from_username(
    username: &str,
    config: &TwitcastingConfig,
) -> Result<String, String> {
    let response = reqwest::Client::new()
        .get(format!("https://apiv2.twitcasting.tv/users/{}", username))
        .headers(get_auth_headers(&config.client_id, &config.client_secret))
        .send()
        .await
        .map_err(|_| "Failed to connect to twitcasting API")?;

    if response.status().is_success() {
        let user_info: serde_json::Value = response
            .json()
            .await
            .map_err(|_| "Failed to parse response")?;
        if let Some(user_id) = user_info["id"].as_str() {
            Ok(user_id.to_string())
        } else {
            Err("Failed to extract user id".to_string())
        }
    } else {
        Err("Failed to get user ID".to_string())
    }
}

pub fn record_hook(pool: &db::Pool, user_id: &str, username: &str) {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute(
            "INSERT OR IGNORE INTO twitcasting (user_id, username) VALUES (?, ?)",
            rusqlite::params![user_id, username],
        )
        .expect("Failed to insert or update user");
}

pub async fn subscribe(
    info: web::Query<SubscribeRequest>,
    config: web::Data<TwitcastingConfig>,
    pool: web::Data<db::Pool>,
) -> impl Responder {
    let username_regex = Regex::new(r"^[A-Za-z0-9_]$").expect("Failed to create validation regex");
    if !username_regex.is_match(&info.username) {
        return HttpResponse::BadRequest().body("Invalid username format");
    }

    let user_id = get_user_id_from_username(&info.username, &config)
        .await
        .expect("Failed to get user ID");

    let response = reqwest::Client::new()
        .post("https://apiv2.twitcasting.tv/webhooks")
        .headers(get_auth_headers(&config.client_id, &config.client_secret))
        .json(&SubscriptionPayload {
            user_id: user_id.clone(),
            events: vec!["livestart".to_string()],
        })
        .send()
        .await
        .expect("Failed to send subscription request");
    debug!("response: {:?}", response);
    if response.status().is_success() {
        let response_body: SubscriptionResponse =
            response.json().await.expect("Failed to parse response");
        record_hook(&pool, &user_id, &info.username);
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
