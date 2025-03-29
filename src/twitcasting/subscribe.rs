use crate::db;
use actix_web::{HttpResponse, Responder, web};
use log::debug;
use regex::Regex;
use serde::{Deserialize, Serialize};

use super::{TwitcastingConfig, get_auth_headers};

#[derive(Deserialize)]
struct User {
    id: String,
}

#[derive(Deserialize)]
struct UsersResponse {
    user: User,
}

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
        let user_info: UsersResponse = response
            .json()
            .await
            .map_err(|_| "Failed to parse response")?;
        Ok(user_info.user.id)
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
    let username_regex = Regex::new(r"^[A-Za-z0-9_]+$").expect("Failed to create validation regex");
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
