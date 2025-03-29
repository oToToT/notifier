use crate::db;
use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use super::{TwitchConfig, get_auth_headers, get_token};

#[derive(Deserialize)]
pub struct UnsubscribeRequest {
    username: String,
}

pub fn remove_from_db(pool: &db::Pool, id: &str) -> rusqlite::Result<()> {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute("DELETE FROM twitch WHERE id = ?", rusqlite::params![id])?;
    Ok(())
}

fn get_id_from_username(username: &str, pool: &db::Pool) -> rusqlite::Result<String> {
    let conn = pool.get().expect("Failed to get connection from pool");
    let mut binding = conn.prepare("SELECT id FROM twitch WHERE username = ?")?;
    let mut rows = binding.query(rusqlite::params![username])?;
    if let Some(row) = rows.next()? {
        Ok(row.get(0)?)
    } else {
        Err(rusqlite::Error::QueryReturnedNoRows)
    }
}

pub async fn unsubscribe(
    req: web::Query<UnsubscribeRequest>,
    config: web::Data<TwitchConfig>,
    pool: web::Data<db::Pool>,
) -> impl Responder {
    let id =
        get_id_from_username(&req.username, &pool).expect("Failed to get user ID from database");

    let token = get_token(&config.client_id, &config.client_secret)
        .await
        .expect("Failed to get token");

    let response = reqwest::Client::new()
        .delete(format!(
            "https://api.twitch.tv/helix/eventsub/subscriptions?id={}",
            id
        ))
        .headers(get_auth_headers(&config.client_id, &token))
        .send()
        .await
        .expect("Failed to connect to Twitch API");

    if response.status().is_success() {
        remove_from_db(&pool, &id).expect("Failed to remove user from database");
        println!("Unsubscribed from Twitch successfully");
    }

    HttpResponse::Ok().finish()
}
