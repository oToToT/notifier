use crate::db;
use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use super::{TwitcastingConfig, get_auth_headers};

#[derive(Deserialize)]
pub struct UnsubscribeRequest {
    username: String,
}

fn remove_from_db(pool: &db::Pool, user_id: &str) -> rusqlite::Result<()> {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute(
            "DELETE FROM twitcasting WHERE user_id = ?",
            rusqlite::params![user_id],
        )?;
    Ok(())
}

fn get_user_id_from_username(username: &str, pool: &db::Pool) -> rusqlite::Result<String> {
    let conn = pool.get().expect("Failed to get connection from pool");
    let mut binding = conn.prepare("SELECT user_id FROM twitcasting WHERE username = ?")?;
    let mut rows = binding.query(rusqlite::params![username])?;
    if let Some(row) = rows.next()? {
        Ok(row.get(0)?)
    } else {
        Err(rusqlite::Error::QueryReturnedNoRows)
    }
}

pub async fn unsubscribe(
    req: web::Query<UnsubscribeRequest>,
    config: web::Data<TwitcastingConfig>,
    pool: web::Data<db::Pool>,
) -> impl Responder {
    let user_id = get_user_id_from_username(&req.username, &pool)
        .expect("Failed to get user ID from database");

    let response = reqwest::Client::new()
        .delete(format!(
            "https://apiv2.twitcasting.tv/webhooks?user_id={}&events[]=livestart",
            user_id
        ))
        .headers(get_auth_headers(&config.client_id, &config.client_secret))
        .send()
        .await
        .expect("Failed to connect to Twitcasting API");

    if response.status().is_success() {
        remove_from_db(&pool, &user_id).expect("Failed to remove user from database");
        println!("Unsubscribed from Twitcasting successfully");
    }

    HttpResponse::Ok().finish()
}
