use crate::db;
use actix_web::{Responder, web};

pub async fn list(pool: web::Data<db::Pool>) -> impl Responder {
    let conn = pool.get().expect("Failed to get connection from pool");
    let mut stmt = conn
        .prepare("SELECT username FROM twitch")
        .expect("Failed to prepare statement");
    let rows = stmt
        .query_map([], |row| row.get(0))
        .expect("Failed to query rows");

    let mut result = Vec::<String>::new();
    for row in rows {
        result.push(row.expect("Failed to get row"));
    }

    web::Json(result)
}
