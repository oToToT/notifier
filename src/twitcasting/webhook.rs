use super::TwitcastingConfig;
use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Movie {
    user_id: String,
    title: String,
    subtitle: Option<String>,
    last_owner_comment: Option<String>,
    link: String,
    is_live: bool,
}

#[derive(Deserialize)]
pub struct User {
    id: String,
    screen_id: String,
    name: String,
}

#[derive(Deserialize)]
pub struct TwitcastingRequestBody {
    signature: String,
    movie: Movie,
    broadcaster: User,
}

pub async fn webhook(
    config: web::Data<TwitcastingConfig>,
    req: web::Json<TwitcastingRequestBody>,
) -> impl Responder {
    if config.webhook_secret != req.signature {
        HttpResponse::BadRequest().finish()
    } else if req.movie.is_live {
        assert!(req.movie.user_id == req.broadcaster.id);
        println!(
            "{} ({}) just went live!",
            req.broadcaster.name, req.broadcaster.screen_id
        );
        println!("title: {}", req.movie.title);
        if let Some(subtitle) = &req.movie.subtitle {
            println!("subtitle: {}", subtitle);
        }
        if let Some(last_owner_comment) = &req.movie.last_owner_comment {
            println!("last owner comment: {}", last_owner_comment);
        }
        println!("link: {}", req.movie.link);
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::Ok().finish()
    }
}
