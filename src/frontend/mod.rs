use actix_web::{HttpResponse, Responder, http::StatusCode, web};

pub async fn index() -> impl Responder {
    HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(include_str!("./index.html"))
}

pub fn get_services() -> Vec<impl actix_web::dev::HttpServiceFactory> {
    vec![web::resource("/").route(web::get().to(index))]
}
