use crate::controller::RateLimitingController;
use actix_web::http::StatusCode;
use actix_web::{
    get, middleware, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_prom::PrometheusMetrics;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use serde::Deserialize;
use std::path::Path;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

mod controller;
mod logging;
mod mutating_webhook;

#[actix_rt::main]
async fn main() -> Result<(), ()> {
    logging::init_logging();

    let prometheus = PrometheusMetrics::new("", Some("/metrics"), None);
    mutating_webhook::register_metrics(&prometheus.registry);

    let controller = controller::RateLimitingController::new(&prometheus.registry).await;
    let controller_app_data = controller.clone();

    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(controller_app_data.clone())
            .wrap(prometheus.clone()) // for now this must be first or we'll log calls to `/metrics` as a 404. Ref: https://github.com/nlopes/actix-web-prom/issues/39
            .wrap(
                middleware::Logger::default()
                    .exclude("/readyz")
                    .exclude("/livez")
                    .exclude("/metrics"),
            )
            .service(ready)
            .service(live)
            .service(echo)
            .service(try_release_pod)
            .service(mutating_webhook::mutate)
            .service(mutating_webhook::validate)
    })
    .bind("0.0.0.0:8080")
    .expect("Can not bind to 0.0.0.0:8080")
    .shutdown_timeout(0);

    // TLS config
    if Path::new("/tmp/k8s-webhook-server/serving-certs").exists() {
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
        builder
            .set_private_key_file(
                "/tmp/k8s-webhook-server/serving-certs/tls.key",
                SslFiletype::PEM,
            )
            .unwrap();
        builder
            .set_certificate_chain_file("/tmp/k8s-webhook-server/serving-certs/tls.crt")
            .unwrap();

        server = server
            .bind_openssl("0.0.0.0:8443", builder)
            .expect("Can not bind to 0.0.0.0:8443");
    }

    tokio::select! {
        _ = controller.run() => {
                error!("controller.run() ended unexpectedly");
                unreachable!("controller.run() ended. This should NOT happen - the `server` should exit first.")
            },
        _ = server.run() => println!("server.run() ended"),
    }

    Ok(())
}

#[get("/readyz")]
async fn ready(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[get("/livez")]
async fn live(controller: RateLimitingController) -> impl Responder {
    if controller.live().await {
        HttpResponse::Ok().body("OK")
    } else {
        HttpResponse::ServiceUnavailable().body("NOT OK")
    }
}

#[post("/echo")]
async fn echo(req: HttpRequest, req_body: String) -> impl Responder {
    println!("{:?}", req);
    println!("{}", req_body);
    HttpResponse::Ok().body(req_body)
}

#[get("/try_release_pod")]
async fn try_release_pod(
    controller: RateLimitingController,
    query: web::Query<PodReleasedQuery>,
) -> impl Responder {
    if controller.try_release_pod(query.node.as_str(), query.pod.as_str()) {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::LOCKED)
    }
}

#[derive(Deserialize)]
struct PodReleasedQuery {
    node: String,
    pod: String,
}
