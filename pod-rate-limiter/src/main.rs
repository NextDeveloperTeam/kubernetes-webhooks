use actix_web::{get, middleware, post, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_prom::PrometheusMetrics;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use std::path::Path;
//use log::{debug, error, info, log_enabled, Level};

mod controller;
mod mutating_webhook;

#[actix_rt::main]
async fn main() -> Result<(), ()> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     .init();

    std::env::set_var("RUST_LOG", "actix_web=debug");
    env_logger::init();

    // TODO: set namespace
    let prometheus = PrometheusMetrics::new("", Some("/metrics"), None);

    let controller = controller::RateLimitingController::new(prometheus.registry.clone()).await;
    let controller_app_data = controller.clone();

    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(controller_app_data.clone())
            .wrap(prometheus.clone()) // for now, this must be first or we'll log /metrics as a 404. Ref: https://github.com/nlopes/actix-web-prom/issues/39
            .wrap(
                middleware::Logger::default()
                    .exclude("/healthz")
                    .exclude("/readyz"),
            )
            .service(health)
            .service(ready)
            .service(echo)
            .service(mutating_webhook::mutate)
            .service(controller::is_pod_released)
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
        _ = controller.run() => unreachable!("controller.run() ended. This should NOT happen - the `server` should exit first."), // TODO: log error as this shouldn't ever return first
        _ = server.run() => println!("server.run() ended"),
    }

    Ok(())
}

#[get("/healthz")]
async fn health(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[get("/readyz")]
async fn ready(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[post("/echo")]
async fn echo(req: HttpRequest, req_body: String) -> impl Responder {
    println!("{:?}", req);
    println!("{}", req_body);
    HttpResponse::Ok().body(req_body)
}
