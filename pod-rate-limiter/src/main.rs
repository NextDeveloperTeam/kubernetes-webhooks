use actix_web::{get, middleware, post, App, HttpRequest, HttpResponse, HttpServer, Responder};
use rustls::internal::pemfile::{certs, pkcs8_private_keys};
use rustls::{NoClientAuth, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
//use log::{debug, error, info, log_enabled, Level};

mod controller;
mod mutating_webhook;

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

#[actix_rt::main]
async fn main() -> Result<(),()> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     .init();

    std::env::set_var("RUST_LOG", "actix_web=debug");
    env_logger::init();

    let controller = controller::RateLimitingController::new();
    let controller_app_data = controller.clone();

    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(controller_app_data.clone())
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
    // TODO? switch this to the openssl style if we don't want to use both openssl & rustls (see rustls issues w/ minikube)
    if Path::new("/tmp/k8s-webhook-server/serving-certs").exists() {
        // load ssl keys
        let mut config = ServerConfig::new(NoClientAuth::new());
        let cert_file = &mut BufReader::new(
            File::open("/tmp/k8s-webhook-server/serving-certs/tls.crt").unwrap(),
        );
        let key_file = &mut BufReader::new(
            File::open("/tmp/k8s-webhook-server/serving-certs/tls.key").unwrap(),
        );
        let cert_chain = certs(cert_file).unwrap();
        let mut keys = pkcs8_private_keys(key_file).unwrap();
        config.set_single_cert(cert_chain, keys.remove(0)).unwrap();

        server = server
            .bind_rustls("0.0.0.0:8443", config)
            .expect("Can not bind to 0.0.0.0:8443");
    }

    tokio::select! {
        _ = controller.run() => println!("controller.run() ended"),
        _ = server.run() => println!("server.run() ended"),
    }

    Ok(())
}
