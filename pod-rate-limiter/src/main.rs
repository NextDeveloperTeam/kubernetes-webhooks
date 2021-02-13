use actix_web::{get, middleware, post, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_prom::PrometheusMetrics;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use std::ffi::OsString;
use std::path::Path;
use tracing::Level;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::EnvFilter;

mod controller;
mod mutating_webhook;

#[actix_rt::main]
async fn main() -> Result<(), ()> {
    init_logging();

    let prometheus = PrometheusMetrics::new("", Some("/metrics"), None);

    let controller = controller::RateLimitingController::new(prometheus.registry.clone()).await;
    let controller_app_data = controller.clone();

    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(controller_app_data.clone())
            .wrap(prometheus.clone()) // for now this must be first or we'll log calls to `/metrics` as a 404. Ref: https://github.com/nlopes/actix-web-prom/issues/39
            .wrap(
                middleware::Logger::default()
                    .exclude("/healthz")
                    .exclude("/readyz")
                    .exclude("/metrics"),
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
        _ = controller.run() => {
                error!("controller.run() ended unexpectedly");
                unreachable!("controller.run() ended. This should NOT happen - the `server` should exit first.")
            },
        _ = server.run() => println!("server.run() ended"),
    }

    Ok(())
}

fn init_logging() {
    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::default()
            .add_directive(Level::DEBUG.into())
            .add_directive("hyper::proto=info".parse().unwrap())
            .add_directive("hyper::client=info".parse().unwrap()),
    };

    let one = OsString::from("1");
    match std::env::var_os("JSON_LOGGING") {
        Some(var) if var == one => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init(),
        _ => tracing_subscriber::fmt().with_env_filter(env_filter).init(),
    }
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
