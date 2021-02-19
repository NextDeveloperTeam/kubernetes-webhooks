use std::ffi::OsString;
use tracing::Level;
use tracing_subscriber::EnvFilter;

pub fn init_logging() {
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
            .try_init()
            .unwrap_or_default(),
        _ => tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .try_init()
            .unwrap_or_default(),
    }
}
