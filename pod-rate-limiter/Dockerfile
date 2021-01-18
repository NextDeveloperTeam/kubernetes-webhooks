FROM rust:1.49 as builder
WORKDIR /build

# crate cache
COPY Cargo.toml .
COPY Cargo.lock .
RUN mkdir src \
  && echo "fn main() {}" > src/main.rs \
  && cargo build --release \ 
  && rm src/main.rs

# build
COPY . .
RUN touch src/main.rs && cargo build --release

# minimize
FROM debian:buster-slim

# TODO - static link ssl?  Switch back to rustls when not working w/ minikube? Find rustls workaround for when running w/ minikube
RUN apt-get update && apt-get -y install ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/pod-rate-limiter /pod-rate-limiter

CMD ["/pod-rate-limiter"]
