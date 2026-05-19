# -- Build stage --
FROM rust:latest AS builder
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
COPY src ./src
RUN touch src/main.rs && cargo build --release

# -- Runtime stage --
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates ffmpeg && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/media_worker /usr/local/bin/app
EXPOSE 8082
CMD ["app"]
