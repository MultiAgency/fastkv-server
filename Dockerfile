FROM rust:1.85 AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
COPY static ./static
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
RUN useradd -r -s /usr/sbin/nologin fastkv
COPY --from=builder /app/target/release/fastkv-server /usr/local/bin/
COPY --from=builder /app/static /app/static
WORKDIR /app
USER fastkv
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD curl -f http://localhost:${PORT:-3001}/health || exit 1
CMD ["fastkv-server"]
