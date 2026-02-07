FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/fastkv-server /usr/local/bin/
COPY --from=builder /app/static /app/static
WORKDIR /app
CMD ["fastkv-server"]
