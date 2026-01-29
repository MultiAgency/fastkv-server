FROM rustlang/rust:nightly AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/fastkv-server /usr/local/bin/
CMD ["fastkv-server"]
