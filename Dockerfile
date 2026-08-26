# ── Build stage ──────────────────────────────────────────────────────────
FROM rust:1.83 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p game-server --bin nexum

# ── Runtime stage ────────────────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/nexum /usr/local/bin/nexum
COPY --from=builder /app/target/release/game-server /usr/local/bin/game-server

EXPOSE 9337
VOLUME ["/data"]

ENTRYPOINT ["nexum"]
CMD ["start", "--config", "/data/nexum.conf", "--persist", "/data/wal"]
