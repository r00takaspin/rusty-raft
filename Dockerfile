FROM rust:1.90.0 as builder

WORKDIR /app
COPY . .

# Собираем релизный бинарь
RUN cargo build --release

# Финальный минимальный образ
FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/target/release/rusty-raft /usr/local/bin/app

CMD ["app"]
