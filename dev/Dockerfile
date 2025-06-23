FROM rust:1.87 as builder

WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/blobnom /usr/local/bin/blobnom

# Create data directory
RUN mkdir -p /app/data

# Expose Redis protocol port and gossip port
EXPOSE 6379 7000

CMD ["blobnom"]
