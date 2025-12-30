FROM rust:1.92-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/blobasaur /usr/local/bin/blobasaur

# Create data directory
RUN mkdir -p /app/data

# Expose Redis protocol port and gossip port
EXPOSE 6379 7000

CMD ["blobasaur"]
