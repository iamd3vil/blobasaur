default: run

run:
    cargo run

run-release:
    cargo run --release

build:
    cargo build

build-linux $RUSTFLAGS="-C target-feature=+crt-static":
    cross build --release --target x86_64-unknown-linux-musl

build-macos $RUSTFLAGS="-C target-feature=+crt-static":
    #!/usr/bin/env sh
    if [ "$(uname)" = "Darwin" ]; then
        # Running on macOS
        cargo build --release --target aarch64-apple-darwin
    else
        # Running on non-macOS (Linux, Windows)
        docker run --rm \
        --volume ${PWD}:/root/src \
        --workdir /root/src \
        joseluisq/rust-linux-darwin-builder:1.86.0 \
        sh -c 'CC=aarch64-apple-darwin22.4-clang CXX=aarch64-apple-darwin22.4-clang++ TARGET_CC=aarch64-apple-darwin22.4-clang TARGET_AR=aarch64-apple-darwin22.4-ar cargo build --release --target aarch64-apple-darwin'
    fi

# Cluster development commands
cluster-clean:
    @echo "Cleaning target directory to avoid architecture conflicts..."
    rm -rf target/

cluster-build:
    just cluster-clean
    cd dev && docker-compose run --rm node1 bash -c "cargo build --release"

cluster-up:
    cd dev && docker-compose up -d

cluster-down:
    cd dev && docker-compose down

cluster-logs:
    cd dev && docker-compose logs -f

cluster-status:
    @echo "Checking cluster status..."
    @echo "Node 1 (port 6381):"
    -redis-cli -p 6381 CLUSTER INFO
    @echo "\nNode 2 (port 6382):"
    -redis-cli -p 6382 CLUSTER INFO
    @echo "\nNode 3 (port 6383):"
    -redis-cli -p 6383 CLUSTER INFO

cluster-reset:
    @echo "Resetting cluster..."
    cd dev && docker-compose down -v
    just cluster-clean
    cd dev && docker-compose up -d
    sleep 5

