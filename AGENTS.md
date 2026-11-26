# Agent guidelines

## Verification
- After making Rust code changes, always run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` before finishing (and run `cargo fmt` if formatting fails).
