#!/usr/bin/env bash
# Required verification entry for MCP-Dispatcher.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
