#!/bin/sh
# Build the Rust bridgekeeper. Zero dependencies, so this works
# fully offline: cargo never needs the network.
set -e
cd "$(dirname "$0")"
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release
echo "built: $(pwd)/target/release/bridgekeeper"
ls -la target/release/bridgekeeper
