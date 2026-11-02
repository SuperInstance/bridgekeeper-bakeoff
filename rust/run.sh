#!/bin/sh
# Start the Rust bridgekeeper in the foreground on port 14112.
cd "$(dirname "$0")"
if [ ! -x target/release/bridgekeeper ]; then
    echo "binary not built yet; running build.sh first" >&2
    ./build.sh
fi
exec ./target/release/bridgekeeper
