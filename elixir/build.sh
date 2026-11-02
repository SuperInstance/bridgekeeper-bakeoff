#!/usr/bin/env bash
# Build the bridgekeeper OTP release. Non-interactive; zero hex
# dependencies, so nothing is fetched from any registry -- the only
# requirement is the installed Elixir/OTP toolchain.
set -euo pipefail
cd "$(dirname "$0")"
export MIX_ENV=prod
mix release --overwrite
echo "built: _build/prod/rel/bridgekeeper"
