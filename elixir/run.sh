#!/usr/bin/env bash
# Start the bridgekeeper daemon in the foreground on 127.0.0.1:14111.
set -euo pipefail
cd "$(dirname "$0")"
REL=_build/prod/rel/bridgekeeper/bin/bridgekeeper
if [ ! -x "$REL" ]; then
  echo "release not built; running build.sh first" >&2
  ./build.sh
fi
# Mix-release scripts have no "foreground" subcommand: "start" IS
# the foreground mode (it blocks, attached); "daemon" is the
# detached one.
exec "$REL" start
