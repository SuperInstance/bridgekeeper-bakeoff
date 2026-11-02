#!/bin/sh
# build.sh -- build the C++ bridgekeeper from this directory.
# Non-interactive; installs nothing. Toolchain + dev libraries must
# already be present (see NOTES.md):
#   g++ (15.2 here), libcurl4-openssl-dev, nlohmann-json3-dev
set -e
cd "$(dirname "$0")"

for hdr in /usr/include/x86_64-linux-gnu/curl/curl.h /usr/include/curl/curl.h; do
    [ -f "$hdr" ] && CURL_OK=1
done
[ -f /usr/include/nlohmann/json.hpp ] && JSON_OK=1
if [ -z "$CURL_OK" ] || [ -z "$JSON_OK" ]; then
    echo "missing dev libraries; install with:" >&2
    echo "  sudo apt-get install -y libcurl4-openssl-dev nlohmann-json3-dev" >&2
    exit 1
fi

g++ -O2 -std=c++17 -Wall -Wextra -o bridgekeeper main.cpp -lcurl
echo "built bridgekeeper: $(du -h bridgekeeper | cut -f1)"
