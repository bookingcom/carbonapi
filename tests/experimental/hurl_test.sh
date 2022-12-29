#!/usr/bin/env bash
set -euo pipefail

docker-compose --ansi never up --build -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }

# generate metrics
./tests/experimental/generate_metrics.sh

# run test with hurl https://hurl.dev
which hurl || exit 1

sleep 5
# you can add --very-verbose when debug tests
hurl --test --no-output --retry --glob "tests/experimental/hurl/*.hurl"
res=$?
echo $res

docker-compose down

exit $res