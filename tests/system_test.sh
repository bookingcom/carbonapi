#!/usr/bin/env bash
set -euo pipefail

# System test for the go-carbon -> zipper -> api stack.
# Brings the systeem up, performs single write, and read.
# Checks if data got in and out.
#
# Needs to be run from the root directory of the project.

docker-compose --ansi never up --build -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }

# generate metrics
./tests/generate_metrics.sh

# run test with hurl https://hurl.dev
which hurl || ./tests/install_hurl.sh

sleep 5
# you can add --very-verbose when debug tests
hurl --test --no-output --retry --glob "tests/hurl/*.hurl"
res=$?
echo $res

docker-compose down

exit $res