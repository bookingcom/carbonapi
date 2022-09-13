#!/usr/bin/env bash

# System test for the go-carbon -> zipper -> api stack.
# Brings the systeem up, performs single write, and read.
# Checks if data got in and out.
#
# Needs to be run from the root directory of the project.

docker-compose --ansi never up -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }

CARBON_HOST=127.0.0.1
CARBON_PORT=2003
CARBON_RETRIES=5
CARBON_SLEEP_SEC=1

for i in $(seq 1 $CARBON_RETRIES); do
    if nc -w 1 $CARBON_HOST $CARBON_PORT; then
        break
    fi
    echo "Waiting for the carbon server to come up..."
    sleep $CARBON_SLEEP_SEC
done

if [ "$i" -eq $CARBON_RETRIES ]; then
    echo ">>> ERROR: The carbon server is not up after $((CARBON_RETRIES*CARBON_SLEEP_SEC)) seconds"
    exit 2
fi

METRIC_NAME="some.test.metric"
TEST_VALUE=123456

echo "$METRIC_NAME $TEST_VALUE $(date +%s)" | nc -w 1 $CARBON_HOST $CARBON_PORT || { printf ">>> ERROR: Putting data in failed"; exit 2; }

CARBAPI_HOST=127.0.0.1
CARBAPI_PORT=8081
CARBAPI_RETRIES=5000
CARBAPI_SLEEP_SEC=1

for i in $(seq 1 $CARBAPI_RETRIES); do
    if curl -sS "http://$CARBAPI_HOST:$CARBAPI_PORT/render?target=$METRIC_NAME&format=json" | grep -q $TEST_VALUE; then
        res='SUCCESS!'
        rc=0
        break
    else
        echo "Waiting for test datapoints to show up..."
        sleep $CARBAPI_SLEEP_SEC
    fi
done

if [ "$i" -eq $CARBAPI_RETRIES ]; then
    res='FAIL!'
    rc=1
fi

echo ">>> $res"

docker-compose down

exit $rc
