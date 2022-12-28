#!/usr/bin/env bash

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

echo "some.test.metric 123456 $(date +%s)" | nc -w 1 $CARBON_HOST $CARBON_PORT || { printf ">>> ERROR: Putting data in failed"; exit 2; }
echo "some.test2.metric 654321 $(date +%s)" | nc -w 1 $CARBON_HOST $CARBON_PORT || { printf ">>> ERROR: Putting data in failed"; exit 2; }
echo "some.test2.metric2 111111 $(date +%s)" | nc -w 1 $CARBON_HOST $CARBON_PORT || { printf ">>> ERROR: Putting data in failed"; exit 2; }
