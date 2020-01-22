#!/usr/bin/env bash

# System test for the go-carbon -> zipper -> api stack.
# Brings the systeem up, performs single write, and read.
# Checks if data got in and out.
#
# Needs to be run from the root directory of the project.

# Use `begin_fold` and `end_fold` to fold large pieces of output in the job log in Travis CI
begin_fold() {
    if [ "$TRAVIS" == true ]; then
        echo "travis_fold:start:$1"
    fi
    echo -e "\e[33;1m$2\e[0m"
}

end_fold() {
    if [ "$TRAVIS" == true ]; then
        echo "travis_fold:end:$1"
    fi
}

begin_fold docker-build "Build docker images"
docker-compose build || { printf ">>> ERROR: Docker build failed"; exit 2; }
end_fold docker-build

begin_fold docker-up "Starting containers"
# `--no-ansi` fixes the console formatting issue of `docker-compose`
docker-compose --no-ansi up -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }
end_fold docker-up

CARBON_HOST=127.0.0.1
CARBON_PORT=2003
CARBON_RETRIES=5
CARBON_SLEEP_SEC=1

for i in `seq 1 $CARBON_RETRIES`; do
    nc $CARBON_HOST $CARBON_PORT
    if [ $? -eq 0 ]; then
        break
    fi
    echo "Waiting for the carbon server to come up..."
    sleep $CARBON_SLEEP_SEC
done

if [ $i -eq $CARBON_RETRIES ]; then
    echo ">>> ERROR: The carbon server is not up after $((CARBON_RETRIES*CARBON_SLEEP_SEC)) seconds"
    exit 2
fi

METRIC_NAME="some.test.metric"
TEST_VALUE=123456

begin_fold testing "Storing and retrieving datapoints"
echo "$METRIC_NAME $TEST_VALUE $(date +%s)" | nc -q 1 $CARBON_HOST $CARBON_PORT || { printf ">>> ERROR: Putting data in failed"; exit 2; }

CARBAPI_HOST=127.0.0.1
CARBAPI_PORT=8081
CARBAPI_RETRIES=5
CARBAPI_SLEEP_SEC=1

for i in `seq 1 $CARBAPI_RETRIES`; do
    curl -sS "http://$CARBAPI_HOST:$CARBAPI_PORT/render?target=$METRIC_NAME&format=json" | grep -q $TEST_VALUE
    if [ $? -eq 0 ]; then
        res='SUCCESS!'
        rc=0
        break
    else
        echo "Waiting for test datapoints to show up..."
        sleep $CARBAPI_SLEEP_SEC
    fi
done

if [ $i -eq $CARBAPI_RETRIES ]; then
    res='FAIL!'
    rc=1
fi
end_fold testing

echo ">>> $res"

begin_fold docker-down "Stopping containers"
docker-compose down
end_fold docker-down

exit $rc
