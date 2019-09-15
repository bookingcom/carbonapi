#!/usr/bin/env bash

# System test for the go-carbon -> zipper -> api stack.
# Brings the systeem up, performs single write, and read.
# Checks if data got in and out.
#
# Needs to be run from the root directory of the project.

# `begin_fold` and `end_fold` create output folds in a job log in Travis CI
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
docker-compose up -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }
echo # `echo` to work around formatting issues after `docker-compose`
end_fold docker-up

# This is guestimate for the time when the system goes up and operational.
# Doing this other ways is much more complex. May need to be increased on slow boxes.
sleep 5

METRIC_NAME="some.test.metric"
TEST_VALUE=123456

begin_fold testing "Storing and retrieving datapoints"
echo "$METRIC_NAME $TEST_VALUE $(date +%s)" | nc -q 1 localhost 2003 || { printf ">>> ERROR: Putting data in failed"; exit 2; }

# wait to make sure data got there
sleep 2

if curl "http://localhost:8081/render?target=$METRIC_NAME&format=json" | grep -q $TEST_VALUE
then
    res='SUCCESS!'
    rc=0
else
    res='FAIL!'
    rc=1
fi
end_fold testing

echo ">>> $res"

begin_fold docker-down "Stopping containers"
docker-compose down
end_fold docker-down

exit $rc
