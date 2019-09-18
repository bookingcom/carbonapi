#!/usr/bin/env bash

# System test for the go-carbon -> zipper -> api stack.
# Brings the systeem up, performs single write, and read.
# Checks if data got in and out.
#
# Needs to be run from the root directory of the project.



echo "travis_fold:start:docker-build"
echo -e "\e[33;1mBuild docker images\e[0m"
docker-compose build || { printf ">>> ERROR: Docker build failed"; exit 2; }
echo "travis_fold:end:docker-build"

echo "travis_fold:start:docker-up"
echo -e "\e[33;1mStarting containers\e[0m"
docker-compose up -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }
echo # `echo` to work around formatting issues after `docker-compose`
echo "travis_fold:end:docker-up"

# This is guestimate for the time when the system goes up and operational.
# Doing this other ways is much more complex. May need to be increased on slow boxes.
sleep 5

METRIC_NAME="some.test.metric"
TEST_VALUE=123456

echo "travis_fold:start:testing"
echo -e "\e[33;1mStoring and retrieving datapoints\e[0m"
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
echo "travis_fold:end:testing"

echo ">>> $res"

echo "travis_fold:start:docker-down"
echo -e "\e[33;1mStopping containers\e[0m"
docker-compose down
echo "travis_fold:end:docker-down"

exit $rc
