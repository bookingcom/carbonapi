#!/usr/bin/env bash

# System test for the go-carbon -> zipper -> api stack.
# Brings the systeem up, performs single write, and read.
# Checks if data got in and out.
#
# Needs to be run from the root directory of the project.

docker-compose build || { printf ">>> ERROR: Docker build failed"; exit 2; }
docker-compose up -d || { printf ">>> ERROR: Launching containers failed"; exit 2; }

# This is guestimate for the time when the system goes up and operational.
# Doing this other ways is much more complex. May need to be increased on slow boxes.
sleep 5

echo "test.test.test 123456 $(date +%s)" | nc -q 1 localhost 2003 || { printf ">>> ERROR: Putting data in failed"; exit 2; }

# wait to make sure data got there
sleep 2

if curl "http://localhost:8081/render?target=test.test.test&format=json" | grep -q "123456"
then
    printf ">>> SUCCESS!\\n"
    docker-compose down
    exit 0
else
    printf ">>> FAIL!\\n"
    docker-compose down
    exit 1
fi
