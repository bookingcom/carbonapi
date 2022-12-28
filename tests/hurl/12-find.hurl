# test find w/raw format
GET http://localhost:8081/metrics/find
[QueryStringParams]
query:some.*
noCache:1
from:-10m
until:now
format:raw

HTTP/* 200
Content-Type: raw
[Asserts]
body contains "some.test."
body contains "some.test2."
