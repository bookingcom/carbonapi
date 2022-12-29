# tesing expand
GET http://localhost:8081/metrics/expand
[QueryStringParams]
query:some.*
from:-10m
until:now
noCache:1

HTTP/* 200
Content-Type: application/json
[Asserts]
jsonpath "$.results[*]" count == 2
jsonpath "$.results[0]" == "some.test"
jsonpath "$.results[1]" == "some.test2"