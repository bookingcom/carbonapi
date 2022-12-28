# testing expand with groupByExpr=1
GET http://localhost:8081/metrics/expand
[QueryStringParams]
query:some.*.*
from:-10m
until:now
noCache:1
groupByExpr:1

HTTP/* 200
Content-Type: application/json
[Asserts]
jsonpath "$.results[*][*]" count == 3
jsonpath "$.results[*][0]" == "some.test.metric"
jsonpath "$.results[*][1]" == "some.test2.metric"
jsonpath "$.results[*][2]" == "some.test2.metric2"