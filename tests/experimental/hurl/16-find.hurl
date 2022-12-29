# testing find w/default format
# should be same as format=json
GET http://localhost:8081/metrics/find
[QueryStringParams]
query:some.test.metric
from:-10m
until:now
noCache:1

HTTP/* 200
Content-Type: application/json
[Asserts]
jsonpath "$[0].text" == "metric"
jsonpath "$[0].leaf" == 1
jsonpath "$[0].expandable" == 0
jsonpath "$[0].allowChildren" == 0