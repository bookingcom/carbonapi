# testing find w/completer format
# i.e. {"metrics":[{"path":"some.test.","name":"","is_leaf":"0"},{"path":"some.test2.","name":"","is_leaf":"0"}]}
GET http://localhost:8081/metrics/find
[QueryStringParams]
query:some.*
noCache:1
from:-10m
until:now
format:completer

HTTP/* 200
Content-Type: json
[Asserts]
jsonpath "$[*]" count == 2
jsonpath "$.metrics[0].path" == "some.test."
jsonpath "$.metrics[0].is_leaf" == "0"
jsonpath "$.metrics[1].path" == "some.test2."
jsonpath "$.metrics[1].is_leaf" == "0"
