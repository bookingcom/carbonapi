# test render w/json format
GET http://localhost:8081/render
[QueryStringParams]
target:some.test2.metric
format:json
from:-10m
until:now
noCache:1

HTTP/* 200
[Asserts]
jsonpath "$[0].target" == "some.test2.metric"
jsonpath "$[0].datapoints[*].[0]" count == 10
jsonpath "$[0].datapoints[*].[0]" includes 654321