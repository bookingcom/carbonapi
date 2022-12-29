# test find w/json format
# i.e. [{"allowChildren":1,"context":{},"expandable":1,"id":"some.test","leaf":0,"text":"test"},{"allowChildren":1,"context":{},"expandable":1,"id":"some.test2","leaf":0,"text":"test2"}]
GET http://localhost:8081/metrics/find
[QueryStringParams]
query:some.*
format:json
from:-10m
until:now
noCache:1

HTTP/* 200
Content-Type: application/json
[Asserts]
jsonpath "$[*]" count == 2
jsonpath "$[0].text" == "test"
jsonpath "$[0].leaf" == 0
jsonpath "$[0].expandable" == 1
jsonpath "$[0].allowChildren" == 1
jsonpath "$[1].text" == "test2"
jsonpath "$[1].leaf" == 0
jsonpath "$[1].expandable" == 1
jsonpath "$[0].allowChildren" == 1