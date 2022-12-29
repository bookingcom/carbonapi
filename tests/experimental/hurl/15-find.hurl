# testing find w/protobuf format
GET http://localhost:8081/metrics/find
[QueryStringParams]
query:some.*
noCache:1
from:-10m
until:now
format:protobuf

HTTP/* 200
Content-Type: application/x-protobuf
[Asserts]
bytes startsWith hex,0a06736f6d652e;