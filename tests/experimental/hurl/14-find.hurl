# test find w/pickle format
GET http://localhost:8081/metrics/find
[QueryStringParams]
query:some.*
noCache:1
from:-10m
until:now
format:pickle

HTTP/* 200
Content-Type: application/pickle
[Asserts]
bytes startsWith hex,5d287d2855;