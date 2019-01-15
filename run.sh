#!/usr/bin/env bash

./carbonapi -config config/carbonapi.yaml > carbonapi.log & ./carbonzipper -config config/carbonzipper.conf > carbonzipper.log && kill $!