FROM golang:1.19

RUN apt-get update
RUN apt-get install -y libcairo2-dev

WORKDIR /go/src/github.com/bookingcom/carbonapi
COPY . .
COPY ./docker/carbonapi/carbonapi.yaml /etc/carbonapi.yaml
COPY ./docker/carbonapi/carbonzipper.yaml /etc/carbonzipper.yaml

RUN make build

EXPOSE 7081
EXPOSE 8081

ENTRYPOINT [ "/go/src/github.com/bookingcom/carbonapi/carbonapi",  "-config", "config/carbonapi.yaml" ]
