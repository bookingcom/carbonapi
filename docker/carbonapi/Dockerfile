FROM golang:1.11

RUN apt-get update
RUN apt-get install -y libcairo2-dev

WORKDIR /go/src/github.com/bookingcom/carbonapi
COPY . .
COPY ./config/carbonapi.yaml /etc/carbonapi.yaml

RUN make build

EXPOSE 7081
EXPOSE 8081

ENTRYPOINT [ "/go/src/github.com/bookingcom/carbonapi/carbonapi",  "-config", "config/carbonapi.yaml" ]