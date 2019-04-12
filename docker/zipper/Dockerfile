FROM golang:1.12

RUN apt-get update
RUN apt-get install -y libcairo2-dev

WORKDIR /go/src/github.com/bookingcom/carbonapi
COPY . .

RUN make build

EXPOSE 7000
EXPOSE 8000

ENTRYPOINT [ "/go/src/github.com/bookingcom/carbonapi/carbonzipper",  "-config", "/etc/carbonzipper.yaml" ]