FROM golang:1.18

RUN mkdir -p /data/graphite/whisper/
COPY ./docker/go-carbon/*.conf /etc/

RUN go install github.com/go-graphite/go-carbon@latest

EXPOSE 2003 2004 7002 7007 2003/udp

CMD ["/go/bin/go-carbon", "-config", "/etc/go-carbon.conf"]
