FROM golang:1.11

RUN mkdir -p /data/graphite/whisper/
COPY ./docker/go-carbon/*.conf /etc/

RUN go get github.com/lomik/go-carbon
RUN go install github.com/lomik/go-carbon

EXPOSE 2003 2004 7002 7007 2003/udp

CMD ["/go/bin/go-carbon", "-config", "/etc/go-carbon.conf"]