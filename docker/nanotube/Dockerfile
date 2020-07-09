FROM golang:1.14

RUN go get github.com/bookingcom/nanotube/cmd/nanotube
RUN go install github.com/bookingcom/nanotube/cmd/nanotube
COPY ./docker/nanotube/config/ /etc/nanotube/config

EXPOSE 2003

ENTRYPOINT [ "/go/bin/nanotube", "-config", "/etc/nanotube/config/config.toml","-clusters", "/etc/nanotube/config/clusters.toml","-rules", "/etc/nanotube/config/rules.toml"]
