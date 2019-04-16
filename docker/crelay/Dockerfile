FROM ubuntu:18.04

COPY ./docker/crelay/relay /usr/local/bin/relay
COPY ./docker/crelay/relay.conf /etc/relay.conf

EXPOSE 2003

ENTRYPOINT [ "/usr/local/bin/relay", "-f", "/etc/relay.conf"]