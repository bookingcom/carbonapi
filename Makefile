all: carbonzipper

VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)

GO ?= go

SOURCES=$(shell find . -name '*.go')
PKG_CARBONZIPPER=github.com/go-graphite/carbonzipper/cmd/carbonzipper

carbonzipper: $(SOURCES)
	$(GO) build --ldflags '-X main.BuildVersion=$(VERSION)' $(PKG_CARBONZIPPER)

test:
	$(GO) test -race
	$(GO) vet

clean:
	rm -f carbonzipper
