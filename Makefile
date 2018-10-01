all: carbonapi carbonzipper

debug: debug_api debug_zipper

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
        EXTRA_PKG_CONFIG_PATH=/opt/X11/lib/pkgconfig
endif

VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)

GO ?= go

SOURCES=$(shell find . -name '*.go')

PKG_CARBONAPI=github.com/bookingcom/carbonapi/cmd/carbonapi
PKG_CARBONZIPPER=github.com/bookingcom/carbonapi/cmd/carbonzipper

carbonapi: $(SOURCES)
	PKG_CONFIG_PATH="$(EXTRA_PKG_CONFIG_PATH)" $(GO) build -tags cairo -ldflags '-X main.BuildVersion=$(VERSION)' $(PKG_CARBONAPI)

carbonzipper: $(SOURCES)
	$(GO) build --ldflags '-X main.BuildVersion=$(VERSION)' $(PKG_CARBONZIPPER)

debug_api: $(SOURCES)
	PKG_CONFIG_PATH="$(EXTRA_PKG_CONFIG_PATH)" $(GO) build -tags cairo -ldflags '-X main.BuildVersion=$(VERSION)' -gcflags=all='-l -N' $(PKG_CARBONAPI)

debug_zipper: $(SOURCES)
	PKG_CONFIG_PATH="$(EXTRA_PKG_CONFIG_PATH)" $(GO) build -ldflags '-X main.BuildVersion=$(VERSION)' -gcflags=all='-l -N' $(PKG_CARBONZIPPER)

nocairo: $(SOURCES)
	$(GO) build -ldflags '-X main.BuildVersion=$(VERSION)'

vet:
	go vet -composites=false ./...

test:
	PKG_CONFIG_PATH="$(EXTRA_PKG_CONFIG_PATH)" $(GO) test -tags cairo ./... -race -coverprofile cover.out

clean:
	rm -f carbonapi carbonzipper

authors:
	git log --format="%an" | sort | uniq > AUTHORS.txt
