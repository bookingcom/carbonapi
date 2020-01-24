# TODO (rgrytskiv): is PKGCONF used?
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	PKGCONF = PKG_CONFIG_PATH="/opt/X11/lib/pkgconfig"
else
	PKGCONF =
endif

GO ?= go
VERSION ?= $(shell git rev-parse --short HEAD)

# Binaries
PKG_CARBONAPI=github.com/bookingcom/carbonapi/cmd/carbonapi
PKG_CARBONZIPPER=github.com/bookingcom/carbonapi/cmd/carbonzipper

# Flags
export GO111MODULE=on
GCFLAGS := -mod=vendor
debug: GCFLAGS += -gcflags=all='-l -N'

LDFLAGS = -ldflags '-X main.BuildVersion=$(VERSION)'

TAGS := -tags cairo
nocairo: TAGS =

# Targets
all: build

nocairo: build

.PHONY: debug
debug: build

build:
	$(PKGCONF) $(GO) build $(TAGS) $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONAPI)
	$(PKGCONF) $(GO) build $(TAGS) $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONZIPPER)

vet:
	go vet -mod=vendor -composites=false ./...

lint:
# Show only issues introduced since switching from gometalinter to
# golangci-lint.  The commit b5dd153 was the merge-base on master at the time.
# This is not in .golangci.yml in order to show all the gore when run directly.
	golangci-lint run --new-from-rev 9ce419bc428b76f1505230e501546e2245374c94

lint-all:
	golangci-lint run

check: test vet

test:
	$(PKGCONF) $(GO) test ./... -mod=vendor -race -coverprofile=coverage.txt -covermode=atomic

clean:
	rm -f carbonapi carbonzipper

authors:
	git log --format="%an" | sort | uniq > AUTHORS.txt
