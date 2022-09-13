UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	PKGCONF = PKG_CONFIG_PATH="/opt/X11/lib/pkgconfig"
else
	PKGCONF =
endif

VERSION ?= $(shell git rev-parse --short HEAD)

PKG_CARBONAPI=github.com/bookingcom/carbonapi/cmd/carbonapi
PKG_CARBONZIPPER=github.com/bookingcom/carbonapi/cmd/carbonzipper

GCFLAGS :=
debug: GCFLAGS += -gcflags=all='-l -N'

LDFLAGS = -ldflags '-X main.BuildVersion=$(VERSION)'

TAGS := -tags cairo
nocairo: TAGS =

### Targets ###

.PHONY: all
all: build

.PHONY: nocairo
nocairo: build

.PHONY: debug
debug: build

.PHONY: build
build:
	$(PKGCONF) go build $(TAGS) $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONAPI)
	$(PKGCONF) go build $(TAGS) $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONZIPPER)

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	$(PKGCONF) go test ./... -race

.PHONY: test-e2e
test-e2e:
	./tests/system_test.sh

.PHONY: clean
clean:
	rm -f carbonapi carbonzipper

.PHONY: authors
authors:
	git log --format="%an" | sort | uniq > AUTHORS.txt
