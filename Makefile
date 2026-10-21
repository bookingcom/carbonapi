UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	PKGCONF = PKG_CONFIG_PATH="/opt/X11/lib/pkgconfig"
else
	PKGCONF =
endif

VERSION ?= $(shell git rev-parse --short HEAD)

PKG_CARBONAPI=github.com/bookingcom/carbonapi/cmd/carbonapi

GCFLAGS :=
debug: GCFLAGS += -gcflags=all='-l -N'

LDFLAGS = -ldflags '-X main.BuildVersion=$(VERSION)'

TAGS := -tags cairo,nativegzip,zlibng
nocairo: TAGS =

### Targets ###

.PHONY: all
all: build

.PHONY: nocairo
nocairo: build

.PHONY: debug
debug: build

.PHONY: build
build: native-deps
	$(PKGCONF) go build $(TAGS) $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONAPI)

.PHONY: check
check: test lint

.PHONY: lint
lint:
	golangci-lint --version
	GOMEMLIMIT=1024MiB golangci-lint run -v

.PHONY: test
test: native-deps
	$(PKGCONF) go test $(TAGS) -timeout 60s -race ./...
	@case '$(TAGS)' in *zlibng*) \
		go test -tags nativegzip -timeout 60s -race ./pkg/backend/net && \
		go test -timeout 60s -race ./pkg/backend/net ;; esac

.PHONY: native-deps
native-deps:
	@case '$(TAGS)' in *zlibng*) ./scripts/build-zlib-ng.sh ;; esac

.PHONY: test-e2e
test-e2e:
	./tests/system_test.sh

.PHONY: test-hurl
test-hurl:
	./tests/experimental/hurl_test.sh

.PHONY: clean
clean:
	rm -f carbonapi carbonzipper
	rm -rf .build/zlib-ng

.PHONY: authors
authors:
	git log --format="%an" | sort | uniq > AUTHORS.txt
