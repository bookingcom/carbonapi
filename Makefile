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
GCFLAGS :=
debug: GCFLAGS += -gcflags=all='-l -N'

LDFLAGS = -ldflags '-X main.BuildVersion=$(VERSION)'

# Targets
all: build

.PHONY: debug
debug: build

build:
	$(PKGCONF) $(GO) build $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONAPI)
	$(PKGCONF) $(GO) build $(LDFLAGS) $(GCFLAGS) $(PKG_CARBONZIPPER)

vet:
	go vet -composites=false ./...

lint:
	gometalinter --vendor --deadline=150s --cyclo-over=15 --exclude="\bexported \w+ (\S*['.]*)([a-zA-Z'.*]*) should have comment or be unexported\b" ./...

test:
	$(PKGCONF) $(GO) test ./... -race -coverprofile cover.out

clean:
	rm -f carbonapi carbonzipper

authors:
	git log --format="%an" | sort | uniq > AUTHORS.txt
