# Carbonapi

[![Build Status](https://travis-ci.com/bookingcom/carbonapi.svg?branch=master)](https://travis-ci.com/bookingcom/carbonapi)
[![Go Report Card](https://goreportcard.com/badge/github.com/bookingcom/carbonapi)](https://goreportcard.com/report/github.com/bookingcom/carbonapi)
[![codecov](https://codecov.io/gh/bookingcom/carbonapi/branch/master/graph/badge.svg)](https://codecov.io/gh/bookingcom/carbonapi)
[![gitter](https://img.shields.io/badge/chat-on%20gitter-green.svg)](https://gitter.im/carbonapi/community)

Carbonapi is a Go-based Graphite frontend. It provides two binaries,
`carbonapi` and `carbonzipper`, that unify responses from multiple Graphite
backends and provide math and graphing functions.

This project is run in production at Booking.com. We are in the process of
documenting its installation and setup, but can answer any questions that
interested persons have.

CarbonAPI supports a significant subset of graphite functions; see
[COMPATIBILITY](COMPATIBILITY.md). In our testing it has shown to be 5x-10x
faster than requesting data from graphite-web.

## Build

To build both the `carbonapi` and `carbonzipper` binaries, run:

```
make
```

To build the binaries with debug symbols, run:

```
make debug
```

Note: build process requires pkg-config to be installed:

### Mac OS X

```
curl https://pkgconfig.freedesktop.org/releases/pkg-config-0.29.tar.gz -o pkgconfig.tgz
tar -zxf pkgconfig.tgz && cd pkg-config-0.29
```
There is a circular dependency between pkg-config and glib. To break it, pkg-config includes a version of glib, which is enough to break the dependency cycle and compile it with  --with-internal-glib key:

```
env LDFLAGS="-framework CoreFoundation -framework Carbon" ./configure --with-internal-glib && make install
```

We do not provide packages for install at this time. Contact us if you're
interested in those.

## Run

Run the full stack `carbonapi` -> `zipper` -> `go-carbon` with:

```
docker-compose up
```

You can feed in sample data with:

```
echo "test.test 5 `date +%s`" | nc -c localhost 2003
```

and get it back with:

```
curl 'http://localhost:8081/render?target=test.test&format=json&from=-10m'
```

## Requirements

We recommend using at least version 1.10 of Go. Booking.com builds its binaries
with the latest stable release of Go at any time. The binaries likely compile
on older versions of Go (at least 1.9), but we don't test the build against
them because of problems with computing test coverage of the whole project.

At the moment, we only guarantee that Carbonapi can talk to the
[go-carbon](https://github.com/go-graphite/go-carbon)
Graphite store. We are interested in supporting other stores.

### Cairo support

The current supported `cairo` version is `v1.14.6`.


## OSX Build Notes

Some additional steps may be needed to build carbonapi with cairo rendering on
MacOSX.

Install cairo:

```
brew install Caskroom/cask/xquartz

brew install cairo
```

Xquartz is a required dependency for cairo.

## Clickhouse support

The pair `carbonapi` and `carbonzipper` works with [Clickhouse](https://clickhouse.yandex) via [graphite-clickhouse](https://github.com/lomik/graphite-clickhouse).

The access chain then looks like this:

`carbonapi` -> `zipper` -> `graphite-clickhouse` -> `clickhouse`

(this presumes the data was written into Clickhouse with [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)).

Run

```
docker-compose --file docker-compose-clickhouse.yaml up
```

to get the setup up and running in several Docker containers.

## Acknowledgement and history

This program was originally developed for Booking.com. With approval
from Booking.com, the code was generalised and published as Open Source
on GitHub, for which the author would like to express his gratitude.

This is Booking.com's fork of
[go-graphite/carbonapi](https://github.com/go-graphite/carbonapi).
That project's current performance characteristics are not sufficient for our
production needs, and we decided it had moved too far ahead for us to be able
to improve them effectively. We thus reverted back to versions 0.9.2 of
carbonapi and 0.74 of carbonzipper, and are moving more slowly in the same
direction as the original project.


## License

This code is licensed under the BSD-2 license.
