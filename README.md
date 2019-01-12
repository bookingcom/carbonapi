# Carbonapi

[![Build Status](https://travis-ci.com/bookingcom/carbonapi.svg?branch=master)](https://travis-ci.com/bookingcom/carbonapi)
[![Go Report Card](https://goreportcard.com/badge/github.com/bookingcom/carbonapi)](https://goreportcard.com/report/github.com/bookingcom/carbonapi)

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

We do not provide packages for install at this time. Contact us if you're
interested in those.


## Requirements

We recommend using at least version 1.10 of Go. Booking.com builds its binaries
with the latest stable release of Go at any time. The binaries likely compile
on older versions of Go (at least 1.9), but we don't test the build against
them because of problems with computing test coverage of the whole project.

At the moment, we only guarantee that Carbonapi can talk to the
[go-carbon](https://github.com/go-graphite/go-carbon)
Graphite store. We are interested in supporting other stores.


## OSX Build Notes

Some additional steps may be needed to build carbonapi with cairo rendering on
MacOSX.

Install cairo:

```
brew install Caskroom/cask/xquartz

brew install cairo --with-x11
```


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
