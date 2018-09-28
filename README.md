# Carbonapi


Carbonapi is a Go-based Graphite frontend. It provides two binaries,
`carbonapi` and `carbonzipper`, that unify responses from multiple Graphite
backends and provide math and graphing functions.

This project is run in production at Booking.com. We are in the process of
documenting its installation and setup, but can answer any questions that
interested persons have.


## Build

To build both the `carbonapi` and `carbonzipper` binaries, run:
```
$ make
```
To build the binaries with debug symbols, run:
```
$ make debug
```
We do not provide packages for install at this time. Contact us if you're
interested in those.


## Requirements

At least version 1.8 of Go. Booking.com builds its binaries with the latest
stable release of Go at any time.


## OSX Build Notes

Some additional steps may be needed to build carbonapi with cairo rendering on
MacOSX.

Install cairo:

```
$ brew install Caskroom/cask/xquartz

$ brew install cairo --with-x11
```


## Acknowledgement

This program was originally developed for Booking.com.  With approval
from Booking.com, the code was generalised and published as Open Source
on GitHub, for which the author would like to express his gratitude.


## License

This code is licensed under the BSD-2 license.
