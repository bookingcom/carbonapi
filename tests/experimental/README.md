# e2e test

This directory contains carbonapi test files for e2e test for the go-carbon -> zipper -> api stack.

Brings the system up, brings data, doing some render, find and expand calls.
Checks if data got in and out.

Needs to be started from Makefile in the root directory of the project.

Requires Docker, docker compose and [hurl](https://hurl.dev/).

Directory `hurl` contains test files for [hurl](https://hurl.dev/). 
See [Hurl manual](https://hurl.dev/docs/manual.html).

For testing and debugging jsonpath you can use [JSON Path Tester](https://codebeautify.org/jsonpath-tester).
