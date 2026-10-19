# Frontend CPU and memory tuning

The standard `make all` build includes Cairo and the native zlib gzip decoder.
Install the Cairo and zlib development libraries and enable cgo. `make test`
tests those same build tags. `make nocairo` retains the portable build with
standard-library gzip; builds without cgo or without the `nativegzip` tag also
use standard-library gzip.

Backend queues allocate pointer storage for their current occupancy rather than
the configured maximum. Expression evaluation borrows read-only aligned inputs
and transfers owned output storage only when the caller supports that contract.

These backend fast paths are opt-in at process startup:

```sh
export CARBONAPI_SYNC_BACKEND_HTTP=1
export CARBONAPI_STREAM_RENDER=1
export CARBONAPI_NATIVE_GZIP=1
export CARBONAPI_PACK_BACKEND_POINTS=1
```

The synchronous HTTP path handles plain GET requests directly; HTTPS, proxies
and other methods retain the standard transport. Streaming render decoding uses
a bounded reader and optionally native gzip. Adaptive packed point storage
preserves float64 bits and absence flags, falls back to dense storage when
packing would not save enough space, and materializes the selected replica
before expression evaluation. Public render results retain dense point slices.

An initial production canary used these runtime settings:

```sh
export GOMAXPROCS=8
export GOGC=50
export GOMEMLIMIT=1280MiB
```

Set **both** `cpus: 8` in the frontend configuration and `maxProcs: 8` in the
embedded zipper configuration: either configuration can override the environment.
The same canary used frontend `resolveGlobs: 0` and `renderBatchSize: 1`, allowing
backend glob resolution without opportunistic render batching. These are measured
workload settings, not universal sizing defaults. The Go memory limit is a soft
runtime target, not an RSS or operating-system memory limit.

Stable literal batching is a separate experimental option:
`CARBONAPI_STABLE_RENDER_BATCH=1` together with `renderBatchSize` greater than one.
It sorts literal targets into deterministic groups while preserving each target's
backend routing, error handling and logical concurrency weight. Glob targets
remain individual requests. The production canary kept `renderBatchSize: 1`, so
this grouping path was inactive; batches of 16 and 64 were tested in frozen
replay only. Keep it disabled in the initial rollout.

## Validation and rollout

Compare exact outputs on frozen backend responses, including missing values,
float bit patterns, replica disagreement and malformed protobuf/gzip. Measure
CPU per render, RSS and latency distributions after cache warm-up, with unchanged
control instances and without profiling during the measured windows.

The combined code and settings were measured on one production instance over
15-minute candidate and restored-reference windows, about 76,000 renders total:
CPU per render fell 66%, median RSS 77%, mean latency 45% and p99 latency 46%,
with zero HTTP 5xx in either window. The CPU savings 95% block-bootstrap interval
was 44–79%; the earlier five-minute comparison showed about 48% adjusted savings.
The workload mix differed, matched latency classes covered only about 5% of
requests, and reference RSS had a late transient spike. Median RSS avoids that
spike dominating the reported memory result. These observations do not establish
a fleet-wide 50% CPU floor or isolate the effect of each individual setting.
The reference was the deployed binary; its exact revision was unavailable in
public refs, so it was not a build of this branch's public source base.

The full native race suite, wire-format differential checks, decoder fuzzing,
and 25 zero-miss frozen-replay runs passed. Restore the previous binary and
configuration together if errors or latency regress. The canary was restored to
the reference binary and configuration after measurement.
