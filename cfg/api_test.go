package cfg

import (
	"strings"
	"testing"
	"time"
)

func TestParseAPIConfig(t *testing.T) {
	DEBUG = true

	var input = `
listen: ":8081"
backends:
    - "http://localhost:8000"

maxProcs: 16
timeouts:
    connect: "200ms"
    global: "600s"
    afterStarted: "600s"
concurrencyLimit: 1024
keepAliveInterval: "30s"
maxIdleConnsPerHost: 512
expireDelaySec: 0

buckets: 10
graphite:
    host: "localhost:3002"
    interval: "60s"
    prefix: "carbon.api"
    pattern: "{prefix}.{fqdn}"

maxBatchSize: 100
sendGlobsAsIs: true
cache:
   type: "memcache"
   size_mb: 0
   defaultTimeoutSec: 60
   memcachedServers:
       - host1:1234
       - host2:1234
tz: "UTC+1,3600"
pidFile: "/var/run/carbonapi/carbonapi.pid"
ignoreClientTimeout: true

logger:
    - logger: ""
      file: "/var/log/carbonapi/carbonapi.log"
      level: "info"
      encoding: "json"
`

	r := strings.NewReader(input)
	got, err := ParseAPIConfig(r)
	if err != nil {
		t.Fatal(err)
	}

	expected := API{
		Zipper: Zipper{
			Common: Common{
				Listen: ":8081",
				Backends: []string{
					"http://localhost:8000",
				},

				MaxProcs: 16,
				Timeouts: Timeouts{
					Global:       10 * time.Minute,
					AfterStarted: 10 * time.Minute,
					Connect:      200 * time.Millisecond,
				},
				ConcurrencyLimitPerServer: 1024,
				KeepAliveInterval:         30 * time.Second,
				MaxIdleConnsPerHost:       512,

				ExpireDelaySec:             0,
				GraphiteWeb09Compatibility: false,

				Buckets: 10,
				Graphite: GraphiteConfig{
					Pattern:  "{prefix}.{fqdn}",
					Host:     "localhost:3002",
					Interval: 60 * time.Second,
					Prefix:   "carbon.api",
				},
			},
		},

		ExtrapolateExperiment: false,
		SendGlobsAsIs:         true,
		AlwaysSendGlobsAsIs:   false,
		MaxBatchSize:          100,
		Cache: CacheConfig{
			Type: "memcache",
			Size: 0,
			MemcachedServers: []string{
				"host1:1234",
				"host2:1234",
			},
			DefaultTimeoutSec: 60,
		},
		TimezoneString: "UTC+1,3600",
		PidFile:        "/var/run/carbonapi/carbonapi.pid",

		IgnoreClientTimeout: true,
	}

	if !eqCommon(got.Common, expected.Common) {
		t.Fatalf("Didn't parse expected struct from config\nGot: %v\nExp: %v", got, expected)
	}
}

func TestParseUpstreamAPIConfig(t *testing.T) {
	DEBUG = true

	var input = `
listen: ":8081"
concurency: 1024
cache:
   type: "memcache"
   size_mb: 0
   defaultTimeoutSec: 60
   memcachedServers:
       - host1:1234
       - host2:1234
cpus: 16
tz: "UTC+1,3600"
sendGlobsAsIs: true
maxBatchSize: 100
ignoreClientTimeout: true
graphite:
    host: "localhost:3002"
    interval: "60s"
    prefix: "carbon.api"
    pattern: "{prefix}.{fqdn}"
idleConnections: 1024
pidFile: "/var/run/carbonapi/carbonapi.pid"
upstreams:
    buckets: 10
    timeouts:
        connect: "200ms"
        global: "600s"
        afterStarted: "600s"
    concurrencyLimit: 1024
    keepAliveInterval: "30s"
    maxIdleConnsPerHost: 1024
    backends:
        - "http://localhost:8000"
expireDelaySec: 0
logger:
    - logger: ""
      file: "/var/log/carbonapi/carbonapi.log"
      level: "info"
      encoding: "json"
`

	r := strings.NewReader(input)
	got, err := ParseAPIConfig(r)
	if err != nil {
		t.Fatal(err)
	}

	expected := API{
		Zipper: Zipper{
			Common: Common{
				Listen: ":8081",
				Backends: []string{
					"http://localhost:8000",
				},

				MaxProcs: 16,
				Timeouts: Timeouts{
					Global:       10 * time.Minute,
					AfterStarted: 10 * time.Minute,
					Connect:      200 * time.Millisecond,
				},
				ConcurrencyLimitPerServer: 1024,
				KeepAliveInterval:         30 * time.Second,
				MaxIdleConnsPerHost:       1024,

				ExpireDelaySec:             0,
				GraphiteWeb09Compatibility: false,

				Buckets: 10,
				Graphite: GraphiteConfig{
					Pattern:  "{prefix}.{fqdn}",
					Host:     "localhost:3002",
					Interval: 60 * time.Second,
					Prefix:   "carbon.api",
				},
			},
		},

		ExtrapolateExperiment: false,
		SendGlobsAsIs:         true,
		AlwaysSendGlobsAsIs:   false,
		MaxBatchSize:          100,
		Cache: CacheConfig{
			Type: "memcache",
			Size: 0,
			MemcachedServers: []string{
				"host1:1234",
				"host2:1234",
			},
			DefaultTimeoutSec: 60,
		},
		TimezoneString: "UTC+1,3600",
		PidFile:        "/var/run/carbonapi/carbonapi.pid",

		IgnoreClientTimeout: true,
	}

	if !eqCommon(got.Common, expected.Common) {
		t.Fatalf("Didn't parse expected struct from config\nGot: %v\nExp: %v", got, expected)
	}
}

func eqAPI(a, b API) bool {
	return eqCommon(a.Common, b.Common) &&
		toComparableAPI(a) == toComparableAPI(b) &&
		eqCacheConfig(a.Cache, b.Cache) &&
		eqStringSlice(a.UnicodeRangeTables, b.UnicodeRangeTables) &&
		eqMapStringString(a.DefaultColors, b.DefaultColors) &&
		eqMapStringString(a.FunctionsConfigs, b.FunctionsConfigs)
}

type comparableAPI struct {
	ExtrapolateExperiment bool
	SendGlobsAsIs         bool
	AlwaysSendGlobsAsIs   bool
	MaxBatchSize          int
	TimezoneString        string
	PidFile               string
	IgnoreClientTimeout   bool
}

func toComparableAPI(a API) comparableAPI {
	return comparableAPI{
		ExtrapolateExperiment: a.ExtrapolateExperiment,
		SendGlobsAsIs:         a.SendGlobsAsIs,
		AlwaysSendGlobsAsIs:   a.AlwaysSendGlobsAsIs,
		MaxBatchSize:          a.MaxBatchSize,
		TimezoneString:        a.TimezoneString,
		PidFile:               a.PidFile,
		IgnoreClientTimeout:   a.IgnoreClientTimeout,
	}
}

func eqCacheConfig(a, b CacheConfig) bool {
	return a.Type == b.Type &&
		a.Size == b.Size &&
		eqStringSlice(a.MemcachedServers, b.MemcachedServers) &&
		a.DefaultTimeoutSec == b.DefaultTimeoutSec
}

func eqStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func eqMapStringString(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, va := range a {
		vb, ok := b[k]
		if !ok || va != vb {
			return false
		}
	}

	return true
}
