package cfg

import (
	"strings"
	"testing"
	"time"
)

func TestParseCommon(t *testing.T) {
	DEBUG = true

	// TODO (grzkv): Move out to support proper indent with spaces
	var input = `
listen: ":8000"
maxProcs: 32
concurrencyLimit: 2048
maxIdleConnsPerHost: 1024
timeouts:
    global: "20s"
    afterStarted: "15s"
graphite09compat: true
backends:
        - "http://10.190.202.30:8080"
        - "http://10.190.197.9:8080"
logger:
    -
       logger: ""
       file: "/var/log/carbonzipper/carbonzipper.log"
       level: "info"
       encoding: "json"
monitoring:
    timeInQueueHistogram:
        start: 0.0
        bucketsNum: 50
        bucketSize: 0.1
    requestDurationExpHistogram:
        start: 0.05
        bucketsNum: 20
        bucketSize: 2.0
    requestDurationLinHistogram:
        start: 0.0
        bucketsNum: 40
        bucketSize: 0.05

`

	r := strings.NewReader(input)
	got, err := ParseCommon(r)
	if err != nil {
		t.Fatal(err)
	}

	expected := Common{
		Listen: ":8000",
		Backends: []string{
			"http://10.190.202.30:8080",
			"http://10.190.197.9:8080",
		},

		MaxProcs: 32,
		Timeouts: Timeouts{
			Global:       20 * time.Second,
			AfterStarted: 15 * time.Second,
			Connect:      200 * time.Millisecond,
		},
		ConcurrencyLimitPerServer: 2048,
		KeepAliveInterval:         30 * time.Second,
		MaxIdleConnsPerHost:       1024,

		ExpireDelaySec:             600,
		GraphiteWeb09Compatibility: true,

		Buckets: 10,
		Graphite: GraphiteConfig{
			Pattern:  "{prefix}.{fqdn}",
			Host:     "127.0.0.1:3002",
			Interval: 60 * time.Second,
			Prefix:   "carbon.zipper",
		},
		Monitoring: MonitoringConfig{
			TimeInQueueHistogram: HistogramConfig{
				Start:      0.0,
				BucketsNum: 50,
				BucketSize: 0.1,
			},
			RequestDurationExp: HistogramConfig{
				Start:      0.05,
				BucketSize: 2,
				BucketsNum: 20,
			},
			RequestDurationLin: HistogramConfig{
				Start:      0.0,
				BucketSize: 0.05,
				BucketsNum: 40,
			},
		},
	}

	if !eqCommon(got, expected) {
		t.Fatalf("Didn't parse expected struct from config\nGot: %v\nExp: %v", got, expected)
	}
}

type comparableCommon struct {
	Listen                     string
	MaxProcs                   int
	Timeouts                   Timeouts
	ConcurrencyLimitPerServer  int
	KeepAliveInterval          time.Duration
	MaxIdleConnsPerHost        int
	ExpireDelaySec             int32
	GraphiteWeb09Compatibility bool
	Buckets                    int
	Graphite                   GraphiteConfig
}

func toComparableCommon(a Common) comparableCommon {
	return comparableCommon{
		Listen:                     a.Listen,
		MaxProcs:                   a.MaxProcs,
		Timeouts:                   a.Timeouts,
		ConcurrencyLimitPerServer:  a.ConcurrencyLimitPerServer,
		KeepAliveInterval:          a.KeepAliveInterval,
		MaxIdleConnsPerHost:        a.MaxIdleConnsPerHost,
		ExpireDelaySec:             a.ExpireDelaySec,
		GraphiteWeb09Compatibility: a.GraphiteWeb09Compatibility,
		Buckets:                    a.Buckets,
		Graphite:                   a.Graphite,
	}
}

func eqCommon(a, b Common) bool {
	return toComparableCommon(a) == toComparableCommon(b) &&
		eqStringSlice(a.Backends, b.Backends)
}
