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
    timeInQueueExpHistogram:
        start: 0.3
        bucketsNum: 30
        bucketSize: 3
    timeInQueueLinHistogram:
        start: 0.0
        bucketsNum: 33
        bucketSize: 0.3
    requestDurationExpHistogram:
        start: 0.05
        bucketsNum: 30
        bucketSize: 3.0
    requestDurationLinHistogram:
        start: 0.0
        bucketsNum: 30
        bucketSize: 0.03
    renderDurationExpHistogram:
        start: 0.03
        bucketsNum: 30
        bucketSize: 6
    findDurationExpHistogram:
        start: 0.06
        bucketsNum: 60
        bucketSize: 3

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
		Limits: Limits{
			MaxSize:     0,
			MaxDuration: 0,
		},
		ConcurrencyLimitPerServer: 2048,
		KeepAliveInterval:         30 * time.Second,
		MaxIdleConnsPerHost:       1024,

		ExpireDelaySec:             600,
		GraphiteWeb09Compatibility: true,

		Buckets: 10,
		Graphite: GraphiteConfig{
			Pattern:  "{prefix}.{fqdn}",
			Host:     "",
			Interval: 60 * time.Second,
			Prefix:   "carbon.zipper",
		},
		Monitoring: MonitoringConfig{
			TimeInQueueExpHistogram: HistogramConfig{
				Start:      0.3,
				BucketsNum: 30,
				BucketSize: 3,
			},
			TimeInQueueLinHistogram: HistogramConfig{
				Start:      0.0,
				BucketsNum: 33,
				BucketSize: 0.3,
			},
			RequestDurationExp: HistogramConfig{
				Start:      0.05,
				BucketSize: 3,
				BucketsNum: 30,
			},
			RequestDurationLin: HistogramConfig{
				Start:      0.0,
				BucketSize: 0.03,
				BucketsNum: 30,
			},
			RenderDurationExp: HistogramConfig{
				Start:      0.03,
				BucketsNum: 30,
				BucketSize: 6,
			},
			FindDurationExp: HistogramConfig{
				Start:      0.06,
				BucketsNum: 60,
				BucketSize: 3,
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
	Limits                     Limits
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
		Limits:                     a.Limits,
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
