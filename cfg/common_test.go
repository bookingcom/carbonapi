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
    - "http://10.190.191.9:8080"
loggerConfig:
   outputPaths: ["/var/log/carbonzipper/carbonzipper.log"]
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
    findDurationLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
    findDurationSimpleLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
    findDurationComplexLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
traces:
     jaegerEndpoint: "http://abc:8080"
     tags:
       a: 1
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
			"http://10.190.191.9:8080",
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
			FindDurationLin: HistogramConfig{
				Start:      0.1,
				BucketsNum: 20,
				BucketSize: 1,
			},
			FindDurationLinSimple: HistogramConfig{
				Start:      0.1,
				BucketSize: 1,
				BucketsNum: 20,
			},
			FindDurationLinComplex: HistogramConfig{
				Start:      0.1,
				BucketSize: 1,
				BucketsNum: 20,
			},
		},
	}

	var backendSize = len(expected.GetBackends())
	var expectedSize = 3
	if backendSize != expectedSize {
		t.Fatalf("Received wrong number of backends: \nExpected %v but returned %v", expectedSize, backendSize)
	}

	if !eqCommon(got, expected) {
		t.Fatalf("Didn't parse expected struct from config\nGot: %v\nExp: %v", got, expected)
	}
}

func TestParseCommonCluster(t *testing.T) {
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
backendsByCluster:
    - name: "cluster1"
      backends:
      - "http://10.190.202.31:8080"
      - "http://10.190.197.91:8080"
    - name: "cluster2"
      backends:
      - "http://10.190.202.32:8080"
      - "http://10.190.197.92:8080"
loggerConfig:
   outputPaths: ["/var/log/carbonzipper/carbonzipper.log"]
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
    findDurationLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
    findDurationSimpleLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
    findDurationComplexLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
`

	r := strings.NewReader(input)
	got, err := ParseCommon(r)
	if err != nil {
		t.Fatal(err)
	}

	expected := Common{
		Listen: ":8000",
		BackendsByCluster: []Cluster{
			{
				Name: "cluster1",
				Backends: []string{
					"http://10.190.202.31:8080",
					"http://10.190.197.91:8080",
				},
			},
			{
				Name: "cluster2",
				Backends: []string{
					"http://10.190.202.32:8080",
					"http://10.190.197.92:8080",
				},
			},
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
			FindDurationLin: HistogramConfig{
				Start:      0.1,
				BucketsNum: 20,
				BucketSize: 1,
			},
			FindDurationLinSimple: HistogramConfig{
				Start:      0.1,
				BucketSize: 1,
				BucketsNum: 20,
			},
			FindDurationLinComplex: HistogramConfig{
				Start:      0.1,
				BucketSize: 1,
				BucketsNum: 20,
			},
		},
	}

	backendSize := len(expected.GetBackends())
	expectedSize := 4
	if backendSize != expectedSize {
		t.Fatalf("Received wrong number of backends: \nExpected %v but returned %v", expectedSize, backendSize)
	}

	_, cluster, err := expected.InfoOfBackend("http://10.190.202.32:8080")
	if err != nil {
		t.Fatal(err)
	}
	expectedCluster := "cluster2"
	if cluster != expectedCluster {
		t.Fatalf("Problem in getting cluster of a backend: \nExpected %v but returned %v", expectedCluster, cluster)
	}

	if !eqCommon(got, expected) {
		t.Fatalf("Didn't parse expected struct from config\nGot: %v\nExp: %v", got, expected)
	}
}

func TestParseCommonDC(t *testing.T) {
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
backendsByDC:
    - name: "dc1"
      clusters:
          - name: "cluster1"
            backends:
            - "http://10.190.202.31:8080"
            - "http://10.190.197.91:8080"
          - name: "cluster2"
            backends:
            - "http://10.190.202.32:8080"
            - "http://10.190.197.92:8080"
    - name: "dc2"
      clusters:
          - name: "cluster1"
            backends:
            - "http://10.290.202.31:8080"
            - "http://10.290.197.91:8080"
          - name: "cluster2"
            backends:
            - "http://10.290.202.32:8080"
loggerConfig:
   outputPaths: ["/var/log/carbonzipper/carbonzipper.log"]
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
    findDurationLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
    findDurationSimpleLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
    findDurationComplexLinHistogram:
        start: 0.1
        bucketsNum: 20
        bucketSize: 1
`

	r := strings.NewReader(input)
	got, err := ParseCommon(r)
	if err != nil {
		t.Fatal(err)
	}

	expected := Common{
		Listen: ":8000",
		BackendsByDC: []DC{
			{
				Name: "dc1",
				Clusters: []Cluster{
					{
						Name: "cluster1",
						Backends: []string{
							"http://10.190.202.31:8080",
							"http://10.190.197.91:8080",
						},
					},
					{
						Name: "cluster2",
						Backends: []string{
							"http://10.190.202.32:8080",
							"http://10.190.197.92:8080",
						},
					},
				},
			},
			{
				Name: "dc2",
				Clusters: []Cluster{
					{
						Name: "cluster1",
						Backends: []string{
							"http://10.290.202.31:8080",
							"http://10.290.197.91:8080",
						},
					},
					{
						Name: "cluster2",
						Backends: []string{
							"http://10.290.202.32:8080",
						},
					},
				},
			},
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
			FindDurationLin: HistogramConfig{
				Start:      0.1,
				BucketsNum: 20,
				BucketSize: 1,
			},
			FindDurationLinSimple: HistogramConfig{
				Start:      0.1,
				BucketSize: 1,
				BucketsNum: 20,
			},
			FindDurationLinComplex: HistogramConfig{
				Start:      0.1,
				BucketSize: 1,
				BucketsNum: 20,
			},
		},
	}

	backendSize := len(expected.GetBackends())
	expectedSize := 7
	if backendSize != expectedSize {
		t.Fatalf("Received wrong number of backends: \nExpected %v but returned %v", expectedSize, backendSize)
	}

	dc, cluster, err := expected.InfoOfBackend("http://10.290.202.32:8080")
	if err != nil {
		t.Fatal(err)
	}
	expectedCluster := "cluster2"
	expectedDC := "dc2"
	if cluster != expectedCluster {
		t.Fatalf("Problem in getting cluster of a backend: \nExpected %v but returned %v", expectedCluster, cluster)
	}
	if dc != expectedDC {
		t.Fatalf("Problem in getting dc of a backend: \nExpected %v but returned %v", expectedDC, dc)
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
		eqStringSlice(a.GetBackends(), b.GetBackends())
}
