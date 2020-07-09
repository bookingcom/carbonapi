package cfg

import (
	"io"
	"log"
	"time"

	"github.com/lomik/zapwriter"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// TODO (grzkv): Remove from global scope. Probably should be replaced with flags
var DEBUG bool = false

// TODO (grzkv): This type of config does not makes sense, since there is no such entity as graphite

// GraphiteConfig does not makes real sense
type GraphiteConfig struct {
	Pattern  string
	Host     string
	Interval time.Duration
	Prefix   string
}

// ParseCommon sets the default config, parses input one, and overrides the defaults
func ParseCommon(r io.Reader) (Common, error) {
	d := yaml.NewDecoder(r)
	d.SetStrict(DEBUG)

	// set the default config
	c := DefaultCommonConfig()

	err := d.Decode(&c)

	return c, err
}

// DefaultCommonConfig gives the default config shared by carbonapi and zipper
func DefaultCommonConfig() Common {
	return Common{
		Listen:         ":8080",
		ListenInternal: ":7080",

		MaxProcs: 1,
		Timeouts: Timeouts{
			Global:       10000 * time.Millisecond,
			AfterStarted: 2 * time.Second,
			Connect:      200 * time.Millisecond,
		},
		ConcurrencyLimitPerServer: 20,
		KeepAliveInterval:         30 * time.Second,
		MaxIdleConnsPerHost:       100,

		ExpireDelaySec:       int32(10 * time.Minute / time.Second),
		InternalRoutingCache: int32(5 * time.Minute / time.Second),

		Buckets: 10,
		Graphite: GraphiteConfig{
			Interval: 60 * time.Second,
			Host:     "",
			Prefix:   "carbon.zipper",
			Pattern:  "{prefix}.{fqdn}",
		},
		Logger: []zapwriter.Config{GetDefaultLoggerConfig()},
		Monitoring: MonitoringConfig{
			TimeInQueueExpHistogram: HistogramConfig{
				Start:      0.01,
				BucketsNum: 25,
				BucketSize: 2,
			},
			TimeInQueueLinHistogram: HistogramConfig{
				Start:      0.05,
				BucketsNum: 25,
				BucketSize: 0.02,
			},
			RequestDurationExp: HistogramConfig{
				Start:      0.05,
				BucketSize: 2,
				BucketsNum: 20,
			},
			RequestDurationLin: HistogramConfig{
				Start:      0.05,
				BucketSize: 0.05,
				BucketsNum: 40,
			},
			RenderDurationExp: HistogramConfig{
				Start:      0.05,
				BucketSize: 2,
				BucketsNum: 20,
			},
			RenderDurationLinSimple: HistogramConfig{
				Start:      0.1,
				BucketSize: 0.1,
				BucketsNum: 30,
			},
			FindDurationExp: HistogramConfig{
				Start:      0.05,
				BucketSize: 2,
				BucketsNum: 20,
			},
			FindDurationLin: HistogramConfig{
				Start:      0.5,
				BucketSize: 0.5,
				BucketsNum: 20,
			},
			FindDurationLinSimple: HistogramConfig{
				Start:      0.5,
				BucketSize: 0.5,
				BucketsNum: 20,
			},
			FindDurationLinComplex: HistogramConfig{
				Start:      0.5,
				BucketSize: 0.5,
				BucketsNum: 20,
			},
		},
		Traces: Traces{
			Timeout: 10 * time.Second,
		},
	}
}

// GetDefaultLoggerConfig returns sane default for the logger conf
func GetDefaultLoggerConfig() zapwriter.Config {
	return zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "info",
		Encoding:         "console",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}
}

// Common is the configuration shared by carbonapi and carbonzipper
type Common struct {
	Listen            string    `yaml:"listen"`
	ListenInternal    string    `yaml:"listenInternal"`
	Backends          []string  `yaml:"backends"`
	BackendsByCluster []Cluster `yaml:"backendsByCluster"`
	BackendsByDC      []DC      `yaml:"backendsByDC"`

	MaxProcs                  int           `yaml:"maxProcs"`
	Timeouts                  Timeouts      `yaml:"timeouts"`
	ConcurrencyLimitPerServer int           `yaml:"concurrencyLimit"`
	KeepAliveInterval         time.Duration `yaml:"keepAliveInterval"`
	MaxIdleConnsPerHost       int           `yaml:"maxIdleConnsPerHost"`

	ExpireDelaySec             int32   `yaml:"expireDelaySec"`
	InternalRoutingCache       int32   `yaml:"internalRoutingCache"`
	GraphiteWeb09Compatibility bool    `yaml:"graphite09compat"`
	CorruptionThreshold        float64 `yaml:"corruptionThreshold"`

	Buckets  int                `yaml:"buckets"`
	Graphite GraphiteConfig     `yaml:"graphite"`
	Logger   []zapwriter.Config `yaml:"logger"`

	Monitoring MonitoringConfig `yaml:"monitoring"`

	Traces Traces `yaml:"traces"`
}

// GetBackends returns the list of backends from common configuration
func (common Common) GetBackends() []string {
	backends := []string{}
	hasDCBackends := false
	hasClusterBackends := false

	for _, dc := range common.BackendsByDC {
		hasDCBackends = true
		for _, cluster := range dc.Clusters {
			backends = append(backends, cluster.Backends...)
		}
	}

	for _, cluster := range common.BackendsByCluster {
		hasClusterBackends = true
		backends = append(backends, cluster.Backends...)
	}
	// TODO: GV - check w/BackendsByDC
	if (hasDCBackends && hasClusterBackends) || (len(common.Backends) > 0) && (len(backends) > 0) {
		log.Fatal("duplicate backend definition in config -- exiting")
	}

	if len(common.Backends) > 0 {
		return common.Backends
	}
	return backends
}

// InfoOfBackend returns the dc and cluster of a given backend address from common configuration
func (common Common) InfoOfBackend(address string) (string, string, error) {
	for _, dc := range common.BackendsByDC {
		for _, cluster := range dc.Clusters {

			for _, backend := range cluster.Backends {
				if backend == address {
					return dc.Name, cluster.Name, nil
				}
			}
		}
	}

	for _, cluster := range common.BackendsByCluster {

		for _, backend := range cluster.Backends {
			if backend == address {
				return "", cluster.Name, nil
			}
		}
	}

	for _, backend := range common.Backends {
		if backend == address {
			return "", "", nil
		}
	}

	return "", "", errors.Errorf("Couldn't find cluster for '%s'", address)
}

// MonitoringConfig allows setting custom monitoring parameters
type MonitoringConfig struct {
	RequestDurationExp      HistogramConfig `yaml:"requestDurationExpHistogram"`
	RequestDurationLin      HistogramConfig `yaml:"requestDurationLinHistogram"`
	RenderDurationExp       HistogramConfig `yaml:"renderDurationExpHistogram"`
	RenderDurationLinSimple HistogramConfig `yaml:"renderDurationLinHistogram"`
	FindDurationExp         HistogramConfig `yaml:"findDurationExpHistogram"`
	FindDurationLin         HistogramConfig `yaml:"findDurationLinHistogram"`
	FindDurationLinSimple   HistogramConfig `yaml:"findDurationSimpleLinHistogram"`
	FindDurationLinComplex  HistogramConfig `yaml:"findDurationComplexLinHistogram"`
	TimeInQueueExpHistogram HistogramConfig `yaml:"timeInQueueExpHistogram"`
	TimeInQueueLinHistogram HistogramConfig `yaml:"timeInQueueLinHistogram"`
}

// HistogramConfig is histogram config for Prometheus metrics
type HistogramConfig struct {
	Start      float64 `yaml:"start"`
	BucketsNum int     `yaml:"bucketsNum"`
	BucketSize float64 `yaml:"bucketSize"`
}

// Timeouts needs some figuring out
type Timeouts struct {
	Global       time.Duration `yaml:"global"`
	AfterStarted time.Duration `yaml:"afterStarted"`
	Connect      time.Duration `yaml:"connect"`
}

// Cluster is a definition for set of backends
type Cluster struct {
	Name     string   `yaml:"name"`
	Backends []string `yaml:"backends"`
}

// DC is a definition for data-cemter with set of clusters
type DC struct {
	Name     string    `yaml:"name"`
	Clusters []Cluster `yaml:"clusters"`
}

// Traces holds configuration related to tracing
type Traces struct {
	JaegerEndpoint string        `yaml:"jaegerEndpoint"`
	Timeout        time.Duration `yaml:"timeout"`
}
