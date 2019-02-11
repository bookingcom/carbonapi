package cfg

import (
	"io"
	"time"

	"github.com/lomik/zapwriter"
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

		ExpireDelaySec: int32(10 * time.Minute / time.Second),

		Buckets: 10,
		Graphite: GraphiteConfig{
			Interval: 60 * time.Second,
			Host:     "127.0.0.1:3002",
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
			FindDurationExp: HistogramConfig{
				Start:      0.05,
				BucketSize: 2,
				BucketsNum: 20,
			},
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
	Listen         string   `yaml:"listen"`
	ListenInternal string   `yaml:"listenInternal"`
	Backends       []string `yaml:"backends"`

	MaxProcs                  int           `yaml:"maxProcs"`
	Timeouts                  Timeouts      `yaml:"timeouts"`
	ConcurrencyLimitPerServer int           `yaml:"concurrencyLimit"`
	KeepAliveInterval         time.Duration `yaml:"keepAliveInterval"`
	MaxIdleConnsPerHost       int           `yaml:"maxIdleConnsPerHost"`

	ExpireDelaySec             int32   `yaml:"expireDelaySec"`
	GraphiteWeb09Compatibility bool    `yaml:"graphite09compat"`
	CorruptionThreshold        float64 `yaml:"corruptionThreshold"`

	Buckets  int                `yaml:"buckets"`
	Graphite GraphiteConfig     `yaml:"graphite"`
	Logger   []zapwriter.Config `yaml:"logger"`

	Monitoring MonitoringConfig `yaml:"monitoring"`
}

// MonitoringConfig allows setting custom monitoring parameters
type MonitoringConfig struct {
	RequestDurationExp      HistogramConfig `yaml:"requestDurationExpHistogram"`
	RequestDurationLin      HistogramConfig `yaml:"requestDurationLinHistogram"`
	RenderDurationExp       HistogramConfig `yaml:"renderDurationExpHistogram"`
	FindDurationExp         HistogramConfig `yaml:"findDurationExpHistogram"`
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
