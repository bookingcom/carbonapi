package cfg

import (
	"io"
	"time"

	"gopkg.in/yaml.v2"
)

// ParseAPIConfig reads carbonapi-specific config
func ParseAPIConfig(r io.Reader) (API, error) {
	d := yaml.NewDecoder(r)
	d.SetStrict(DEBUG)

	pre := preAPI{
		API:       DefaultAPIConfig(),
		Upstreams: DefaultCommonConfig(),
	}
	err := d.Decode(&pre)
	if err != nil {
		return API{}, err
	}

	api := pre.API

	// Backwards compatibility is king
	if pre.Concurrency > 0 {
		api.ConcurrencyLimitPerServer = pre.Concurrency
	}

	if pre.CPUs > 0 {
		api.MaxProcs = pre.CPUs
	}

	if pre.IdleConnections > 0 {
		api.MaxIdleConnsPerHost = pre.IdleConnections
	}

	var defaultCfg = DefaultCommonConfig()

	if pre.Upstreams.Buckets != defaultCfg.Buckets {
		api.Buckets = pre.Upstreams.Buckets
	}

	// Any value set to a non-default in a nested structure means we pick all
	// values from that structure, for the sanity of the ops people.
	if pre.Upstreams.Timeouts != defaultCfg.Timeouts {
		api.Timeouts = pre.Upstreams.Timeouts
	}

	if len(pre.Upstreams.Backends) >= 1 {
		api.Backends = pre.Upstreams.Backends
	}

	return api, nil
}

// DefaultAPIConfig gives a starter carbonapi conf
func DefaultAPIConfig() API {
	cfg := API{
		Zipper: fromCommon(DefaultCommonConfig()),

		ResolveGlobs: 100,
		Cache: CacheConfig{
			Type:              "mem",
			DefaultTimeoutSec: 60,
			QueryTimeoutMs:    50,
			Prefix:            "capi",
		},
		// This is an intentionally large number as an intermediate refactored state.
		// This effectively turns off the queue size limitation.
		QueueSize: 1000000,
		// This is left large to make the limit light.
		MaxConcurrentUpstreamRequests: 20000,
		// In most cases a single worker should suffice.
		// The default is set to 4 as a precaution against bottlenecks.
		ProcWorkers:  4,
		LargeReqSize: 10000,

		UpstreamSubRenderNumHistParams: HistogramConfig{
			Start:      1,
			BucketsNum: 12,
			BucketSize: 3,
		},
		UpstreamTimeInQSecHistParams: HistogramConfig{
			Start:      0.1,
			BucketsNum: 10,
			BucketSize: 2,
		},
	}

	cfg.Listen = ":8081"
	cfg.MaxProcs = 0
	cfg.Graphite.Prefix = "carbon.api"

	return cfg
}

// API is carbonapi-specific config
type API struct {
	Zipper `yaml:",inline"`

	ResolveGlobs                     int               `yaml:"resolveGlobs"`
	EnableCacheForRenderResolveGlobs bool              `yaml:"enableCacheForRenderResolveGlobs"`
	Cache                            CacheConfig       `yaml:"cache"`
	TimezoneString                   string            `yaml:"tz"`
	PidFile                          string            `yaml:"pidFile"`
	BlockHeaderFile                  string            `yaml:"blockHeaderFile"`
	BlockHeaderUpdatePeriod          time.Duration     `yaml:"blockHeaderUpdatePeriod"`
	HeadersToLog                     []string          `yaml:"headersToLog"`
	UnicodeRangeTables               []string          `yaml:"unicodeRangeTables"`
	IgnoreClientTimeout              bool              `yaml:"ignoreClientTimeout"`
	DefaultColors                    map[string]string `yaml:"defaultColors"`
	FunctionsConfigs                 map[string]string `yaml:"functionsConfig"`
	GraphiteVersionForGrafana        string            `yaml:"graphiteVersionForGrafana"`

	// The size of the requests queue propagated to backends.
	// During this stage of refactoring it is a placeholder and should not fill-up.
	// At the later stages it will play a key role in the request processing.
	QueueSize                     int `yaml:"queueSize"`
	MaxConcurrentUpstreamRequests int `yaml:"maxConcurrentUpstreamRequests"`
	// The number of workers to process requests queue.
	ProcWorkers int `yaml:"procWorkers"`
	// The threshold of the number of sub-requests after which the render requests are considered large.
	// It is used to select the processing queue: Small requests get on the fast queue, large ones on the slow one.
	LargeReqSize int `yaml:"largeRequestSize"`

	UpstreamSubRenderNumHistParams HistogramConfig `yaml:"upstreamSubRenderNumHistParams"`
	UpstreamTimeInQSecHistParams   HistogramConfig `yaml:"upstreamTimeInQSecHistParams"`

	// EmbedZipper makes carbonapi to use zipper as a package, not as a service.
	EmbedZipper bool `yaml:"embedZipper"`
	// ZipperConfig represents the config for the embedded zipper.
	ZipperConfig string `yaml:"zipperConfig"`
}

// CacheConfig configs the cache
type CacheConfig struct {
	// possible values are: null, mem, memcache, replicatedMemcache
	Type             string   `yaml:"type"`
	Size             int      `yaml:"size_mb"`
	MemcachedServers []string `yaml:"memcachedServers"`
	// TODO (grzkv): This looks to be used as expiration time for cache
	DefaultTimeoutSec int32  `yaml:"defaultTimeoutSec"`
	QueryTimeoutMs    uint64 `yaml:"queryTimeoutMs"`
	Prefix            string `yaml:"prefix"`
}

type preAPI struct {
	API             `yaml:",inline"`
	Concurrency     int    `yaml:"concurency"`
	CPUs            int    `yaml:"cpus"`
	IdleConnections int    `yaml:"idleConnections"`
	Upstreams       Common `yaml:"upstreams"`
}
