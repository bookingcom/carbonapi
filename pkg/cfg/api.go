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
			Type:                  "mem",
			DefaultTimeoutSec:     60,
			QueryTimeoutMs:        50,
			Prefix:                "capi",
			MemcachedTimeoutMs:    1000,
			MemcachedMaxIdleConns: 50,
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

	// = 0 - faster (no 'find in advance', direct render)
	// = 1 - slower (always 'find in advance' and every metric rendered individually)
	// > 1 - slower (always 'find in advance' and then render strategy depend on amount of metrics)
	// If resolveGlobs is = 0 (zero) then /metrics/find request won't be send and a query will be passed
	// to a /render as it is.
	//
	// If resolveGlobs is = 1 (zero) then send /metrics/find request and send /render for every metric separately
	//
	// If resolveGlobs is set > 1 (one) then carbonapi will send a /metrics/find request and it will check
	// the resulting response if it contain more than resolveGlobs metrics
	//  If find returns MORE metrics than resolveGlobs - carbonapi will query metrics one by one
	//  If find returns LESS metrics than resolveGlobs - revert to the old behaviour and send the query as it is.
	// This allows you to use benifits of passing globs as is but keep memory usage in carbonzipper within sane limits.
	//
	// For go-carbon you might want to keep it in some reasonable limits, 100 is a good "safe" default
	// For some backends you might want to set it to 0
	// If you noticing carbonzipper OOM then this is a parameter to tune.
	ResolveGlobs int `yaml:"resolveGlobs"`
	// Indicates if carbonapi should use cache when sending preflight find requests to resolve the globs.
	// Note that useCache parameter should be sent in the request for this config to be taken into account.
	EnableCacheForRenderResolveGlobs bool        `yaml:"enableCacheForRenderResolveGlobs"`
	Cache                            CacheConfig `yaml:"cache"`
	// Timezone, default - local
	TimezoneString string `yaml:"tz"`
	PidFile        string `yaml:"pidFile"`
	// The path and the name of the file with a list of headers to block.
	// Based on the value of header you can block requests which are coming to carbonapi
	// This file can be updated via API call to the port specified in listenInternal: config option,
	// like so:
	// curl 'localhost:7081/block-headers/?x-webauth-user=el-diablo&x-real-ip=1.2.3.4'
	// carbonapi needs to have write access to this file/folder
	BlockHeaderFile         string        `yaml:"blockHeaderFile"`
	BlockHeaderUpdatePeriod time.Duration `yaml:"blockHeaderUpdatePeriod"`
	// List of HTTP headers to log. This can be usefull to track request to the source of it.
	// Defaults allow you to find grafana user/dashboard/panel which send a request
	HeadersToLog        []string          `yaml:"headersToLog"`
	UnicodeRangeTables  []string          `yaml:"unicodeRangeTables"`
	IgnoreClientTimeout bool              `yaml:"ignoreClientTimeout"`
	DefaultColors       map[string]string `yaml:"defaultColors"`
	FunctionsConfigs    map[string]string `yaml:"functionsConfig"`
	// Config to ensure we return version needed for providing integrated graphite docs in grafana
	// without supporting tags
	GraphiteVersionForGrafana string `yaml:"graphiteVersionForGrafana"`

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
	Type string `yaml:"type"`
	// Cache limit in megabytes
	Size             int      `yaml:"size_mb"`
	MemcachedServers []string `yaml:"memcachedServers"`
	// Cache entries expiration time. Identical to DEFAULT_CACHE_DURATION in graphite-web.
	DefaultTimeoutSec int32  `yaml:"defaultTimeoutSec"`
	QueryTimeoutMs    uint64 `yaml:"queryTimeoutMs"`
	// prefix is added to every key in memcache
	Prefix                string `yaml:"prefix"`
	MemcachedTimeoutMs    int    `yaml:"memcachedTimeoutMs"`
	MemcachedMaxIdleConns int    `yaml:"memcachedMaxIdleConns"`
}

type preAPI struct {
	API         `yaml:",inline"`
	Concurrency int `yaml:"concurency"`
	// Amount of CPUs to use. 0 - unlimited
	CPUs            int    `yaml:"cpus"`
	IdleConnections int    `yaml:"idleConnections"`
	Upstreams       Common `yaml:"upstreams"`
}
