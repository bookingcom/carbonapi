package cfg

import (
	"io"

	"gopkg.in/yaml.v2"
)

func ParseAPIConfig(r io.Reader) (API, error) {
	d := yaml.NewDecoder(r)
	d.SetStrict(true)

	api := API{}
	err := d.Decode(api)
	if err != nil {
		return API{}, err
	}

	return api, nil
}

var DefaultAPIConfig = defaultAPIConfig()

func defaultAPIConfig() API {
	cfg := API{
		Zipper: DefaultZipperConfig,

		ExtrapolateExperiment: false,
		SendGlobsAsIs:         false,
		AlwaysSendGlobsAsIs:   false,
		MaxBatchSize:          100,
		Cache: CacheConfig{
			Type:              "mem",
			DefaultTimeoutSec: 60,
		},
	}

	cfg.Listen = ":8081"
	cfg.MaxProcs = 0
	cfg.Graphite.Prefix = "carbon.api"

	return cfg
}

type API struct {
	Zipper `yaml:",inline"`

	ExtrapolateExperiment bool        `yaml:"extrapolateExperiment"`
	SendGlobsAsIs         bool        `yaml:"sendGlobsAsIs"`
	AlwaysSendGlobsAsIs   bool        `yaml:"alwaysSendGlobsAsIs"`
	MaxBatchSize          int         `yaml:"maxBatchSize"`
	Cache                 CacheConfig `yaml:"cache"`
	TimezoneString        string      `yaml:"tz"`
	PidFile               string      `yaml:"pidFile"`

	UnicodeRangeTables  []string          `yaml:"unicodeRangeTables"`
	IgnoreClientTimeout bool              `yaml:"ignoreClientTimeout"`
	DefaultColors       map[string]string `yaml:"defaultColors"`
	FunctionsConfigs    map[string]string `yaml:"functionsConfig"`

	// NOTE(gmagnusson): Added in 21c5a3cc.
	// See: https://github.com/go-graphite/carbonapi/issues/163
	// It's possible this implements a feature in graphite-web that
	// graphite-web has decided to replace with something completely different.
	GraphTemplates string `yaml:"graphTemplates"`
}

type CacheConfig struct {
	Type              string   `yaml:"type"`
	Size              int      `yaml:"size_mb"`
	MemcachedServers  []string `yaml:"memcachedServers"`
	DefaultTimeoutSec int32    `yaml:"defaultTimeoutSec"`
}
