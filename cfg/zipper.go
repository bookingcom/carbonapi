package cfg

import (
	"io"

	"github.com/go-graphite/carbonapi/pathcache"
)

type Zipper struct {
	Common      `yaml:",inline"`
	PathCache   pathcache.PathCache
	SearchCache pathcache.PathCache
}

func ParseZipperConfig(r io.Reader) (Zipper, error) {
	cfg, err := ParseCommon(r)

	if err != nil {
		return Zipper{}, err
	}

	return fromCommon(cfg), nil
}

func fromCommon(c Common) Zipper {
	return Zipper{
		Common:      c,
		PathCache:   pathcache.NewPathCache(c.ExpireDelaySec),
		SearchCache: pathcache.NewPathCache(c.ExpireDelaySec),
	}
}

var DefaultZipperConfig = fromCommon(DefaultConfig)
