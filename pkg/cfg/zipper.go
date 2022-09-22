package cfg

import (
	"io"
)

// Zipper is the zipper config
type Zipper struct {
	Common `yaml:",inline"`
}

// ParseZipperConfig reads the zipper config from a supplied reader
func ParseZipperConfig(r io.Reader) (Zipper, error) {
	cfg, err := ParseCommon(r)

	if err != nil {
		return Zipper{}, err
	}

	return fromCommon(cfg), nil
}

// DefaultZipperConfig makes a testable default config
func DefaultZipperConfig() Zipper {
	return fromCommon(DefaultCommonConfig())
}

func fromCommon(c Common) Zipper {
	return Zipper{
		Common: c,
	}
}
