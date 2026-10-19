//go:build !nativegzip || !cgo

package net

import (
	"compress/gzip"
	"io"
)

func newNativeInflater(src io.Reader) (renderInflater, error) { return gzip.NewReader(src) }
