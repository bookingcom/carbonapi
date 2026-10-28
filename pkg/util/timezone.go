package util

import (
	"context"
	"time"
)

type timeZoneKey struct{}

// WithTimeZone carries the render timezone through nested expression evaluation.
func WithTimeZone(ctx context.Context, location *time.Location) context.Context {
	return context.WithValue(ctx, timeZoneKey{}, location)
}

// GetTimeZone returns the request timezone, or the local timezone for callers
// outside the HTTP render handler.
func GetTimeZone(ctx context.Context) *time.Location {
	if location, ok := ctx.Value(timeZoneKey{}).(*time.Location); ok && location != nil {
		return location
	}
	return time.Local
}
