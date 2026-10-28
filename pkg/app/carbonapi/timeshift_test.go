package carbonapi

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/expr/functions"
	"go.uber.org/zap"
)

func TestRenderTimeShiftTimezone(t *testing.T) {
	logger := zap.NewNop()
	functions.New(nil, logger)
	location, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Fatal(err)
	}
	config := cfg.DefaultAPIConfig()
	app := &App{config: config, defaultTimeZone: location, queryCache: cache.NullCache{}, ms: newPrometheusMetrics(config)}
	for _, tt := range []struct {
		name, zone, target string
		wantOffset         float64
	}{
		{"configured timezone", "", "timeShift(time('clock',60),'7d',true,true)", -608400},
		{"request override", "UTC", "timeShift(time('clock',60),'7d',true,true)", -604800},
		{"explicit timezone", "Europe/Amsterdam", "timeShift(time('clock',60),'7d',true,true)", -608400},
		{"default fixed shift", "", "timeShift(time('clock',60),'7d')", -604800},
		{"bad alignDST", "", "timeShift(time('clock',60),'7d',true,'invalid')", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{"target": {tt.target}, "from": {"1793174400"}, "until": {"1793174520"}, "format": {"json"}, "noCache": {"1"}}
			if tt.zone != "" {
				params.Set("tz", tt.zone)
			}
			r := httptest.NewRequest("GET", "/render?"+params.Encode(), nil)
			w := httptest.NewRecorder()
			app.renderHandler(w, r, logger)
			if tt.wantOffset == 0 {
				if w.Code != 400 {
					t.Fatalf("status %d, want 400: %s", w.Code, w.Body.String())
				}
				return
			}
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			var results []struct {
				Datapoints [][2]float64 `json:"datapoints"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil {
				t.Fatal(err)
			}
			if len(results) != 1 || len(results[0].Datapoints) != 2 {
				t.Fatalf("unexpected response: %s", w.Body.String())
			}
			for _, point := range results[0].Datapoints {
				if point[0]-point[1] != tt.wantOffset {
					t.Fatalf("offset = %v, want %v", point[0]-point[1], tt.wantOffset)
				}
			}
		})
	}
}
