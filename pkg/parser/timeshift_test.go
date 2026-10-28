package parser

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeShiftOffset(t *testing.T) {
	cases := []struct {
		name, zone, start, end, args string
		want                         int32
	}{
		{"autumn", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d',true,true", -169 * 3600},
		{"spring", "Europe/Amsterdam", "2026-04-01T08:00:00Z", "2026-04-01T09:00:00Z", "'7d',true,true", -167 * 3600},
		{"positive", "Europe/Amsterdam", "2026-10-21T08:00:00Z", "2026-10-21T09:00:00Z", "'+7d',true,true", 169 * 3600},
		{"explicit negative", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'-7d',true,true", -169 * 3600},
		{"same season", "Europe/Amsterdam", "2026-10-21T08:00:00Z", "2026-10-21T09:00:00Z", "'7d',true,true", -168 * 3600},
		{"UTC", "UTC", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d',true,true", -168 * 3600},
		{"disabled", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d',true,false", -168 * 3600},
		{"omitted", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d'", -168 * 3600},
		{"resetEnd false", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d',false,true", -169 * 3600},
		{"named", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d',alignDST=true", -169 * 3600},
		{"graphite spelling", "Europe/Amsterdam", "2026-10-28T08:00:00Z", "2026-10-28T09:00:00Z", "'7d',alignDst=true", -169 * 3600},
		{"half hour", "Australia/Lord_Howe", "2026-04-08T08:00:00Z", "2026-04-08T09:00:00Z", "'7d',true,true", -168*3600 - 1800},
		{"request crosses fall back", "Europe/Amsterdam", "2026-10-25T00:00:00Z", "2026-10-25T03:00:00Z", "'7d',true,true", -168 * 3600},
		{"source crosses fall back", "Europe/Amsterdam", "2026-11-01T00:00:00Z", "2026-11-01T03:00:00Z", "'7d',true,true", -168 * 3600},
		{"request crosses spring forward", "Europe/Amsterdam", "2026-03-29T00:00:00Z", "2026-03-29T03:00:00Z", "'7d',true,true", -168 * 3600},
		{"source crosses spring forward", "Europe/Amsterdam", "2026-04-05T00:00:00Z", "2026-04-05T03:00:00Z", "'7d',true,true", -168 * 3600},
		{"repeated hour first occurrence", "Europe/Amsterdam", "2026-10-25T00:10:00Z", "2026-10-25T00:20:00Z", "'7d',true,true", -168 * 3600},
		{"repeated hour second occurrence", "Europe/Amsterdam", "2026-10-25T01:10:00Z", "2026-10-25T01:20:00Z", "'7d',true,true", -169 * 3600},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			location, err := time.LoadLocation(tt.zone)
			if err != nil {
				t.Fatal(err)
			}
			start, _ := time.Parse(time.RFC3339, tt.start)
			end, _ := time.Parse(time.RFC3339, tt.end)
			e, rest, err := ParseExpr("timeShift(metric," + tt.args + ")")
			if err != nil || rest != "" {
				t.Fatalf("parse: %v %q", err, rest)
			}
			got, err := TimeShiftOffset(e, int32(start.Unix()), int32(end.Unix()), location)
			if err != nil || got != tt.want {
				t.Fatalf("offset = %d, %v; want %d", got, err, tt.want)
			}
		})
	}
}

func TestTimeShiftInvalidArguments(t *testing.T) {
	for _, target := range []string{"timeShift(metric)", "timeShift(metric,'bad')", "timeShift(metric,'7d',true,'invalid')"} {
		e, _, err := ParseExpr(target)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = TimeShiftOffset(e, 0, 1, time.UTC); err == nil {
			t.Errorf("%s: expected error", target)
		}
	}
}

func TestMetricsForRange(t *testing.T) {
	location, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, "2026-10-28T08:00:00Z")
	from, until := int32(start.Unix()), int32(start.Unix())+3600
	cases := []struct {
		target  string
		offsets [][2]int32
	}{
		{"timeShift(metric,'7d',true,true)", [][2]int32{{-608400, -608400}}},
		{"timeShift(timeShift(metric,'7d',true,true),'7d',true,true)", [][2]int32{{-1213200, -1213200}}},
		{"timeStack(timeShift(metric,'7d',true,true),'7d',0,2)", [][2]int32{{-608400, -608400}, {-1209600, -1209600}}},
		{"movingAverage(timeShift(metric,'7d',true,true),'1h')", [][2]int32{{-612000, -608400}}},
		{"timeShift(movingAverage(metric,'1h'),'7d',true,true)", [][2]int32{{-612000, -608400}}},
		{"sumSeries(timeShift(metric,'7d',true,true),timeShift(metric,'14d',true,true),timeShift(metric,'21d',true,true))", [][2]int32{{-608400, -608400}, {-1213200, -1213200}, {-1818000, -1818000}}},
	}
	for _, tt := range cases {
		t.Run(tt.target, func(t *testing.T) {
			e, _, err := ParseExpr(tt.target)
			if err != nil {
				t.Fatal(err)
			}
			var want []MetricRequest
			for _, offset := range tt.offsets {
				want = append(want, MetricRequest{"metric", from + offset[0], until + offset[1]})
			}
			if got := e.MetricsForRange(from, until, location); !reflect.DeepEqual(got, want) {
				t.Fatalf("got %+v; want %+v", got, want)
			}
		})
	}
}
