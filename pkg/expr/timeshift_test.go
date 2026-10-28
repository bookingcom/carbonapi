package expr

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/util"
	th "github.com/bookingcom/carbonapi/tests"
)

func TestTimeShiftDSTEvaluation(t *testing.T) {
	location, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Fatal(err)
	}
	ctx := util.WithTimeZone(context.Background(), location)
	start, _ := time.Parse(time.RFC3339, "2026-10-28T08:00:00Z")
	from := int32(start.Unix())
	until := from + 120
	for _, tt := range []struct {
		target string
		offset int32
	}{
		{"timeShift(metric,'7d')", -604800},
		{"timeShift(metric,'7d',true,false)", -604800},
		{"timeShift(metric,'7d',true,true)", -608400},
		{"timeShift(metric,'7d',alignDST=true)", -608400},
		{"timeShift(timeShift(metric,'7d',true,true),'7d',true,true)", -1213200},
	} {
		t.Run(tt.target, func(t *testing.T) {
			e, _, err := parser.ParseExpr(tt.target)
			if err != nil {
				t.Fatal(err)
			}
			requests := e.MetricsForRange(from, until, location)
			wantRequest := parser.MetricRequest{Metric: "metric", From: from + tt.offset, Until: until + tt.offset}
			if len(requests) != 1 || requests[0] != wantRequest {
				t.Fatalf("fetches = %+v, want %+v", requests, wantRequest)
			}
			input := types.MakeMetricData("metric", []float64{11, 22}, 60, wantRequest.From)
			before := *input
			values := map[parser.MetricRequest][]*types.MetricData{wantRequest: {input}}
			result, err := EvalExpr(ctx, e, from, until, values, th.NoopGetTargetData)
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 1 {
				t.Fatalf("got %d series", len(result))
			}
			if result[0].StartTime != from || result[0].StopTime != until || result[0].StepTime != 60 || !reflect.DeepEqual(result[0].Values, []float64{11, 22}) {
				t.Fatalf("unexpected result: %+v", result[0])
			}
			if !reflect.DeepEqual(*input, before) {
				t.Fatal("input mutated")
			}
		})
	}
}
