package backend

import (
	"context"
	"errors"
	"fmt"
	"github.com/bookingcom/carbonapi/cfg"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	"github.com/bookingcom/carbonapi/pkg/types"
)

func TestFilter(t *testing.T) {
	backends := []Backend{
		mock.New(mock.Config{
			Contains: func([]string) bool { return true },
		}),
		mock.New(mock.Config{
			Contains: func([]string) bool { return false },
		}),
	}

	got := Filter(backends, nil)
	if len(got) != 1 {
		t.Errorf("Expected 1 backend, got %d", len(got))
	}
}

func TestFilterNoneContains(t *testing.T) {
	backends := []Backend{
		mock.New(mock.Config{
			Contains: func([]string) bool { return false },
		}),
	}

	got := Filter(backends, nil)
	if len(got) != 1 {
		t.Errorf("Expected 1 backend, got %d", len(got))
	}
}

func TestCarbonapiv2InfosEmpty(t *testing.T) {
	got, err := Infos(context.Background(), []Backend{}, types.NewInfoRequest(""))
	if err != nil {
		t.Error(err)
		return
	}

	if got != nil {
		t.Error("Expected nil response")
	}
}

func TestCarbonapiv2FindsEmpty(t *testing.T) {
	got, err := Finds(context.Background(), []Backend{}, types.NewFindRequest(""))
	if err != nil {
		t.Error(err)
		return
	}

	if len(got.Matches) != 0 {
		t.Error("Expected emtpy response")
	}
}

func TestCarbonapiv2RendersEmpty(t *testing.T) {
	got, _, err := Renders(context.Background(), []Backend{}, types.NewRenderRequest(nil, 0, 1), cfg.RenderReplicaMismatchConfig{
		RenderReplicaMismatchApproximateCheck: false,
		RenderReplicaMatchMode:                cfg.ReplicaMatchModeNormal,
		RenderReplicaMismatchReportLimit:      10,
	})
	if err != nil {
		t.Error(err)
		return
	}

	if got != nil {
		t.Error("Expected nil response")
	}
}

func TestCarbonapiv2Renders(t *testing.T) {
	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		render := func(context.Context, types.RenderRequest) ([]types.Metric, error) {
			return []types.Metric{
				types.Metric{
					Name:      "foo",
					StartTime: 0,
					StopTime:  5,
					Values:    []float64{0, 1, 2, 3, 4, 5},
					IsAbsent:  []bool{false, false, false, false, false, false},
					StepTime:  1,
				},
			}, nil
		}
		b := mock.New(mock.Config{Render: render})
		backends = append(backends, b)
	}

	got, stats, errs := Renders(context.Background(), backends, types.NewRenderRequest(nil, 0, 1), cfg.RenderReplicaMismatchConfig{
		RenderReplicaMismatchApproximateCheck: false,
		RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
		RenderReplicaMismatchReportLimit:      10,
	})
	if len(errs) != 0 {
		t.Error(errs[0])
		return
	}

	if len(got) != 1 {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}

	if stats.DataPointCount != 6 {
		t.Errorf("Expected %d points, got %d", 6, stats.DataPointCount)
		return
	}

	if stats.MismatchCount != 0 {
		t.Errorf("Expected %d mismatches, got %d", 0, stats.MismatchCount)
		return
	}

	if stats.FixedMismatchCount != 0 {
		t.Errorf("Expected %d fixed mismatches, got %d", 0, stats.FixedMismatchCount)
		return
	}
}

func TestCarbonapiv2RendersError(t *testing.T) {
	render := func(context.Context, types.RenderRequest) ([]types.Metric, error) {
		return nil, errors.New("No")
	}

	backends := []Backend{mock.New(mock.Config{Render: render})}

	_, _, err := Renders(context.Background(), backends, types.NewRenderRequest(nil, 0, 1), cfg.RenderReplicaMismatchConfig{
		RenderReplicaMismatchApproximateCheck: false,
		RenderReplicaMatchMode:                cfg.ReplicaMatchModeNormal,
		RenderReplicaMismatchReportLimit:      10,
	})
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2InfosCorrectMerge(t *testing.T) {
	backends := []Backend{
		mock.New(mock.Config{
			Info: func(context.Context, types.InfoRequest) ([]types.Info, error) {
				return []types.Info{
					types.Info{
						Host:              "host_A",
						Name:              "metric",
						AggregationMethod: "sum",
					},
				}, nil
			},
		}),
		mock.New(mock.Config{
			Info: func(context.Context, types.InfoRequest) ([]types.Info, error) {
				return []types.Info{
					types.Info{
						Host:              "host_B",
						Name:              "metric",
						AggregationMethod: "average",
					},
				}, nil
			},
		}),
	}

	got, errs := Infos(context.Background(), backends, types.NewInfoRequest(""))
	if len(errs) != 0 {
		t.Errorf("Infos returned errors %v ...", errs[0])
		return
	}

	if len(got) != len(backends) {
		t.Errorf("Expected %d responses, got %d", len(backends), len(got))
		return
	}

	if got[0].AggregationMethod == got[1].AggregationMethod {
		t.Error("Expected different aggregation methods")
	}
}

func TestCarbonapiv2InfosError(t *testing.T) {
	backends := []Backend{
		mock.New(mock.Config{
			Info: func(context.Context, types.InfoRequest) ([]types.Info, error) {
				return nil, errors.New("No")
			},
		}),
	}

	_, err := Infos(context.Background(), backends, types.NewInfoRequest(""))
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2Infos(t *testing.T) {
	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		j := i
		info := func(context.Context, types.InfoRequest) ([]types.Info, error) {
			return []types.Info{
				types.Info{
					Host: fmt.Sprintf("host_%d", j),
					Name: fmt.Sprintf("foo/%d", j),
				},
			}, nil
		}
		b := mock.New(mock.Config{Info: info})
		backends = append(backends, b)
	}

	got, errs := Infos(context.Background(), backends, types.NewInfoRequest(""))
	if len(errs) != 0 {
		t.Errorf("Infos returned errors %v ...", errs[0])
		return
	}

	if len(got) != N {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}
}

func TestCarbonapiv2FindsError(t *testing.T) {
	find := func(context.Context, types.FindRequest) (types.Matches, error) {
		return types.Matches{}, errors.New("No")
	}

	backends := []Backend{mock.New(mock.Config{Find: find})}

	_, err := Finds(context.Background(), backends, types.NewFindRequest(""))
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2Finds(t *testing.T) {

	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		j := i
		find := func(context.Context, types.FindRequest) (types.Matches, error) {
			return types.Matches{
				Name: "foo",
				Matches: []types.Match{
					types.Match{
						Path:   fmt.Sprintf("foo/%d", j),
						IsLeaf: true,
					},
				},
			}, nil
		}
		b := mock.New(mock.Config{Find: find})
		backends = append(backends, b)
	}

	got, errs := Finds(context.Background(), backends, types.NewFindRequest(""))
	if len(errs) != 0 {
		t.Error(errs[0])
		return
	}

	if len(got.Matches) != N {
		t.Errorf("Expected %d responses, got %d", N, len(got.Matches))
		return
	}
}
