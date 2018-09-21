package backend

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-graphite/carbonapi/pkg/backend/mock"
	"github.com/go-graphite/carbonapi/pkg/types"

	"go.uber.org/zap"
)

func TestCarbonapiv2InfosEmpty(t *testing.T) {
	got, err := Infos(context.Background(), []Backend{}, "foo")
	if err != nil {
		t.Error(err)
		return
	}

	if got != nil {
		t.Error("Expected nil response")
	}
}

func TestCarbonapiv2FindsEmpty(t *testing.T) {
	got, err := Finds(context.Background(), []Backend{}, "foo")
	if err != nil {
		t.Error(err)
		return
	}

	if got != nil {
		t.Error("Expected nil response")
	}
}

func TestCarbonapiv2RendersEmpty(t *testing.T) {
	got, err := Renders(context.Background(), []Backend{}, 0, 1, []string{"foo"})
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
		render := func(context.Context, int32, int32, []string) ([]types.Metric, error) {
			return []types.Metric{
				types.Metric{
					Name: "foo",
				},
			}, nil
		}
		b := mock.New(mock.Config{Render: render})
		backends = append(backends, b)
	}

	got, err := Renders(context.Background(), backends, 0, 1, []string{"foo"})
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != 1 {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}
}

func TestCarbonapiv2RendersError(t *testing.T) {
	render := func(context.Context, int32, int32, []string) ([]types.Metric, error) {
		return nil, errors.New("No")
	}

	backends := []Backend{mock.New(mock.Config{Render: render})}

	_, err := Renders(context.Background(), backends, 0, 1, []string{"foo"})
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2InfosCorrectMerge(t *testing.T) {
	backends := []Backend{
		mock.New(mock.Config{
			Info: func(context.Context, string) ([]types.Info, error) {
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
			Info: func(context.Context, string) ([]types.Info, error) {
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

	got, err := Infos(context.Background(), backends, "metric")
	if err != nil {
		t.Error(err)
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
			Info: func(context.Context, string) ([]types.Info, error) {
				return nil, errors.New("No")
			},
		}),
	}

	_, err := Infos(context.Background(), backends, "foo")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2Infos(t *testing.T) {
	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		j := i
		info := func(context.Context, string) ([]types.Info, error) {
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

	got, err := Infos(context.Background(), backends, "foo")
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != N {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}
}

func TestCarbonapiv2FindsError(t *testing.T) {
	find := func(context.Context, string) ([]types.Match, error) {
		return nil, errors.New("No")
	}

	backends := []Backend{mock.New(mock.Config{Find: find})}

	_, err := Finds(context.Background(), backends, "foo")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2Finds(t *testing.T) {

	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		j := i
		find := func(context.Context, string) ([]types.Match, error) {
			return []types.Match{
				types.Match{
					Path:   fmt.Sprintf("foo/%d", j),
					IsLeaf: true,
				},
			}, nil
		}
		b := mock.New(mock.Config{Find: find})
		backends = append(backends, b)
	}

	got, err := Finds(context.Background(), backends, "foo")
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != N {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}
}

func TestCheckErrs(t *testing.T) {
	ctx := context.Background()
	logger := zap.New(nil)

	if err := checkErrs(ctx, nil, 0, logger); err != nil {
		t.Error("Expected no error")
	}

	if err := checkErrs(ctx, []error{errors.New("no")}, 1, logger); err == nil {
		t.Error("Expected error")
	}

	if err := checkErrs(ctx, []error{errors.New("no")}, 0, logger); err == nil {
		t.Error("Expected error")
	}

	if err := checkErrs(ctx, []error{errors.New("no")}, 2, logger); err != nil {
		t.Error("Expected no error")
	}
}
