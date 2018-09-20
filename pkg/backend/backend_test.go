package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/go-graphite/carbonapi/pkg/backend/mock"
)

func TestScatterGatherEmpty(t *testing.T) {
	resp, err := ScatterGather(context.Background(), nil, nil, nil)
	if err != nil {
		t.Error(err)
	}

	if len(resp) > 0 {
		t.Error("Expected an empty response")
	}
}

func TestScatterGather(t *testing.T) {
	mCall := func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
		return []byte("yo"), nil
	}
	mURL := func(path string) *url.URL { return new(url.URL) }
	b := mock.New(mCall, mURL)

	resps, err := ScatterGather(context.Background(), []Backend{b}, b.URL("/render"), nil)
	if err != nil {
		t.Error(err)
	}

	if len(resps) != 1 {
		t.Error("Didn't get all responses")
	}

	if !bytes.Equal(resps[0], []byte("yo")) {
		t.Errorf("Didn't get expected response\nGot %v\nExp %v", resps[0], []byte("yo"))
	}
}

func TestScatterGatherHammer(t *testing.T) {
	N := 10

	backends := make([]Backend, N)
	for i := 0; i < N; i++ {
		j := i
		mCall := func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
			return []byte(fmt.Sprintf("%d", j)), nil
		}
		mURL := func(path string) *url.URL { return new(url.URL) }
		backends[i] = mock.New(mCall, mURL)
	}

	u := backends[0].URL("/render")
	resps, err := ScatterGather(context.Background(), backends, u, nil)
	if err != nil {
		t.Error(err)
	}

	if len(resps) != N {
		t.Error("Didn't get all responses")
	}

	uniqueBodies := make(map[string]struct{})
	for i := 0; i < N; i++ {
		uniqueBodies[string(resps[i])] = struct{}{}
	}
	if len(uniqueBodies) != N {
		t.Errorf("Expected %d unique responses, got %d:\n%+v", N, len(uniqueBodies), uniqueBodies)
	}
}

func TestScatterGatherHammerOneTimeout(t *testing.T) {
	N := 10

	backends := make([]Backend, 0, N)
	mURL := func(path string) *url.URL { return new(url.URL) }
	for i := 0; i < N; i++ {
		var mCall func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error)
		if i == 0 {
			mCall = func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
				return nil, errors.New("no")
			}
		} else {
			mCall = func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
				return nil, nil
			}
		}
		backends = append(backends, mock.New(mCall, mURL))
	}

	u := backends[0].URL("/render")
	resps, err := ScatterGather(context.Background(), backends, u, nil)
	if err != nil {
		t.Error(err)
	}

	if len(resps) != N-1 {
		t.Errorf("Expected %d responses, got %d", N-1, len(resps))
	}
}
