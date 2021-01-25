package net

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"

	"github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func BenchmarkCall(b *testing.B) {
	resp := make([]byte, 1<<10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(resp)
	}))
	defer server.Close()

	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: 100,
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: time.Second,
			DualStack: true,
		}).DialContext,
	}

	bk, err := New(Config{
		Address: server.URL,
		Client:  client,
	})
	if err != nil {
		b.Error(err)
		return
	}

	ctx := context.Background()
	trace := types.NewTrace()
	u := bk.url("")
	for i := 0; i < b.N; i++ {
		bk.call(ctx, trace, u)
	}
}

func BenchmarkCallOld(b *testing.B) {
	resp := make([]byte, 1<<10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(resp)
	}))
	defer server.Close()

	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: 100,
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: time.Second,
			DualStack: true,
		}).DialContext,
	}

	for i := 0; i < b.N; i++ {
		req, err := http.NewRequest("GET", server.URL, nil)
		if err != nil {
			b.Fatal(err)
		}

		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}

		_, err = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRender(b *testing.B) {
	secPerHour := int(12 * time.Hour / time.Second)

	metrics := carbonapi_v2_pb.MultiFetchResponse{
		Metrics: []carbonapi_v2_pb.FetchResponse{
			{
				Name:     "foo",
				Values:   make([]float64, secPerHour),
				IsAbsent: make([]bool, secPerHour),
			},
		},
	}
	blob, err := metrics.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(blob)
	}))
	defer server.Close()

	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: 100,
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: time.Second,
			DualStack: true,
		}).DialContext,
	}

	bk, err := New(Config{
		Address: server.URL,
		Client:  client,
	})
	if err != nil {
		b.Error(err)
		return
	}

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		bk.Render(ctx, types.NewRenderRequest(nil, 0, 0))
	}
}
