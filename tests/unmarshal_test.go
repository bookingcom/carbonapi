package tests

import (
	"testing"

	proto2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func TestUnmarshal(t *testing.T) {
	r := &proto2.FetchResponse{
		Name:      "foo",
		StartTime: int32(1),
		StopTime:  int32(2),
		StepTime:  int32(3),
		Values:    []float64{0, 1, 2, 3, 4, 5},
		IsAbsent:  []bool{true, false, true, false, true, false},
	}

	blob, err := r.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	got := &proto2.FetchResponse{}
	if err := got.Unmarshal(blob); err != nil {
		t.Fatal(err)
	}

	if !got.Equal(r) {
		t.Fatalf("Unmarshal mismatch\nGot\t\t%v\nExpected\t%v\n", got, r)
	}
}
