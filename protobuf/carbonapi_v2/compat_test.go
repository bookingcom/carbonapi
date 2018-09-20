package carbonapi_v2

import (
	"testing"

	old "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func TestInfos(t *testing.T) {
	oldInfo := old.ServerInfoResponse{
		Server: "server",
		Info: &old.InfoResponse{
			Name:              "name",
			AggregationMethod: "method",
			MaxRetention:      1,
			XFilesFactor:      2,
			Retentions: []old.Retention{
				old.Retention{
					SecondsPerPoint: 3,
					NumberOfPoints:  4,
				},
			},
		},
	}

	blob, err := oldInfo.Marshal()
	if err != nil {
		t.Error(err)
	}

	infos := Infos{}
	if err := infos.Unmarshal(blob); err != nil {
		t.Error(err)
	}

	if len(infos.Infos) != 1 {
		t.Error("Invalid response length")
	}

	info := infos.Infos[0]

	if info.Name != "name" {
		t.Error("Invalid name")
	}

	if info.AggregationMethod != "method" {
		t.Error("Invalid method")
	}

	if info.MaxRetention != 1 {
		t.Error("Invalid retention")
	}

	if info.XFilesFactor != 2 {
		t.Error("Invalid xFiles")
	}

	if len(info.Retentions) != 1 {
		t.Error("Invalid retention length")
	}

	if info.Retentions[0].SecondsPerPoint != 3 {
		t.Error("Invalid seconds per point")
	}

	if info.Retentions[0].NumberOfPoints != 4 {
		t.Error("Invalid number per point")
	}
}
