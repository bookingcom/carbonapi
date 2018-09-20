package carbonapi_v2

import (
	"testing"

	old "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func TestInfos(t *testing.T) {
	// This works because of how protobuf decodes repeated non-packed fields.
	// The wire protocol gives us (key, byte_blob) pairs and since we know we
	// want an array of those, we create one and append to it whenever we see
	// one of those pairs. Thus having an encoder that only knows about single
	// InfoResponse types encode that and send it to us, who decode it
	// expecting multiple ones of a compatible type, is fine.

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
		t.Fatal(err)
	}

	if len(infos.Hosts) != len(infos.Infos) {
		t.Error("Amount of names and infos differ")
	}

	if len(infos.Infos) != 1 {
		t.Error("Invalid response length")
	}

	info := infos.Infos[0]

	if infos.Hosts[0] != "server" {
		t.Error("Invalid server")
	}

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
