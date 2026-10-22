package types

import (
	"testing"

	"github.com/bookingcom/carbonapi/pkg/cfg"
)

func BenchmarkPackedReplicaMerge(b *testing.B) {
	for _, density := range []string{"zero", "sparse", "dense"} {
		for _, mode := range []cfg.ReplicaMatchMode{cfg.ReplicaMatchModeNormal, cfg.ReplicaMatchModeCheck, cfg.ReplicaMatchModeMajority} {
			b.Run(density+"/"+string(mode), func(b *testing.B) {
				metrics := make([]Metric, 3)
				for replica := range metrics {
					m := Metric{StepTime: 60, Values: make([]float64, 10080), IsAbsent: make([]bool, 10080)}
					for i := range m.Values {
						m.IsAbsent[i] = (i+replica)%3 == 0
						if density == "dense" || density == "sparse" && i%997 == 0 {
							m.Values[i] = 17
						}
					}
					m.PackPoints()
					metrics[replica] = m
				}
				c := cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: mode}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m, stats := mergeMetrics(metrics, c)
					if len(m.Values) != 10080 || stats.MismatchCount != 0 {
						b.Fatal("invalid merge")
					}
				}
			})
		}
	}
}
