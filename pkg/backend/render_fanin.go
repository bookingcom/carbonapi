package backend

import (
	"errors"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// Keep replica replies together until errors have been classified. Only the
// backends actually selected for this request can establish that a group answered.
type renderGroup struct {
	n       int
	cluster string
	results chan []types.Metric
	errors  chan error
}

func newRenderGroups(backends []Backend) ([]renderGroup, []int) {
	groups := make([]renderGroup, 0)
	membership := make([]int, len(backends))
	byGroup := make(map[[2]string]int)
	for i, b := range backends {
		_, cluster, dc := b.BackendInfo()
		if cluster == "" {
			// Ungrouped backends share channels, but never recover each other's errors.
			dc = ""
		}
		key := [2]string{dc, cluster}
		g, ok := byGroup[key]
		if !ok {
			g = len(groups)
			byGroup[key] = g
			groups = append(groups, renderGroup{cluster: cluster})
		}
		groups[g].n++
		membership[i] = g
	}
	for i := range groups {
		groups[i].results = make(chan []types.Metric, groups[i].n)
		groups[i].errors = make(chan error, groups[i].n)
	}
	return groups, membership
}

func collectRenderGroups(groups []renderGroup) ([][]types.Metric, []error) {
	var messages [][]types.Metric
	var errs []error
	for _, g := range groups {
		start := len(errs)
		answered := false
		for range g.n {
			select {
			case metrics := <-g.results:
				messages = append(messages, metrics)
				answered = true
			case err := <-g.errors:
				errs = append(errs, err)
				var notFound types.ErrNotFound
				if errors.As(err, &notFound) {
					answered = true
				}
			}
		}
		if g.cluster != "" && answered {
			for i := start; i < len(errs); i++ {
				errs[i] = types.ErrMetricsNotFound
			}
		}
	}
	return messages, errs
}
