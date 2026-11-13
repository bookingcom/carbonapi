package backend

import (
	"errors"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// Keep replica replies together until errors have been classified. Only the
// backends actually selected for this request can establish that a group answered.
type renderGroup struct {
	n        int
	cluster  string
	results  chan []types.Metric
	errors   chan error
	answered bool
	errs     []error
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

// receive collects n replies and returns how many of them were answers: data
// or a backend's own not-found.
func (g *renderGroup) receive(n int, messages [][]types.Metric) ([][]types.Metric, int) {
	answers := 0
	for range n {
		select {
		case metrics := <-g.results:
			messages = append(messages, metrics)
			answers++
		case err := <-g.errors:
			g.errs = append(g.errs, err)
			var notFound types.ErrNotFound
			if errors.As(err, &notFound) {
				answers++
			}
		}
	}
	if answers != 0 {
		g.answered = true
	}
	return messages, answers
}

// unrecovered reports whether this group's errors still fail the fetch.
func (g *renderGroup) unrecovered() bool { return g.cluster == "" || !g.answered }

func collectRenderGroups(f *renderFetch) {
	for i := range f.groups {
		f.messages, _ = f.groups[i].receive(f.groups[i].n, f.messages)
	}
}

func renderGroupErrors(groups []renderGroup) []error {
	var errs []error
	for _, g := range groups {
		for _, err := range g.errs {
			if !g.unrecovered() {
				err = types.ErrMetricsNotFound
			} else if u, ok := err.(unreachableError); ok {
				err = u.error
			}
			errs = append(errs, err)
		}
	}
	return errs
}
