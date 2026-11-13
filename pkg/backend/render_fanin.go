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
	data     bool // a member returned at least one series
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

// next waits for one member's reply.
func (g *renderGroup) next() ([]types.Metric, error) {
	return nextReply(g.results, g.errors)
}

func nextReply(results <-chan []types.Metric, errs <-chan error) ([]types.Metric, error) {
	select {
	case metrics := <-results:
		return metrics, nil
	case err := <-errs:
		return nil, err
	}
}

// take records one member's reply and reports whether it was an answer: data
// or the backend's own not-found.
func (g *renderGroup) take(metrics []types.Metric, err error, messages [][]types.Metric) ([][]types.Metric, bool) {
	if err != nil {
		g.errs = append(g.errs, err)
		var notFound types.ErrNotFound
		if !errors.As(err, &notFound) {
			return messages, false
		}
	} else {
		messages = append(messages, metrics)
		g.data = g.data || len(metrics) != 0
	}
	g.answered = true
	return messages, true
}

// receive collects n replies and returns how many of them were answers.
func (g *renderGroup) receive(n int, messages [][]types.Metric) ([][]types.Metric, int) {
	answers := 0
	for range n {
		metrics, err := g.next()
		var answer bool
		if messages, answer = g.take(metrics, err, messages); answer {
			answers++
		}
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

// renderGroupErrors returns the errors that still decide the fetch: an
// answered group's errors become not-found, and so do the connect failures of
// a group whose cluster's other replica groups all answered not-found
// (clusterNotFound). Every other error is returned as the backend reported it.
func renderGroupErrors(groups []renderGroup) []error {
	var errs []error
	for gi := range groups {
		g := &groups[gi]
		notFound := !g.unrecovered()
		unreachableNotFound := !notFound && clusterNotFound(groups, gi)
		for _, err := range g.errs {
			switch {
			case notFound, unreachableNotFound && connectFailure(err):
				err = types.ErrMetricsNotFound
			default:
				if u, ok := err.(unreachableError); ok {
					err = u.error
				}
			}
			errs = append(errs, err)
		}
	}
	return errs
}

// clusterNotFound reports whether the cluster of group gi has at least one
// other replica group and every one of them gave a definitive not-found: some
// member answered, and none returned data. Replica groups of one cluster hold
// the same names, so that is what gi's refused or timed-out stores would have
// answered too; their connect failures then count as the cluster's not-found
// instead of failing the fetch. The fe-112 canary needed this: the literal
// target "*" (as in sumSeries(x, *)) got not-found from 93 stores and a refusal
// from the one store of the dc1 carbon group, and the required-fetch rule (not-found only
// when every error is a not-found) turned the whole render into HTTP 500.
//
// The connect failure stays an error, so the required-fetch rule still fails a fetch no store has
// data for, when gi is its cluster's only group, when any other group is
// unreachable too or failed (for example HTTP 5xx), or when another group
// returned data (the render then has that data anyway). Server errors and
// failures after the backend accepted the request are never converted, and
// ungrouped backends never recover each other.
func clusterNotFound(groups []renderGroup, gi int) bool {
	cluster := groups[gi].cluster
	if cluster == "" {
		return false
	}
	others := 0
	for oi := range groups {
		o := &groups[oi]
		if oi == gi || o.cluster != cluster {
			continue
		}
		if !o.answered || o.data {
			return false
		}
		others++
	}
	return others != 0
}
