package backend

import (
	"context"
	"errors"
	"net"
	"net/http"

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
	clusters, sizes, membership := replicaGroups(backends)
	groups := make([]renderGroup, len(clusters))
	for i := range groups {
		groups[i] = renderGroup{n: sizes[i], cluster: clusters[i],
			results: make(chan []types.Metric, sizes[i]), errors: make(chan error, sizes[i])}
	}
	return groups, membership
}

// replicaGroups numbers the replica groups, keyed by (dc, cluster), of the
// selected backends. It returns each group's cluster and size and each
// backend's group.
func replicaGroups(backends []Backend) (clusters []string, sizes, membership []int) {
	membership = make([]int, len(backends))
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
			g = len(clusters)
			byGroup[key] = g
			clusters = append(clusters, cluster)
			sizes = append(sizes, 0)
		}
		sizes[g]++
		membership[i] = g
	}
	return clusters, sizes, membership
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
// answered group's errors become not-found, and so do temporary availability failures of
// a group whose cluster's other replica groups all answered not-found
// (clusterNotFound). Every other error is returned as the backend reported it.
func renderGroupErrors(groups []renderGroup) []error {
	var errs []error
	for gi := range groups {
		g := &groups[gi]
		notFound := !g.unrecovered()
		unavailableNotFound := !notFound && clusterNotFound(groups, gi)
		for _, err := range g.errs {
			switch {
			case notFound, unavailableNotFound && replicaUnavailable(err):
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

// replicaUnavailable identifies availability failures that a definitive
// not-found from every other replica group of this cluster can cover. Keep
// decoding failures and arbitrary server errors visible: they can indicate
// corruption rather than a replica temporarily unable to serve the request.
func replicaUnavailable(err error) bool {
	if connectFailure(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var code interface{ HTTPStatusCode() int }
	if errors.As(err, &code) && code.HTTPStatusCode() == http.StatusServiceUnavailable {
		return true
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

// clusterNotFound reports whether this cluster has another selected replica
// group and every other group answered without data. Only replies for this
// exact fetch count. This lets a temporarily unavailable replica share the
// definitive not-found of its peers without hiding an entirely failed cluster,
// unrelated clusters, or failures such as malformed response bodies.
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
