package backend

import (
	"errors"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// lookupGroup holds the find or info replies of one replica group.
type lookupGroup struct {
	cluster  string
	n        int // selected members
	notFound int // members that answered with their own not-found
	errs     []error
}

// fanOut sends one find or info request per backend through send and waits
// for every reply. It returns the answers and every error, with the connect
// failures that lookupGroupErrors recovers replaced by notFound.
func fanOut[T any](backends []Backend, notFound types.ErrNotFound,
	send func(b Backend, results chan T, errs chan error)) ([]T, []error) {
	clusters, sizes, membership := replicaGroups(backends)
	groups := make([]lookupGroup, len(clusters))
	results := make([]chan T, len(clusters))
	errChs := make([]chan error, len(clusters))
	for gi := range groups {
		groups[gi] = lookupGroup{cluster: clusters[gi], n: sizes[gi]}
		results[gi] = make(chan T, sizes[gi])
		errChs[gi] = make(chan error, sizes[gi])
	}
	for i, b := range backends {
		send(b, results[membership[i]], errChs[membership[i]])
	}

	answers := make([]T, 0, len(backends))
	for gi := range groups {
		g := &groups[gi]
		for range g.n {
			select {
			case answer := <-results[gi]:
				answers = append(answers, answer)
			case err := <-errChs[gi]:
				g.errs = append(g.errs, err)
				var nf types.ErrNotFound
				if errors.As(err, &nf) {
					g.notFound++
				}
			}
		}
	}
	return answers, lookupGroupErrors(groups, notFound)
}

// lookupGroupErrors returns every error of the groups. A connect failure
// (refused, reset or dial timeout, before any response) becomes notFound when
// another replica group of the same cluster is a complete not-found: every
// selected member of it answered with its own not-found. Replica groups of one
// cluster hold the same names, so that group already says what the unreachable
// store would answer. The fe-112 canary needed this: a /metrics/find matching
// nothing got not-found from 7 stores and a refusal from one cy store, and
// the required-fetch rule (not-found only when every error is a not-found) turned it into
// HTTP 422 instead of the empty 200 it returned before the required-fetch rule.
//
// Every member of the vouching group must answer, not just one as in
// clusterNotFound for renders: the members of a group hold different names, so
// a partly answered group says nothing about the names of its failed member.
// The error stays when no other group of the cluster answered in full (for
// example a one-group cluster, or a store failing in every group), so a
// cluster that cannot be read still fails the lookup. Server errors and
// failures after the backend accepted the request are never converted, and
// ungrouped backends never recover each other.
func lookupGroupErrors(groups []lookupGroup, notFound types.ErrNotFound) []error {
	var errs []error
	for gi := range groups {
		covered := clusterAnsweredNotFound(groups, gi)
		for _, err := range groups[gi].errs {
			if covered && connectFailure(err) {
				err = notFound
			}
			errs = append(errs, err)
		}
	}
	return errs
}

// clusterAnsweredNotFound reports whether another replica group of gi's
// cluster is a complete not-found.
func clusterAnsweredNotFound(groups []lookupGroup, gi int) bool {
	cluster := groups[gi].cluster
	if cluster == "" {
		return false
	}
	for oi := range groups {
		o := &groups[oi]
		if oi != gi && o.cluster == cluster && o.notFound == o.n {
			return true
		}
	}
	return false
}
