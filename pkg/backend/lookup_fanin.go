package backend

import (
	"errors"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// lookupGroup holds the find or info replies of one replica group.
type lookupGroup struct {
	cluster  string
	n        int // selected members
	answered int // members that returned matches or info
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
				g.answered++
			case err := <-errChs[gi]:
				g.errs = append(g.errs, err)
			}
		}
	}
	return answers, lookupGroupErrors(groups, notFound)
}

// lookupGroupErrors returns every error of the groups. The connect failures
// (refused, reset or dial timeout, before any response) of a cluster become
// notFound when the cluster's reachable stores all answered not-found
// (clusterReachableNotFound). A stopped store then reads as the not-found that
// its cluster's other stores gave, instead of turning a find that matches
// nothing into HTTP 422 and an info into HTTP 500.
func lookupGroupErrors(groups []lookupGroup, notFound types.ErrNotFound) []error {
	var errs []error
	for gi := range groups {
		covered := clusterReachableNotFound(groups, groups[gi].cluster)
		for _, err := range groups[gi].errs {
			if covered && connectFailure(err) {
				err = notFound
			}
			errs = append(errs, err)
		}
	}
	return errs
}

// clusterReachableNotFound reports whether every reachable store of cluster
// answered with its own not-found: at least one member answered not-found, no
// member returned matches or info, and every other member failed to connect.
// A store can be stopped in every replica group at once, for example the
// retired stores of a cluster being resized, so requiring one replica group to
// answer in full would fail every lookup that matches nothing.
//
// The errors stay when no store of the cluster answered (an unreachable
// cluster still fails the lookup), when any member failed in another way (a
// server error, or a timeout after the store accepted the request), and for
// ungrouped backends, which never recover each other.
func clusterReachableNotFound(groups []lookupGroup, cluster string) bool {
	if cluster == "" {
		return false
	}
	notFound := 0
	for gi := range groups {
		g := &groups[gi]
		if g.cluster != cluster {
			continue
		}
		if g.answered != 0 {
			return false
		}
		for _, err := range g.errs {
			var nf types.ErrNotFound
			switch {
			case errors.As(err, &nf):
				notFound++
			case !connectFailure(err):
				return false
			}
		}
	}
	return notFound != 0
}
