package consensus

import (
	"doozer/store"
	"sort"
)


type run struct {
	seqn  int64
	cals  []string
	addrs map[string]bool

	coordinator coordinator
	acceptor    acceptor
	learner     learner

	out chan packet
}


func (r *run) Deliver(p packet) {
	m := r.coordinator.Deliver(p)
	if m != nil {
		r.out <- packet{M: *m}
	}

	m = r.acceptor.Put(&p.M)
	if m != nil {
		r.out <- packet{M: *m}
	}

	r.learner.Deliver(p)
}


func (r *run) broadcast(m *M) {
	for addr := range r.addrs {
		r.out <- packet{addr, *m}
	}
}


func GenerateRuns(alpha int64, w <-chan store.Event, runs chan<- *run) {
	for e := range w {
		runs <- &run{
			seqn:  e.Seqn + alpha,
			cals:  getCals(e),
			addrs: getAddrs(e),
		}
	}
}

func getCals(g store.Getter) []string {
	slots := store.Getdir(g, "/doozer/slot")
	cals := make([]string, len(slots))

	for i, slot := range slots {
		cals[i] = store.GetString(g, "/doozer/slot/"+slot)
	}

	sort.SortStrings(cals)

	return cals
}


func getAddrs(g store.Getter) map[string]bool {
	// TODO include only CALs, once followers use TCP for updates.

	members := store.Getdir(g, "/doozer/info")
	addrs := make(map[string]bool)

	for _, member := range members {
		addrs[store.GetString(g, "/doozer/info/"+member+"/addr")] = true
	}

	return addrs
}