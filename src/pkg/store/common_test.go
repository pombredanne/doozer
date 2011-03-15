package store

func mustWait(st *Store, rev int64) <-chan Event {
	ch, err := st.Wait(rev)
	if err != nil {
		panic(err)
	}
	return ch
}

func mustWatch(st *Store, glob *Glob, rev int64) <-chan Event {
	ch, err := st.Watch(glob, rev)
	if err != nil {
		panic(err)
	}
	return ch
}

func mustWatchOn(st *Store, glob *Glob, c chan Event, from, to int64) *Watch {
	w, err := st.watchOn(glob, c, from, to)
	if err != nil {
		panic(err)
	}
	return w
}

func mustNewWatch(st *Store, glob *Glob, from int64) *Watch {
	w, err := NewWatch(st, glob, from)
	if err != nil {
		panic(err)
	}
	return w
}
