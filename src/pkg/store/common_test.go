package store

func mustWait(st *Store, rev int64) <-chan Event {
	ch, err := st.Wait(rev)
	if err != nil {
		panic(err)
	}
	return ch
}
