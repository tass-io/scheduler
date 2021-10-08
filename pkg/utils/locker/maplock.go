package locker

import "sync"

type MapLocker struct {
	lockers map[string]sync.Locker
}

func NewMapLocker() *MapLocker {
	return &MapLocker{
		lockers: make(map[string]sync.Locker),
	}
}

func (ml *MapLocker) Lock(key string) {
	if _, ok := ml.lockers[key]; ok {
		ml.lockers[key].Lock()
	} else {
		ml.lockers[key] = &sync.Mutex{}
		ml.lockers[key].Lock()
	}
}

func (ml *MapLocker) Unlock(key string) {
	ml.lockers[key].Unlock()
}
