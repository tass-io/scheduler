package qps

import (
	"sync"
	"time"
)

type Manager struct {
	periodMicros     int64
	nextPeriodMicros int64
	currentPermits   int64

	start time.Time
	mutex sync.Mutex
}

func newQPSManager(periodMs int64) *Manager {
	l := &Manager{
		periodMicros: periodMs * int64(time.Millisecond),
		start:        time.Now(),
	}
	l.nextPeriodMicros = int64(time.Since(l.start)) + l.periodMicros
	return l
}

func (l *Manager) refresh() {
	var nowMicros = int64(time.Since(l.start))
	if nowMicros >= l.nextPeriodMicros {
		l.nextPeriodMicros = ((nowMicros-l.nextPeriodMicros)/l.periodMicros+1)*l.periodMicros + l.nextPeriodMicros
		l.currentPermits = 0
	}
}

// TryAcquire limit
func (l *Manager) Inc() int64 {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.refresh()
	l.currentPermits++
	return l.currentPermits
}

func (l *Manager) Get() int64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.refresh()
	return l.currentPermits
}
