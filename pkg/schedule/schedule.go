package schedule

// Scheduler will adjust function instances num with upstream expectation
type Scheduler interface {
	Refresh(functionName string, target int)
}

var GetScheduler = func() Scheduler {
	return nil
}

func Register(f func() Scheduler) {
	GetScheduler = f
}
