package schedule

// Scheduler will adjust function instances num with upstream expectation
type Scheduler interface {
	Refresh(functionName string, target int)
}

var GetScheduler = func() Scheduler {
	return nil
}

// Register replaces the GetScheduler function with the param
// it's called in the FunctionSchedulerInit method
func Register(f func() Scheduler) {
	GetScheduler = f
}
