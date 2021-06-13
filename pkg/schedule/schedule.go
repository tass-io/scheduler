package schedule

// Scheduler adjusts function instances number with upstream events
type Scheduler interface {
	Refresh(functionName string, target int)
	ColdStartDone(functionName string)
}

// GetScheduler returns a Scheduler
// Note that the function scheduler implements runner.Runner and schedule.Scheduler interface,
// This method is expected to be easy to change when testing
var GetScheduler = func() Scheduler {
	return nil
}

// Register replaces the GetScheduler function with the param
// it's called in the FunctionSchedulerInit method
func Register(f func() Scheduler) {
	GetScheduler = f
}
