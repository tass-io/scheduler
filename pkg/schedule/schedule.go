package schedule

// Scheduler is responsible for scheduling the function process instances
type Scheduler interface {
	// Refresh adjusts function instances number(param2) with upstream events
	Refresh(functionName string, target int)
	// ColdStartDone returns when the function process (param1) cold start stage is finished
	ColdStartDone(functionName string)
	// NewInstanceSetIfNotExist creates a not existed instance set struct for the function(param1)
	NewInstanceSetIfNotExist(functionName string)
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
