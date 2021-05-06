package schedule

import (
	"github.com/tass-io/scheduler/pkg/runner/function_scheduler"
)

// Scheduler will adjust function instances num with upstream expectation
type Scheduler interface {
	Refresh(functionName string, target int)
}

var GetScheduler = func() Scheduler {
	return function_scheduler.GetFunctionScheduler()
}
