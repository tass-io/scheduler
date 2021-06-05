package helper

import (
	"github.com/tass-io/scheduler/pkg/runner"
)

// GetMasterRunner returns a Runner
// Note that the function scheduler implements runner.Runner and schedule.Scheduler interface,
// while lsds implements only runner.Runner interface.
// This method is expected to be easy to change when testing
var GetMasterRunner = func() runner.Runner {
	return nil
}

// Register registers a runner.Runner,
// it actually changes the implementation of GetMasterRunner
func Register(f func() runner.Runner) {
	GetMasterRunner = f
}
