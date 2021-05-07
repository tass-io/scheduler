package helper

import (
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/fnscheduler"
)

var NewRunner = func() runner.Runner {
	return fnscheduler.GetFunctionScheduler()
}
