package helper

import (
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/function_scheduler"
)

var NewRunner = func() runner.Runner {
	return function_scheduler.GetFunctionScheduler()
}
