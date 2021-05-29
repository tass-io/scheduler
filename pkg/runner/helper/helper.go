package helper

import (
	"github.com/tass-io/scheduler/pkg/runner"
)

var GetMasterRunner = func() runner.Runner {
	return nil
}

func Register(f func() runner.Runner) {
	GetMasterRunner = f
}
