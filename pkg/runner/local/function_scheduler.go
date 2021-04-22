package local

import (
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
)

type FunctionScheduler struct {
	runner.Runner
	instances map[string][]*instance
}

func NewFunctionScheduler() *FunctionScheduler {
	return &FunctionScheduler{
		instances: make(map[string][]*instance, 10),
	}
}

// canCreate is a policy for determining whether function instance creation is still possible
// todo check how to get metrics
// todo policy architecture
func (fs *FunctionScheduler) canCreate() bool {
	return true
}

// FunctionScheduler.Run will find/create a process to handle the function
func (fs *FunctionScheduler) Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error) {
	return
}
