package local

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/errorutils"
)

type FunctionScheduler struct {
	sync.Locker
	runner.Runner
	instances map[string]*set
}

type set struct {
	sync.Locker
	instances []*instance
}

func NewFunctionScheduler() *FunctionScheduler {
	return &FunctionScheduler{
		instances: make(map[string]*set, 10),
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
	fs.Lock()
	target, existed := fs.instances[span.FunctionName]
	if !existed {
		// cold start, create target and create process
		target = &set{
			Locker:    &sync.Mutex{},
			instances: []*instance{},
		}
	}
	// take care of the lock order, the set lock is before fs unlock
	target.Lock()
	defer target.Unlock()
	fs.instances[span.FunctionName] = target
	fs.Unlock()
	if len(target.instances) > 0 {
		// warm start, try to find a lowest latency process to work
		process := ChooseTargetInstance(target.instances)
		return process.Invoke(parameters)
	} else {
		if fs.canCreate() {
			// cold start, create a new process to handle request
			newInstance := NewInstance(span.FunctionName)
			newInstance.Start()
			target.instances = append(target.instances, newInstance)
			newInstance.Invoke(parameters)
		} else {
			// resource limit, return error
			return nil, &errorutils.ResourceLimitError{}
		}
	}
	return
}
