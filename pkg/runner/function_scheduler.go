package runner

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tass-io/scheduler/pkg/span"
)

var (
	ResourceLimitError              = errors.New("resource limit")
	TargetInstanceType InstanceType = ProcessInstance
)

const SCORE_MAX = 9999

type FunctionScheduler struct {
	sync.Locker
	Runner
	instances map[string]*set
	lsds      *LSDS
}

type set struct {
	sync.Locker
	instances []instance
}

func newSet() *set {
	return &set{
		Locker:    &sync.Mutex{},
		instances: []instance{},
	}
}

func NewFunctionScheduler() *FunctionScheduler {
	// todo context architecture
	return &FunctionScheduler{
		instances: make(map[string]*set, 10),
		Runner:    NewLSDS(context.Background()),
	}
}

func (fs *FunctionScheduler) Sync() {
	for {
		syncMap := make(map[string]int, len(fs.instances))
		fs.Lock()
		for functionName, ins := range fs.instances {
			syncMap[functionName] = len(ins.instances)
		}
		fs.Unlock()
		fs.lsds.Sync(syncMap)
		time.Sleep(100 * time.Millisecond)
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
		target = newSet()
	}
	// take care of the lock order, the set lock is before fs unlock
	fs.instances[span.FunctionName] = target
	target.Lock()
	fs.Unlock()
	if len(target.instances) > 0 {
		// warm start, try to find a lowest latency process to work
		process := ChooseTargetInstance(target.instances)
		target.Unlock()
		return process.Invoke(parameters)
	} else {
		if fs.canCreate() {
			// cold start, create a new process to handle request
			newInstance := NewInstance(span.FunctionName)
			err = newInstance.Start()
			if err != nil {
				return nil, err
			}
			target.instances = append(target.instances, newInstance)
			target.Unlock()
			result, err = newInstance.Invoke(parameters)
		} else {
			// resource limit, return error
			return nil, ResourceLimitError
		}
	}
	return
}

func ChooseTargetInstance(instances []instance) (target instance) {
	max := SCORE_MAX
	for _, i := range instances {
		score := i.Score()
		if max > i.Score() {
			max = score
			target = i
		}
	}
	return
}

// add test inject point here
var NewInstance = func(functionName string) instance {
	return NewProcessInstance(functionName)
}
