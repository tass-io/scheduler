package fnscheduler

import (
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/runner/lsds"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/errorutils"
)

var (
	once = &sync.Once{}
	fs *FunctionScheduler
)

func GetFunctionScheduler() *FunctionScheduler {
	once.Do(FunctionSchedulerInit)
	return fs
}

func FunctionSchedulerInit() {
	fs = NewFunctionScheduler()
	go fs.Sync()
}

// FunctionScheduler implements Runner and Scheduler
type FunctionScheduler struct {
	sync.Locker
	runner.Runner
	instances map[string]*set
}

type set struct {
	sync.Locker
	instances []instance.Instance
}

func newSet() *set {
	return &set{
		Locker:    &sync.Mutex{},
		instances: []instance.Instance{},
	}
}

func NewFunctionScheduler() *FunctionScheduler {
	// todo context architecture
	return &FunctionScheduler{
		Locker: &sync.Mutex{},
		instances: make(map[string]*set, 10),
	}
}

func (fs *FunctionScheduler) Refresh(functionName string, target int) {
	ins, existed := fs.instances[functionName]
	if !existed {
		zap.S().Warnw("function scheduler refresh warning for function set not found", "function", functionName)
		return
	}
	ins.Lock()
	defer ins.Unlock()
	l := len(ins.instances)
	if l > target {
		// scale down
		// todo policy
		cut := ins.instances[:l-target]
		rest := ins.instances[l-target:]
		for _, i := range cut {
			i.Release()
		}
		ins.instances = rest
	} else if l < target {
		// scale up
		for i := 0; i < target-l; i++ {
			if fs.canCreate() {
				newIns := NewInstance(functionName)
				zap.S().Infow("function scheduler creates instance", "instance", newIns)
				err := newIns.Start()
				if err != nil {
					zap.S().Warnw("function scheduler start instance error", "err", err)
					continue
				}
				ins.instances = append(ins.instances, newIns)
			}
		}
	}
}

// Sync Function Scheduler info to api server via LSDS
func (fs *FunctionScheduler) Sync() {
	for {
		syncMap := make(map[string]int, len(fs.instances))
		fs.Lock()
		for functionName, ins := range fs.instances {
			syncMap[functionName] = len(ins.instances)
		}
		fs.Unlock()
		lsds.GetLSDSIns().Sync(syncMap)
		time.Sleep(100 * time.Millisecond)
	}
}

// canCreate is a policy for determining whether function instance creation is still possible
// todo check how to get metrics
// todo policy architecture
func (fs *FunctionScheduler) canCreate() bool {
	return true
}

// FunctionScheduler.Run will choose a target instance to run
// now with middleware and events, fs run will be simplified
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
		return nil, errorutils.NewNoInstanceError(span.FunctionName)
		// if fs.canCreate() {
		// 	// cold start, create a new process to handle request
		// 	newInstance := NewInstance(span.FunctionName)
		// 	err = newInstance.Start()
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	target.instances = append(target.instances, newInstance)
		// 	target.Unlock()
		// 	result, err = newInstance.Invoke(parameters)
		// } else {
		// 	// resource limit, return error
		// 	return nil, ResourceLimitError
		// }
	}
}

// todo add policy architecture
func ChooseTargetInstance(instances []instance.Instance) (target instance.Instance) {
	max := runner.SCORE_MAX
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
var NewInstance = func(functionName string) instance.Instance {
	return instance.NewProcessInstance(functionName)
}

func (fs *FunctionScheduler) Stats() runner.InstanceStatus {
	fs.Lock()
	defer fs.Unlock()
	stats := runner.InstanceStatus{}
	for functionName, s := range fs.instances {
		s.Lock()
		stats[functionName] = len(s.instances)
		s.Unlock()
	}
	return stats
}
