package fnscheduler

import (
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/collector"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/schedule"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"
	"go.uber.org/zap"
)

var (
	fs *FunctionScheduler
)

var _ runner.Runner = &FunctionScheduler{}
var _ schedule.Scheduler = &FunctionScheduler{}

// FunctionScheduler implements Runner and Scheduler interface
type FunctionScheduler struct {
	sync.Locker
	runner.Runner
	schedule.Scheduler
	// instances records all process instances of each Function
	instances map[string]*instanceSet
	trigger   chan struct{}
}

// GetFunctionScheduler returns a FunctionScheduler pointer
// this is for global get FunctionScheduler
func GetFunctionScheduler() *FunctionScheduler {
	return fs
}

// Init inits a new FunctionScheduler,
// it also prepares environments of the function scheduler
func Init() {
	// if use mock mode, it doesn't create real process
	if viper.GetBool(env.Mock) {
		NewInstance = func(functionName string) instance.Instance {
			return instance.NewMockInstance(functionName)
		}
	}
	fs = &FunctionScheduler{
		Locker:    &sync.Mutex{},
		instances: make(map[string]*instanceSet, 10),
		trigger:   make(chan struct{}, 100),
	}
	// schedule.Register and helper.Register are all for decouple the logic, so it can be easy to test
	schedule.Register(func() schedule.Scheduler {
		return fs
	})
	helper.Register(func() runner.Runner {
		return fs
	})
	go fs.sync()
}

// sync syncs Function Scheduler intances info to api server via k8sutils
func (fs *FunctionScheduler) sync() {
	for range fs.trigger {
		fs.Lock()
		syncMap := make(map[string]int, len(fs.instances))
		for functionName, ins := range fs.instances {
			syncMap[functionName] = len(ins.instances)
		}
		fs.Unlock()
		k8sutils.Sync(syncMap)
	}
}

// canCreateInstance is a policy for determining whether function instance creation is possible
// todo policy architecture
func (fs *FunctionScheduler) canCreateInstance() bool {
	return canCreatePolicies[viper.GetString(env.CreatePolicy)]()
}

// runner.Runner interface implementation
//

// Run chooses a target instance to run
// now with middleware and events, fs run can be simplified
func (fs *FunctionScheduler) Run(span *span.Span, parameters map[string]interface{}) (map[string]interface{}, error) {
	fs.Lock()
	functionName := span.GetFunctionName()
	flowName := span.GetFlowName()
	target, existed := fs.instances[functionName]
	if !existed {
		// when the Function comes to this method, it has gone through the middleware
		// so here if there is no funcionName map, it just means that this is the first HTTP request.
		// The middleware has sent a Event for this Function, no matter it is created successfully or not.
		// cold start, create target and create process
		zap.S().Panicw("instance set not initialized", "function", functionName)
	}
	// take care of the lock order, the set lock is before fs unlock
	fs.instances[functionName] = target
	fs.Unlock()
	start := time.Now()
	result, err := target.Invoke(parameters)
	collector.GetCollector().Record(flowName, functionName, collector.RecordExec, time.Since(start))
	return result, err
}

// ChooseTargetInstance chooses a target instance which has the lowest score
func ChooseTargetInstance(instances []instance.Instance) (target instance.Instance) {
	min := runner.MaxScore
	for _, item := range instances {
		if item.IsRunning() {
			score := item.Score()
			if min > score {
				min = score
				target = item
			}
		}
	}
	return
}

// Stats records the instances status that fnscheduler manages.
// it returns a InstanceStatus map which the key is function name and the value is process number
func (fs *FunctionScheduler) Stats() runner.InstanceStatus {
	stats := runner.InstanceStatus{}
	fs.Lock()
	for functionName, s := range fs.instances {
		stats[functionName] = s.Stats()
	}
	fs.Unlock()
	return stats
}

// FunctionStats returns the current running instances numbers for the given function (param1)
// if the instanceSet for the function doesn't exist, it returns 0
func (fs *FunctionScheduler) FunctionStats(functionName string) int {
	set, ok := fs.instances[functionName]
	if !ok {
		return 0
	}
	return set.Stats()
}

// 	schedule.Scheduler interface implementation
//

// Refresh refreshes information of instances and does scaling.
// Everytime when ScheduleHandler receives a new event for upstream,
// it decides the status of the instance, and calls Refresh.
func (fs *FunctionScheduler) Refresh(functionName string, target int) {
	zap.S().Debugw("refresh", "function", functionName)

	ins, existed := fs.instances[functionName]
	if !existed {
		zap.S().Panicw("instance set not initialized", "function", functionName)
	}

	ins.Scale(target, functionName)
	// FIXME: when trigger, the process status may not still running, update the logic here
	fs.trigger <- struct{}{}
}

// ColdStartDone returns when the instace cold start stage of the function (param1) is done
func (fs *FunctionScheduler) ColdStartDone(functionName string) {
	ins, existed := fs.instances[functionName]
	if !existed {
		zap.S().Panicw("instance set not initialized", "function", functionName)
	}
	ins.functionColdStartDone()
}

// NewInstanceSetIfNotExist creates a new instance set structure for the given function (param1)
func (fs *FunctionScheduler) NewInstanceSetIfNotExist(functionName string) {
	// the lock guarantees that if many cold start requests at the same time,
	// the second request doesn't create a repeated instance set
	fs.Lock()
	defer fs.Unlock()

	_, existed := fs.instances[functionName]
	if !existed {
		newSet := newInstanceSet(functionName)
		fs.instances[functionName] = newSet
	}
}
