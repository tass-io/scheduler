package fnscheduler

import (
	"sync"

	"github.com/avast/retry-go"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/runner/ttl"
	"github.com/tass-io/scheduler/pkg/schedule"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/errorutils"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	"go.uber.org/zap"
)

var (
	fs                *FunctionScheduler
	canCreatePolicies = map[string]func() bool{
		"default": DefaultCanCreatePolicy,
	}
)

// DefaultCanCreatePolicy is a policy to judge whether to create a new process or not
// DefaultCanCreatePolicy always returns true
func DefaultCanCreatePolicy() bool {
	return true
}

// GetFunctionScheduler returns a FunctionScheduler pointer
// this is for global get FunctionScheduler
func GetFunctionScheduler() *FunctionScheduler {
	return fs
}

// FunctionSchedulerInit inits a new FunctionScheduler,
// it also prepares environments of the function scheduler
func FunctionSchedulerInit() {
	if viper.GetBool(env.Mock) {
		NewInstance = func(functionName string) instance.Instance {
			return instance.NewMockInstance(functionName)
		}
	}
	fs = newFunctionScheduler()
	// schedule.Register and helper.Register are all for decouple the logic
	// so it can be easy to test
	schedule.Register(func() schedule.Scheduler {
		return fs
	})
	helper.Register(func() runner.Runner {
		return fs
	})
	go fs.sync()
}

// FunctionScheduler implements Runner and Scheduler interface
type FunctionScheduler struct {
	sync.Locker
	runner.Runner
	schedule.Scheduler
	// instances records all process instances of each Function
	instances map[string]*set
	trigger   chan struct{}
}

// set is the status of the function and its instances
// todo take care of terminated instances clean
type set struct {
	sync.Locker
	functionName string
	instances    []instance.Instance
	ttl          *ttl.TTLManager
}

// Invoke is a set-level invocation
// it finds a lowest score process to run the function
// if no available processes, it returns en error
func (s *set) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	if s.stats() > 0 {
		// warm start, try to find a lowest latency process to work
		var result map[string]interface{}
		var err error
		err = retry.Do(
			func() error {
				s.Lock()
				process := ChooseTargetInstance(s.instances)
				s.Unlock()
				err = s.resetInstanceTimer(process)
				zap.S().Infow("reset timer", "instance", process)
				if err != nil {
					zap.S().Warnw("reset timer failed:", "err", err)
				}
				result, err = process.Invoke(parameters)
				if err != nil {
					zap.S().Debugw("retry in invoke err", "err", err)
				}
				return err
			},
			retry.RetryIf(func(err error) bool {
				// this retry to avoid this case:
				// one request choose the process when it is Running
				// after the chosen and before the reset, the process Released
				// the process will return instance.InstanceNotServiceErr to describe this case.
				// todo thinking about the request is unlucky to retry at the fourth time.
				return err == instance.ErrInstanceNotService
			}),
			retry.Attempts(3),
		)
		return result, err
	} else {
		return nil, errorutils.NewNoInstanceError(s.functionName)
	}
}

// resetInstanceTimer resets the timer of a process instance
func (s *set) resetInstanceTimer(process instance.Instance) error {
	return s.ttl.ResetInstanceTimer(process)
}

// stats returns the alive number of instances
func (s *set) stats() int {
	alive := 0
	for _, ins := range s.instances {
		if ins.IsRunning() {
			alive++
		}
	}
	return alive
}

// Stats is the public method of the stats
func (s *set) Stats() int {
	s.Lock()
	defer s.Unlock()
	return s.stats()
}

// Scale reads the target and does process-level scaling.
// It's called when Refresh
func (s *set) Scale(target int, functionName string) {
	s.Lock()
	defer s.Unlock()
	l := len(s.instances)
	if l > target {
		zap.S().Debugw("set scale down", "function", s.functionName)
		// scale down
		// select the instances with the lowest scores
		// get scores
		// the ins.Score is the most important part
		scores := []int{}
		for _, ins := range s.instances {
			scores = append(scores, ins.Score())
		}
		targetIndexes := []int{}
		for rest := 0; rest < l-target; rest++ {
			smallest := 99999
			index := -1
			for i, score := range scores {
				if score < smallest {
					index = i
					smallest = score
				}
			}
			if index == -1 {
				zap.S().Warnw("cannot find one instance")
			} else {
				scores = append(scores[:index], scores[index+1:]...)
				targetIndexes = append(targetIndexes, index)
			}
		}
		for _, targetIndex := range targetIndexes {
			ins := s.instances[targetIndex]
			s.instances = append(s.instances[:targetIndex], s.instances[targetIndex+1:]...)
			ins.Release()
			zap.S().Debugw("fnscheduler release instance")
			s.ttl.Release(ins)
		}
	} else if l < target {
		zap.S().Debugw("set scale up", "function", s.functionName)
		// scale up
		for i := 0; i < target-l; i++ {
			if fs.canCreate() {
				newIns := NewInstance(functionName)
				zap.S().Infow("function scheduler creates instance", "instance", newIns)
				if newIns == nil {
					zap.S().Warn("instance is nil")
					return
				}
				err := newIns.Start()
				if err != nil {
					zap.S().Warnw("function scheduler start instance error", "err", err)
					continue
				}
				s.instances = append(s.instances, newIns)
				s.ttl.Append(newIns)
			}
		}
	}
}

// newSet returns a new set for the input function
func newSet(functionName string) *set {
	return &set{
		Locker:       &sync.Mutex{},
		functionName: functionName,
		instances:    []instance.Instance{},
		ttl:          ttl.NewTTLManager(functionName),
	}
}

// newFunctionScheduler inits a new FunctionScheduler.
// By default, the capacity of process instances is 10 and the trigger channel length is 100
func newFunctionScheduler() *FunctionScheduler {
	// todo context architecture
	return &FunctionScheduler{
		Locker:    &sync.Mutex{},
		instances: make(map[string]*set, 10),
		trigger:   make(chan struct{}, 100),
	}
}

// Refresh refreshes information of instances and does scaling.
// Everytime when ScheduleHandler recieves a new event for upstream,
// it decides the status of the instance, and calls Refresh.
func (fs *FunctionScheduler) Refresh(functionName string, target int) {
	zap.S().Debugw("refresh")
	fs.Lock()
	ins, existed := fs.instances[functionName]
	if !existed {
		fs.instances[functionName] = newSet(functionName)
		ins = fs.instances[functionName]
	}
	fs.Unlock()
	ins.Scale(target, functionName)
	fs.trigger <- struct{}{}
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

// canCreate is a policy for determining whether function instance creation is still possible
// todo check how to get metrics
// todo policy architecture
func (fs *FunctionScheduler) canCreate() bool {
	return canCreatePolicies[viper.GetString(env.CreatePolicy)]()
}

// Run chooses a target instance to run
// now with middleware and events, fs run can be simplified
func (fs *FunctionScheduler) Run(span *span.Span, parameters map[string]interface{}) (result map[string]interface{}, err error) {
	fs.Lock()
	functionName := span.GetFunctionName()
	target, existed := fs.instances[functionName]
	if !existed {
		// when the Function comes to this method, it has gone through the middleware
		// so here if there is no funcionName map, it just means that this is the first HTTP request.
		// The middleware has sent a Event for this Function, no matter it is created successfully or not.
		// cold start, create target and create process
		target = newSet(functionName)
	}
	// take care of the lock order, the set lock is before fs unlock
	fs.instances[functionName] = target
	fs.Unlock()
	return target.Invoke(parameters)
}

// ChooseTargetInstance chooses a target instance which has the lowest score
func ChooseTargetInstance(instances []instance.Instance) (target instance.Instance) {
	min := runner.SCORE_MAX
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

// NewInstance creates a new process instance.
// This method is extracted as a helper function to mock instance creation in test injection.
var NewInstance = func(functionName string) instance.Instance {
	return instance.NewProcessInstance(functionName)
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
