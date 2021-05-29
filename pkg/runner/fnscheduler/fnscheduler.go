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

func DefaultCanCreatePolicy() bool {
	return true
}

// For global get FunctionScheduler
func GetFunctionScheduler() *FunctionScheduler {
	return fs
}

func FunctionSchedulerInit() {
	if viper.GetBool(env.Mock) {
		NewInstance = func(functionName string) instance.Instance {
			return instance.NewMockInstance(functionName)
		}
	}
	fs = NewFunctionScheduler()
	schedule.Register(func() schedule.Scheduler {
		return fs
	})
	helper.Register(func() runner.Runner {
		return fs
	})
	go fs.sync()
}

// FunctionScheduler implements Runner and Scheduler
type FunctionScheduler struct {
	sync.Locker
	runner.Runner
	instances map[string]*set
	trigger   chan struct{}
}

// todo take care of terminated instances clean
type set struct {
	sync.Locker
	functionName string
	instances    []instance.Instance
	ttl          *ttl.TTLManager
}

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
				result, err = process.Invoke(parameters)
				if err != nil {
					zap.S().Debugw("retry in invoke err", "err", err)
				}
				return err
			},
			retry.RetryIf(func(err error) bool {
				// this retry to avoid this case:
				// one request choose the process when it is Running
				// after the choose, the process Released
				// the process will return instance.InstanceNotServiceErr to describe this case.
				// todo thinking about the request is unlucky to retry at the fourth time.
				if err == instance.InstanceNotServiceErr {
					return true
				}
				return false
			}),
			retry.Attempts(3),
		)
		return result, err
	} else {
		return nil, errorutils.NewNoInstanceError(s.functionName)
	}
}

func (s *set) stats() int {
	alive := 0
	for _, ins := range s.instances {
		if ins.IsRunning() {
			alive++
		}
	}
	return alive
}

func (s *set) Stats() int {
	s.Lock()
	defer s.Unlock()
	return s.stats()
}

func (s *set) Scale(target int, functionName string) {
	s.Lock()
	defer s.Unlock()
	l := len(s.instances)
	if l > target {
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
			ins.Release()
			s.ttl.Release(ins)
		}
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
				s.instances = append(s.instances, newIns)
				s.ttl.Append(newIns)
			}
		}
	}
}

func newSet(functionName string) *set {
	return &set{
		Locker:       &sync.Mutex{},
		functionName: functionName,
		instances:    []instance.Instance{},
		ttl:          ttl.NewTTLManager(functionName),
	}
}

func NewFunctionScheduler() *FunctionScheduler {
	// todo context architecture
	return &FunctionScheduler{
		Locker:    &sync.Mutex{},
		instances: make(map[string]*set, 10),
		trigger:   make(chan struct{}, 100),
	}
}

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

// Sync Function Scheduler info to api server via k8sutils
func (fs *FunctionScheduler) sync() {
	for _ = range fs.trigger {
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

// FunctionScheduler.Run will choose a target instance to run
// now with middleware and events, fs run will be simplified
func (fs *FunctionScheduler) Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error) {
	fs.Lock()
	target, existed := fs.instances[span.FunctionName]
	if !existed {
		// cold start, create target and create process
		target = newSet(span.FunctionName)
	}
	// take care of the lock order, the set lock is before fs unlock
	fs.instances[span.FunctionName] = target
	fs.Unlock()
	return target.Invoke(parameters)
}

// choose target by instance score
func ChooseTargetInstance(instances []instance.Instance) (target instance.Instance) {
	max := runner.SCORE_MAX
	for _, item := range instances {
		if item.IsRunning() {
			score := item.Score()
			if max > score {
				max = score
				target = item
			}
		}
	}
	return
}

// add test inject point here
var NewInstance = func(functionName string) instance.Instance {
	return instance.NewProcessInstance(functionName)
}

func (fs *FunctionScheduler) Stats() runner.InstanceStatus {
	stats := runner.InstanceStatus{}
	fs.Lock()
	for functionName, s := range fs.instances {
		stats[functionName] = s.Stats()
	}
	fs.Unlock()
	return stats
}
