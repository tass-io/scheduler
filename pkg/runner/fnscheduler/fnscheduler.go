package fnscheduler

import (
	"sync"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/errorutils"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	"go.uber.org/zap"
)

var (
	once = &sync.Once{}
	fs   *FunctionScheduler
)

func GetFunctionScheduler() *FunctionScheduler {
	once.Do(FunctionSchedulerInit)
	return fs
}

func FunctionSchedulerInit() {
	if viper.GetBool(env.Mock) {
		NewInstance = func(functionName string) instance.Instance {
			return instance.NewMockInstance(functionName)
		}
	}
	fs = NewFunctionScheduler()
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
}

func (s *set) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	if s.stats() > 0 {
		// warm start, try to find a lowest latency process to work
		s.Lock()
		process := ChooseTargetInstance(s.instances)
		s.Unlock()
		return process.Invoke(parameters)
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
			}
		}
	}
}

func newSet(functionName string) *set {
	return &set{
		Locker:       &sync.Mutex{},
		functionName: functionName,
		instances:    []instance.Instance{},
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
	ins, existed := fs.instances[functionName]
	if !existed {
		fs.Lock()
		fs.instances[functionName] = newSet(functionName)
		ins = fs.instances[functionName]
		fs.Unlock()
	}
	ins.Scale(target, functionName)
	fs.instances[functionName] = ins
	fs.trigger <- struct{}{}
}

// Sync Function Scheduler info to api server via LSDS
func (fs *FunctionScheduler) sync() {
	for _ = range fs.trigger {
		syncMap := make(map[string]int, len(fs.instances))
		fs.Lock()
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
	return true
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
	stats := runner.InstanceStatus{}
	for functionName, s := range fs.instances {
		stats[functionName] = s.Stats()
	}
	return stats
}
