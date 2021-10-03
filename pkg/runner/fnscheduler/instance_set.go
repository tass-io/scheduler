package fnscheduler

import (
	"sync"

	"github.com/avast/retry-go"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/runner/ttl"
	"github.com/tass-io/scheduler/pkg/utils/errorutils"
	"go.uber.org/zap"
)

// instanceSet is the status of the function and its instances
// todo take care of terminated instances clean
type instanceSet struct {
	sync.Locker
	functionName  string
	instances     []instance.Instance
	coldStartDone chan struct{}
	accesslimit   chan struct{}
	ttl           *ttl.TTLManager
}

// newInstanceSet returns a new instance set for the input function
func newInstanceSet(functionName string) *instanceSet {
	return &instanceSet{
		Locker:        &sync.Mutex{},
		functionName:  functionName,
		instances:     []instance.Instance{},
		coldStartDone: make(chan struct{}, 1),
		accesslimit:   make(chan struct{}, 1),
		ttl:           ttl.NewTTLManager(functionName),
	}
}

// Invoke is a set-level invocation, it finds a lowest score process to run the function,
// if no available processes, it returns en error.
//
// Invoke is called after middleware, so if there is a cold start case, it has triggered a
// cold start event. Here Invoke assumes that the instance is already running, if no running
// instances, it returns an ErrInstanceNotService error
func (s *instanceSet) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
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
	}
	return nil, errorutils.NewNoInstanceError(s.functionName)
}

// resetInstanceTimer resets the timer of a process instance
func (s *instanceSet) resetInstanceTimer(process instance.Instance) error {
	return s.ttl.ResetInstanceTimer(process)
}

// Stats is the public method of the stats
func (s *instanceSet) Stats() int {
	s.Lock()
	defer s.Unlock()
	return s.stats()
}

// stats returns the alive number of instances
func (s *instanceSet) stats() int {
	alive := 0
	for _, ins := range s.instances {
		if ins.IsRunning() {
			alive++
		}
	}
	return alive
}

// Scale reads the target and does process-level scaling.
// It's called when Refresh
func (s *instanceSet) Scale(target int, functionName string) {
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
			if fs.canCreateInstance() {
				// 1. preprare data structure for the new process instance
				newIns := NewInstance(functionName)
				zap.S().Infow("function scheduler creates instance", "instance", newIns)
				if newIns == nil {
					zap.S().Warn("instance is nil")
					return
				}

				// 2. start the process instance
				err := newIns.Start()
				if err != nil {
					zap.S().Warnw("function scheduler start instance error", "err", err)
					continue
				}

				// 3. Notify the coldStartDone channel when the cold start phase is done
				go func() {
					newIns.InitDone()
					zap.S().Debug("an instance initialization done")
					// this step is important,
					// ensure that we olny send a signal when a cold start stage done
					//
					// the newIns.InitDone() makes sure that the newIns status is running, guarantees
					// the alive number is at least 1.
					alive := s.stats()
					if alive == 1 {
						s.notifyColdStartDone()
						zap.S().Debug("an instance cold start done")
					}
				}()

				s.instances = append(s.instances, newIns)
				s.ttl.Append(newIns)
			}
		}
	}
}

// functionColdStartDone returns when a cold start stage completes
func (s *instanceSet) functionColdStartDone() {
	// accesslimit channel guarantees that at most one request can access at a time
	s.accesslimit <- struct{}{}
	// check again to avoid tocttou problem
	alive := s.stats()
	if alive == 0 {
		s.receiveColdStartDone()
	}
	<-s.accesslimit
}

// notifyColdStartDone is used to notify the coldStartDone channel
func (s *instanceSet) notifyColdStartDone() {
	s.coldStartDone <- struct{}{}
}

// receiveColdStartDone is used to receive the coldStartDone channel
func (s *instanceSet) receiveColdStartDone() {
	<-s.coldStartDone
}

// NewInstance creates a new process instance.
// This method is extracted as a helper function to mock instance creation in test injection.
var NewInstance = func(functionName string) instance.Instance {
	return instance.NewProcessInstance(functionName)
}
