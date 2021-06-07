package ttl

import (
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/source"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"go.uber.org/zap"
)

// TTLManager is a center to records all instances live time for a specific function
type TTLManager struct {
	sync.Locker
	functionName string
	timers       map[instance.Instance]*time.Timer
	timeout      chan instance.Instance
	append       chan instance.Instance
}

// clean stops the clock for an instance
func (ttl *TTLManager) clean(ins instance.Instance) {
	ttl.Lock()
	defer ttl.Unlock()
	if timer, existed := ttl.timers[ins]; existed {
		timer.Stop()
		delete(ttl.timers, ins)
	}
}

// Release tries releasing an instance,
// the TTLManager checks the status of the instance, if it's busy,
// the instance will not be released
func (ttl *TTLManager) Release(ins instance.Instance) {
	ttl.timeout <- ins
}

// Append appends a new instance clock in the TTLManager
func (ttl *TTLManager) Append(ins instance.Instance) {
	ttl.append <- ins
}

// start starts the instance clock in the TTLManager
// TODO: When a request comes, update the timer
func (ttl *TTLManager) start(ins instance.Instance) {
	ttl.timers[ins] = time.NewTimer(viper.GetDuration(env.TTL))
	go func() {
		timer := ttl.timers[ins]
		<-timer.C
		ttl.Release(ins)
	}()
}

// NewTTLManager initializes a new TTLManager and starts it
// it gets messages from timeout channel and append channel and sends events
func NewTTLManager(functionName string) *TTLManager {
	ttl := &TTLManager{
		Locker:       &sync.Mutex{},
		functionName: functionName,
		timers:       make(map[instance.Instance]*time.Timer),
		timeout:      make(chan instance.Instance, 10),
		append:       make(chan instance.Instance, 10),
	}
	go func() {
		for {
			select {
			case ins := <-ttl.timeout:
				{
					if ins.IsRunning() && ins.HasRequests() {
						ttl.timers[ins].Reset(viper.GetDuration(env.TTL))
						go func() {
							timer := ttl.timers[ins]
							<-timer.C
							ttl.Release(ins)
						}()
						continue
					}
					zap.S().Infow("clean up timer")
					ttl.clean(ins)
					// generate ttl event
					event.FindEventHandlerBySource(source.MetricsSource).AddEvent(source.ScheduleEvent{
						FunctionName: functionName,
						Target:       len(ttl.timers),
						Trend:        source.Decrease,
						Source:       source.TTLSource,
					})
				}
			case ins := <-ttl.append:
				{
					ttl.start(ins)
					event.FindEventHandlerBySource(source.MetricsSource).AddEvent(source.ScheduleEvent{
						FunctionName: functionName,
						Target:       len(ttl.timers),
						// NOTE: ttl never does Increase action
						Trend:  source.Decrease,
						Source: source.TTLSource,
					})
				}
			}
		}
	}()
	return ttl
}
