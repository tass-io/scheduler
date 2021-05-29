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

type TTLManager struct {
	sync.Locker
	functionName string
	timers       map[instance.Instance]*time.Timer
	timeout      chan instance.Instance
	append       chan instance.Instance
}

func (ttl *TTLManager) clean(ins instance.Instance) {
	ttl.Lock()
	defer ttl.Unlock()
	if timer, existed := ttl.timers[ins]; existed {
		timer.Stop()
		delete(ttl.timers, ins)
	}
}

func (ttl *TTLManager) Release(ins instance.Instance) {
	ttl.timeout <- ins
}

func (ttl *TTLManager) Append(ins instance.Instance) {
	ttl.append <- ins
}

func (ttl *TTLManager) start(ins instance.Instance) {
	ttl.timers[ins] = time.NewTimer(viper.GetDuration(env.TTL))
	go func() {
		timer := ttl.timers[ins]
		<-timer.C
		ttl.timeout <- ins
	}()
}

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
						Trend:        source.Decrease,
						Source:       source.TTLSource,
					})
				}
			}
		}
	}()
	return ttl
}
