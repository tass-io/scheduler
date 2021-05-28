package qps

import (
	"time"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/schedule"
	"github.com/tass-io/scheduler/pkg/middleware/qps"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
)

var (
	qpsHandler *QPSHandler
	QPSSource  event.Source = "QPS"
)

func Initial() {
	qpsHandler = newQPSHandler()
	workflow.GetManagerIns().RegisterEvent(QPSSource, qpsHandler, 2, false)
}

// QPSHandler
type QPSHandler struct {
	qpsMap map[string]int64
}

var GetQPSHandlerIns = func() event.Handler {
	return qpsHandler
}

func newQPSHandler() *QPSHandler {
	return &QPSHandler{
		qpsMap: make(map[string]int64),
	}
}

// noone will use it, because the handler will poll middleware QPSMiddleware
func (handler *QPSHandler) AddEvent(e interface{}) {
	panic("do not use QPSHandler.AddEvent")
}

func (handler *QPSHandler) GetSource() event.Source {
	return QPSSource
}

func (handler *QPSHandler) Start() error {
	go func() {
		middlewareRaw := workflow.GetManagerIns().GetMiddlewareBySource(qps.QPSMiddlewareSource)
		if middlewareRaw == nil {
			return
		}
		middleware := middlewareRaw.(*qps.QPSMiddleware)
		for {
			time.Sleep(1 * time.Second)
			stats := middleware.GetStat()
			zap.S().Debugw("QPS event SYNC", "stats", stats)
			for functionName, metrics := range stats {
				before, existed := handler.qpsMap[functionName]
				if !existed {
					handler.qpsMap[functionName] = 0
					before = 0
				}
				target := metrics / 10
				if target < 1 {
					target = 1
				}
				if metrics == 0 {
					target = 0
				}
				zap.S().Debugw("qps get before and target", "before", before, "target", target)
				var trend schedule.Trend
				if target == before {
					continue
				} else if target > before {
					trend = schedule.Increase
				} else if target < before {
					trend = schedule.Decrease
				}
				handler.qpsMap[functionName] = target
				event := schedule.ScheduleEvent{
					FunctionName: functionName,
					Target:       int(target),
					Trend:        trend,
					Source:       QPSSource,
				}
				zap.S().Debugw("QPS add event", "event", event)
				schedule.GetScheduleHandlerIns().AddEvent(event)
			}
		}
	}()
	return nil
}
