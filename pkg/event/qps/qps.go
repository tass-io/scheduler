package qps

import (
	"time"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/source"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/middleware/qps"
	"go.uber.org/zap"
)

var (
	qpsHandler *QPSHandler
)

func Initial() {
	qpsHandler = newQPSHandler()
	_ = qpsHandler.Start()
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

func (handler *QPSHandler) GetSource() source.Source {
	return source.QPSSource
}

func (handler *QPSHandler) Start() error {
	go func() {
		middlewareRaw := middleware.FindMiddlewareBySource(qps.QPSMiddlewareSource)
		if middlewareRaw == nil {
			zap.S().Errorw("middleware QPS not")
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
				var trend source.Trend
				if target == before {
					continue
				} else if target > before {
					trend = source.Increase
				} else if target < before {
					trend = source.Decrease
				}
				handler.qpsMap[functionName] = target
				e := source.ScheduleEvent{
					FunctionName: functionName,
					Target:       int(target),
					Trend:        trend,
					Source:       source.QPSSource,
				}
				zap.S().Debugw("QPS add event", "event", e)
				event.FindEventHandlerBySource(source.MetricsSource).AddEvent(e)
			}
		}
	}()
	return nil
}
