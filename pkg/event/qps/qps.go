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

// QPSHandler is a status of the current function calling status.
type QPSHandler struct {
	// qpsMap is a statistics of function and number of calls
	// the key is the function name and the value is the number of calls
	qpsMap map[string]int64
}

// GetQPSHandlerIns returns a QPSHandler instance
var GetQPSHandlerIns = func() event.EventHandler {
	return qpsHandler
}

// newQPSHandler creates a new QPSHandler
func newQPSHandler() *QPSHandler {
	return &QPSHandler{
		qpsMap: make(map[string]int64),
	}
}

// noone should use it, because the qps handler pulls the middleware QPSMiddleware periodly
func (handler *QPSHandler) AddEvent(e interface{}) {
	zap.S().Panic("do not use QPSHandler.AddEvent")
}

// GetSource returns QPSSource
func (handler *QPSHandler) GetSource() source.Source {
	return source.QPSSource
}

// Start starts a QPSHandler.
// QPSHandler considers the performance impact, so when the request goes to QPSMiddleware,
// it only does some info recording and does nothing on events adding.
// Instead, QPSHandler pulls the QPSMiddleware periodly and sends events to MetricsHandler
// By default, the pulling cycle is one second
func (handler *QPSHandler) Start() error {
	go func() {
		middlewareRaw := middleware.FindMiddlewareBySource(qps.QPSMiddlewareSource)
		if middlewareRaw == nil {
			zap.S().Errorw("middleware QPS not")
			return
		}
		middleware := middlewareRaw.(*qps.Middleware)
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
				switch {
				case target == before:
					continue
				case target > before:
					trend = source.Increase
				case target < before:
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
