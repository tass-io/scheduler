package qps

import (
	"time"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/middleware/qps"
	"go.uber.org/zap"
)

var (
	qpsh *qpsHandler
)

// qpsHandler is a status of the current function calling status.
type qpsHandler struct {
	// qpsMap is a statistics of function and number of calls
	// the key is the function name and the value is the number of calls
	qpsMap map[string]int64
}

var _ event.Handler = &qpsHandler{}

func Init() {
	qpsh = newQPSHandler()
	_ = qpsh.Start()
}

// newQPSHandler creates a new QPSHandler
func newQPSHandler() *qpsHandler {
	return &qpsHandler{
		qpsMap: make(map[string]int64),
	}
}

// noone should use it, because the qps handler pulls the middleware QPSMiddleware periodly
func (handler *qpsHandler) AddEvent(e interface{}) {
	zap.S().Panic("do not use QPSHandler.AddEvent")
}

// GetSource returns QPSSource
func (handler *qpsHandler) GetSource() event.Source {
	return event.QPSSource
}

// Start starts a QPS EventHandler.
// QPS EventHandler considers the performance impact, so when the request goes to QPSMiddleware,
// it only does some info recording and does nothing on events adding.
// Instead, QPS EventHandler pulls the QPSMiddleware periodly and sends events to MetricsHandler
// By default, the pulling cycle is one second
func (handler *qpsHandler) Start() error {
	go func() {
		mwHandler := middleware.GetHandlerBySource(middleware.QPSMiddlewareSource)
		if mwHandler == nil {
			zap.S().Errorw("middleware QPS not init")
			return
		}
		// note that only qps middleware implements both middleware.Handler & qps.Statistics
		statistics, ok := mwHandler.(qps.Statistics)
		if !ok {
			zap.S().Errorw("middleware QPS not implement Statistics")
			return
		}
		for {
			time.Sleep(1 * time.Second)
			stats := statistics.GetStat()
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
				var trend event.Trend
				switch {
				case target == before:
					continue
				case target > before:
					trend = event.Increase
				case target < before:
					trend = event.Decrease
				}
				handler.qpsMap[functionName] = target
				e := event.ScheduleEvent{
					FunctionName: functionName,
					Target:       int(target),
					Trend:        trend,
					Source:       event.QPSSource,
				}
				zap.S().Debugw("QPS add event", "event", e)
				event.GetHandlerBySource(event.MetricsSource).AddEvent(e)
			}
		}
	}()
	return nil
}
