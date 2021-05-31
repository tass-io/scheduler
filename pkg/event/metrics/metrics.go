package metrics

import (
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/qps"
	"github.com/tass-io/scheduler/pkg/event/source"
	"go.uber.org/zap"
)

var (
	metricsHandler *MetricsHandler
)

func Initial() {
	qps.Initial()
	metricsHandler = newMetricsHandler()
	event.Register(source.MetricsSource, metricsHandler, 2, false)
}

// MetricsHandler is the handler for metrics events.
type MetricsHandler struct {
	channel        chan source.ScheduleEvent
	// metricsSources is the map of functions,
	// each function has a map of ScheduleEvent for different source
	metricsSources map[string]map[source.Source]source.ScheduleEvent
}

var GetMetricsHandlerIns = func() event.Handler {
	return metricsHandler
}

func newMetricsHandler() *MetricsHandler {
	return &MetricsHandler{
		channel:        make(chan source.ScheduleEvent, 100),
		metricsSources: make(map[string]map[source.Source]source.ScheduleEvent),
	}
}

func (handler *MetricsHandler) AddEvent(e interface{}) {
	event := e.(source.ScheduleEvent)
	handler.channel <- event
}

func (handler *MetricsHandler) GetSource() source.Source {
	return source.MetricsSource
}

// Starts starts a MetricsHandler
// When a metric event is received, the handler first updates the event for the coming function,
// and then makes a decision.
func (handler *MetricsHandler) Start() error {
	go func() {
		for e := range handler.channel {
			zap.S().Debugw("get event from channel", "event", e)
			sources, existed := handler.metricsSources[e.FunctionName]
			if !existed {
				handler.metricsSources[e.FunctionName] = make(map[source.Source]source.ScheduleEvent)
				sources = handler.metricsSources[e.FunctionName]
			}
			sources[e.Source] = e
			handler.decide(e.FunctionName)
		}
	}()
	return nil
}

// decide considers the qps and ttl events both, and sends a final decision to ScheduleHandler
func (handler *MetricsHandler) decide(functionName string) {
	sources := handler.metricsSources[functionName]
	qps, qpsexisted := sources[source.QPSSource]
	ttl, ttlexisted := sources[source.TTLSource]
	target := 0
	trend := source.None

	if !qpsexisted {
		zap.S().Infow("metrics handler final desicion", "desicion", ttl)
		event.FindEventHandlerBySource(source.ScheduleSource).AddEvent(source.ScheduleEvent{
			FunctionName: ttl.FunctionName,
			Target:       ttl.Target,
			Trend:        ttl.Trend,
			Source:       source.MetricsSource,
		})
		return
	}

	if !ttlexisted {
		zap.S().Infow("metrics handler final desicion", "desicion", qps)
		event.FindEventHandlerBySource(source.ScheduleSource).AddEvent(source.ScheduleEvent{
			FunctionName: qps.FunctionName,
			Target:       qps.Target,
			Trend:        qps.Trend,
			Source:       source.MetricsSource,
		})
		return
	}

	// the target of ttl is always Decrease
	if qps.Trend == source.Increase {
		// when qps is Increase and ttl is Decrease, take the higher Target
		if ttl.Target > qps.Target {
			target = ttl.Target
		} else {
			target = qps.Target
		}
		trend = source.Increase
	} else if qps.Trend == source.Decrease {
		// when qps is Decrease, always takes ttl Target
		trend = source.Decrease
		target = ttl.Target
	}
	finalDesicion := source.ScheduleEvent{
		FunctionName: functionName,
		Target:       target,
		Trend:        trend,
		Source:       source.MetricsSource,
	}
	zap.S().Infow("metrics handler final desicion", "desicion", finalDesicion)
	event.FindEventHandlerBySource(source.ScheduleSource).AddEvent(finalDesicion)
}
