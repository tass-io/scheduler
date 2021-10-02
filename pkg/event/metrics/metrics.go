package metrics

import (
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/qps"
	"go.uber.org/zap"
)

var (
	mh *metricsHandler
)

// metricsHandler is the handler for metrics events.
type metricsHandler struct {
	channel chan event.ScheduleEvent
	// metricsSources is the map of functions,
	// each function has a map of ScheduleEvent for different Source
	metricsSources map[string]map[event.Source]event.ScheduleEvent
}

var _ event.Handler = &metricsHandler{}

func Init() {
	qps.Init()
	mh = newMetricsHandler()
	event.Register(event.MetricsSource, mh, 2, false)
}

// newMetricsHandler returns a new Metrics handler.
func newMetricsHandler() *metricsHandler {
	return &metricsHandler{
		channel:        make(chan event.ScheduleEvent, 100),
		metricsSources: make(map[string]map[event.Source]event.ScheduleEvent),
	}
}

// AddEvent provides a way to add a metric event to trigger the metrics handler
func (handler *metricsHandler) AddEvent(e interface{}) {
	event := e.(event.ScheduleEvent)
	handler.channel <- event
}

// GetSource returns metrics event.
func (handler *metricsHandler) GetSource() event.Source {
	return event.MetricsSource
}

// Starts starts a MetricsHandler
// When a metric event is received, the handler first updates the event for the coming function,
// and then makes a decision.
func (handler *metricsHandler) Start() error {
	go func() {
		for e := range handler.channel {
			zap.S().Debugw("get event from channel", "event", e)
			sources, existed := handler.metricsSources[e.FunctionName]
			if !existed {
				handler.metricsSources[e.FunctionName] = make(map[event.Source]event.ScheduleEvent)
				sources = handler.metricsSources[e.FunctionName]
			}
			sources[e.Source] = e
			handler.decide(e.FunctionName)
		}
	}()
	return nil
}

// decide considers the qps and ttl events both, and sends a final decision event to ScheduleHandler
func (handler *metricsHandler) decide(functionName string) {
	sources := handler.metricsSources[functionName]
	qps, qpsexisted := sources[event.QPSSource]
	ttl, ttlexisted := sources[event.TTLSource]
	target := 0
	trend := event.None

	if !qpsexisted {
		zap.S().Infow("ttl handler ttl single final desicion", "desicion", ttl)
		event.GetHandlerBySource(event.ScheduleSource).AddEvent(event.ScheduleEvent{
			FunctionName: ttl.FunctionName,
			Target:       ttl.Target,
			Trend:        ttl.Trend,
			Source:       event.MetricsSource,
		})
		return
	}

	if !ttlexisted {
		zap.S().Infow("metrics handler qps single final desicion", "desicion", qps)
		event.GetHandlerBySource(event.ScheduleSource).AddEvent(event.ScheduleEvent{
			FunctionName: qps.FunctionName,
			Target:       qps.Target,
			Trend:        qps.Trend,
			Source:       event.MetricsSource,
		})
		return
	}

	// the target of ttl is always Decrease
	if qps.Trend == event.Increase {
		// when qps is Increase and ttl is Decrease, take the higher Target
		if ttl.Target > qps.Target {
			target = ttl.Target
		} else {
			target = qps.Target
		}
		trend = event.Increase
	} else if qps.Trend == event.Decrease {
		// when qps is Decrease, always takes ttl Target
		trend = event.Decrease
		target = ttl.Target
	}
	finalDesicion := event.ScheduleEvent{
		FunctionName: functionName,
		Target:       target,
		Trend:        trend,
		Source:       event.MetricsSource,
	}
	zap.S().Infow("metrics handler final desicion", "desicion", finalDesicion)
	event.GetHandlerBySource(event.ScheduleSource).AddEvent(finalDesicion)
}
