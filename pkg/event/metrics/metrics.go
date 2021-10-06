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
	upstream chan event.ScheduleEvent
	// functionMetrcis is the map of functions and their runtime metrics,
	// each function records its latest qps and ttl events.
	functionMetrcis map[string]metricsEvents
}
type metricsEvents struct {
	qps *event.ScheduleEvent
	ttl *event.ScheduleEvent
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
		upstream:        make(chan event.ScheduleEvent, 100),
		functionMetrcis: make(map[string]metricsEvents),
	}
}

// AddEvent provides a way to add a metric event to trigger the metrics handler
func (handler *metricsHandler) AddEvent(e event.ScheduleEvent) {
	handler.upstream <- e
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
		for e := range handler.upstream {
			zap.S().Debugw("get event from channel", "event", e)
			metrics, existed := handler.functionMetrcis[e.FunctionName]
			if !existed {
				handler.functionMetrcis[e.FunctionName] = metricsEvents{}
				metrics = handler.functionMetrcis[e.FunctionName]
			}
			metrics = updateMetrics(metrics, e)
			handler.functionMetrcis[e.FunctionName] = metrics
			handler.decide(e.FunctionName)
		}
	}()
	return nil
}

func updateMetrics(m metricsEvents, e event.ScheduleEvent) metricsEvents {
	if e.Source == event.QPSSource {
		m.qps = copyEvent(e)
	} else if e.Source == event.TTLSource {
		m.ttl = copyEvent(e)
	} else {
		zap.S().Errorw("metrics handler get unknown event", "event", e)
	}
	return m
}

// copyEvent is for making the event obj immutable
func copyEvent(e event.ScheduleEvent) *event.ScheduleEvent {
	return &event.ScheduleEvent{
		FunctionName: e.FunctionName,
		Target:       e.Target,
		Trend:        e.Trend,
		Source:       e.Source,
	}
}

// decide considers the qps and ttl events both, and sends a final decision event to ScheduleHandler
func (handler *metricsHandler) decide(functionName string) {
	metrics := handler.functionMetrcis[functionName]
	qpsEvent := metrics.qps
	ttlEvent := metrics.ttl

	if qpsEvent == nil && ttlEvent == nil {
		zap.S().Errorw("metrics handler get no event", "function", functionName)
		return
	}

	if qpsEvent == nil && ttlEvent != nil {
		zap.S().Infow("ttl handler ttl single final desicion", "desicion", ttlEvent)
		event.GetHandlerBySource(event.ScheduleSource).AddEvent(event.ScheduleEvent{
			FunctionName: ttlEvent.FunctionName,
			Target:       ttlEvent.Target,
			Trend:        ttlEvent.Trend,
			Source:       event.MetricsSource,
		})
		return
	}

	if qpsEvent != nil && ttlEvent == nil {
		zap.S().Infow("metrics handler qps single final desicion", "desicion", qpsEvent)
		event.GetHandlerBySource(event.ScheduleSource).AddEvent(event.ScheduleEvent{
			FunctionName: qpsEvent.FunctionName,
			Target:       qpsEvent.Target,
			Trend:        qpsEvent.Trend,
			Source:       event.MetricsSource,
		})
		return
	}

	target := 0
	trend := event.None
	// the target of ttl is always Decrease
	if qpsEvent.Trend == event.Increase {
		// when qps is Increase and ttl is Decrease, take the higher Target
		if ttlEvent.Target > qpsEvent.Target {
			target = ttlEvent.Target
		} else {
			target = qpsEvent.Target
		}
		trend = event.Increase
	} else if qpsEvent.Trend == event.Decrease {
		// when qps is Decrease, always takes ttl Target
		trend = event.Decrease
		target = ttlEvent.Target
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
