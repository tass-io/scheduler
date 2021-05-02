package event

import (
	"go.uber.org/zap"
)

// Trend means the operation trend Trend with num will show the event  mean.
// for example, Increase function a with number 2 means you wanna the instance a increase to 2.
type Trend string

const (
	Increase       Trend  = "Increase"
	Decrease       Trend  = "Decrease"
	ScheduleSource Source = "source"
)

var (
	sh *ScheduleHandler
)

func init() {
	sh = &ScheduleHandler{
		upstream: make(chan ScheduleEvent, 1000),
	}
}

// ScheduleHandler will handle all upstream Event for schedule process
// all other handlers must convert their events to ScheduleEvent
type ScheduleHandler struct {
	Handler
	upstream chan ScheduleEvent
}

func GetScheduleHandlerIns() Handler {
	return sh
}

type ScheduleEvent struct {
	FunctionName string
	Target       int
	Trend        Trend
	Source       Source
}

func (sh *ScheduleHandler) AddEvent(e interface{}) {
	se, ok := e.(ScheduleEvent)
	if !ok {
		zap.S().Errorw("schedule handler add event convert error", "event", e)
	}
	go func(se ScheduleEvent) {
		sh.upstream <- se
	}(se)
}

func (sh *ScheduleHandler) GetSource() Source {
	return ScheduleSource
}

func (sh *ScheduleHandler) Start() error {
	// todo handle listen channel work
	go func() {}()
	return nil
}
