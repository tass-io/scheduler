package schedule

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/schedule"
	"go.uber.org/zap"
)

var (
	sh *scheduleHandler
)

func Init() {
	sh = newScheduleHandler()
	event.Register(event.ScheduleSource, sh, 1, true)
}

// scheduleHandler will handle all upstream Event for schedule process
// all other handlers must convert their events to ScheduleEvent
type scheduleHandler struct {
	event.Handler
	lock       sync.Locker
	upstream   chan event.ScheduleEvent
	orders     []event.Source
	scoreboard map[string]scoreBoard
}

var _ event.Handler = &scheduleHandler{}

// newScheduleHandler returns a new scheduler handler for a specific Function.
// ScheduleHandler is the final handler in events, ScheduleHandler is a function-level function.
func newScheduleHandler() *scheduleHandler {
	return &scheduleHandler{
		lock: &sync.Mutex{},
		// orders records the current Source items in an increasing order
		orders: nil,
		// upstream receives all upstream ScheduleEvent
		upstream: make(chan event.ScheduleEvent, 1000),
		// scoreboard records scoreBoards for functions in a workflow.
		// the key is the function name
		scoreboard: make(map[string]scoreBoard, 10),
	}
}

// GetScheduleHandlerIns returns a scheduler handler for a Function
var GetScheduleHandlerIns = func() event.Handler {
	return sh
}

// AddEvent add a scheduler event for the ScheduleHandler
// All kinds of events finally go to ScheduleHandler
func (sh *scheduleHandler) AddEvent(e interface{}) {
	se, ok := e.(event.ScheduleEvent)
	if !ok {
		zap.S().Errorw("schedule handler add event convert error", "event", e)
	}
	sh.upstream <- se
}

// GetSource returns Schedule Source
func (sh *scheduleHandler) GetSource() event.Source {
	return event.ScheduleSource
}

// Start creates a goroutine to handle upstream ScheduleEvent,
// it stores new upstream ScheduleEvent and triggers a Decide
func (sh *scheduleHandler) Start() error {
	zap.S().Debug("schedule handler start")
	go func() {
		for e := range sh.upstream {
			zap.S().Debugw("schedule handler get event", "event", e)
			board, existed := sh.scoreboard[e.FunctionName]
			// channel serialized, do not worry
			if !existed {
				sh.lock.Lock()
				sh.scoreboard[e.FunctionName] = newScoreBoard(e.FunctionName)
				sh.lock.Unlock()
				board = sh.scoreboard[e.FunctionName]
			}
			board.Update(e)
			// lazy load for test convenient
			if sh.orders == nil {
				sh.orders = event.GetOrderedSources()
			}
			decision := board.Decide(e.FunctionName, sh.orders)
			zap.S().Debugw("schedule handler get decision", "decision", decision)
			schedule.GetScheduler().Refresh(decision.FunctionName, decision.Target)
		}
	}()
	return nil
}
