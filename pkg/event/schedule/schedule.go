package schedule

import (
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

// scheduleHandler will handle all upstream Event for schedule process,
// including create or delete process instances.
// NOTE: all other event handlers must convert their events to ScheduleEvent finally.
type scheduleHandler struct {
	upstream       chan event.ScheduleEvent
	orderedSources []event.Source
	scoreboard     map[string]scoreBoard
}

var _ event.Handler = &scheduleHandler{}

// newScheduleHandler returns a new schedule handler for a specific Function.
// ScheduleHandler is the final handler in events, ScheduleHandler is a function-level function.
func newScheduleHandler() *scheduleHandler {
	return &scheduleHandler{
		// orderedSources records the current Source items in an increasing order
		orderedSources: nil,
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

// AddEvent adds a scheduler event for the ScheduleHandler
// All kinds of events finally go to ScheduleHandler
func (sh *scheduleHandler) AddEvent(e event.ScheduleEvent) {
	sh.upstream <- e
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
			// lazy load for test convenient
			if sh.orderedSources == nil {
				sh.orderedSources = event.GetOrderedSources()
			}
			zap.S().Debugw("schedule handler get event", "event", e)
			board, existed := sh.scoreboard[e.FunctionName]
			if !existed {
				sh.scoreboard[e.FunctionName] = newScoreBoard(e.FunctionName)
				board = sh.scoreboard[e.FunctionName]
			}
			board.Update(e)
			decision := board.Decide(e.FunctionName, sh.orderedSources)
			zap.S().Debugw("schedule handler get decision", "decision", decision)
			schedule.GetScheduler().Refresh(decision.FunctionName, decision.Target)
			// NOTE: actually, this line is not necessary, because the fields in `scoreBoard` are all pointers.
			// so the changes of the `board` variable will be reflected in the scoreboard[e.FunctionName].
			// However, we still copy the board to scoreboard[e.FunctionName] to make the developer not surprise.
			sh.scoreboard[e.FunctionName] = board
		}
	}()
	return nil
}
