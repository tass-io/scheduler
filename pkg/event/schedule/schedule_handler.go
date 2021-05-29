package schedule

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/source"
	"github.com/tass-io/scheduler/pkg/schedule"
	"github.com/tass-io/scheduler/pkg/tools/register"
	"go.uber.org/zap"
)

var (
	sh *ScheduleHandler
)

func Initial() {
	sh = newScheduleHandler()
	register.Register(func(fucntionName string, target int, trend, src string) {
		event := source.ScheduleEvent{
			FunctionName: fucntionName,
			Target:       target,
			Trend:        source.Trend(trend),
			Source:       source.Source(src),
		}
		sh.AddEvent(event)
	})
	event.Register(source.ScheduleSource, sh, 1, true)
}

// Source -> cold start 1
// METRICS

// TTL 2
// QPS 3

// scoreBoard will store different Source suggestions for the function
type scoreBoard struct {
	lock       sync.Locker
	bestWishes *source.ScheduleEvent
	scores     map[source.Source]source.ScheduleEvent
}

func newScoreBoard(functionName string) scoreBoard {
	return scoreBoard{
		lock:       &sync.Mutex{},
		bestWishes: source.NewNoneScheduleEvent(functionName),
		scores:     make(map[source.Source]source.ScheduleEvent, 10),
	}
}

// scoreBoard will see all event and make a decision
func (board *scoreBoard) Decide(functionName string, orders []source.Source) *source.ScheduleEvent {
	board.lock.Lock()
	defer board.lock.Unlock()
	zap.S().Debugw("board before decide", "wishes", board.bestWishes)
	origin := source.NewNoneScheduleEvent(functionName)
	usedList := []source.Source{}
	zap.S().Debugw("board get orders", "orders", orders)
	for _, order := range orders {
		event, existed := board.scores[order]
		if !existed {
			continue
		}
		zap.S().Debugw("scoreboard handle source with event", "source", order, "event", event)
		if used := origin.Merge(&event); used {
			zap.S().Debugw("scoreboard use event", "event", event, "result", origin)
			usedList = append(usedList, event.Source)
		}
	}

	for _, source := range usedList {
		if event.NeedDelete(source) {
			delete(board.scores, source)
		}
	}
	origin.Merge(board.bestWishes)
	board.bestWishes = origin
	zap.S().Debugw("board after decide", "wishes", board.bestWishes)
	return origin
}

func (board *scoreBoard) Update(e source.ScheduleEvent) {
	board.lock.Lock()
	board.scores[e.Source] = e
	board.lock.Unlock() // no defer, performance not will for the hot code and just 3 line code
}

// ScheduleHandler will handle all upstream Event for schedule process
// all other handlers must convert their events to ScheduleEvent
type ScheduleHandler struct {
	event.Handler
	lock       sync.Locker
	upstream   chan source.ScheduleEvent
	orders     []source.Source
	scoreboard map[string]scoreBoard
}

func newScheduleHandler() *ScheduleHandler {
	return &ScheduleHandler{
		lock:       &sync.Mutex{},
		orders:     nil,
		upstream:   make(chan source.ScheduleEvent, 1000),
		scoreboard: make(map[string]scoreBoard, 10),
	}
}

var GetScheduleHandlerIns = func() event.Handler {
	return sh
}

func (sh *ScheduleHandler) AddEvent(e interface{}) {
	se, ok := e.(source.ScheduleEvent)
	if !ok {
		zap.S().Errorw("schedule handler add event convert error", "event", e)
	}
	sh.upstream <- se
}

func (sh *ScheduleHandler) GetSource() source.Source {
	return source.ScheduleSource
}

// Start will create go routine to handle upstream ScheduleEvent.
// store new upstream ScheduleEvent and trigger a Decide
func (sh *ScheduleHandler) Start() error {
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
				sh.orders = event.Orders()
			}
			decision := board.Decide(e.FunctionName, sh.orders)
			zap.S().Debugw("schedule handler get decision", "decision", decision)
			schedule.GetScheduler().Refresh(decision.FunctionName, decision.Target)
		}
	}()
	return nil
}
