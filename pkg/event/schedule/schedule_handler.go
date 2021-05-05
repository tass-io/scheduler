package schedule

import (
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/schedule"
	"github.com/tass-io/scheduler/pkg/tools/common"
	"go.uber.org/zap"
	"sync"
)

// Trend means the operation trend Trend with num will show the event  mean.
// for example, Increase function a with number 2 means you wanna the instance a increase to 2.
type Trend string

const (
	Increase       Trend  = "Increase"
	Decrease       Trend  = "Decrease"
	ScheduleSource event.Source = "source"
)

var (
	sh *ScheduleHandler
)

func init() {
	sh = &ScheduleHandler{
		lock:       &sync.Mutex{},
		orders:     nil,
		upstream:   make(chan ScheduleEvent, 1000),
		scoreboard: make(map[string]scoreBoard, 10),
	}
	event.Register(ScheduleSource, sh, 1)
}

type ScheduleEvent struct {
	FunctionName string
	Target       int
	Trend        Trend
	Source       event.Source
}

func (event *ScheduleEvent) Merge(target *ScheduleEvent) {
	switch event.Trend {
	case Increase:
		{
			if event.Target < target.Target {
				event.Target = target.Target
			}
		}
	case Decrease:
		{
			if event.Target > target.Target {
				event.Target = target.Target
			}
		}
	}
}

// scoreBoard will store different Source suggestion for the function
type scoreBoard struct {
	lock       sync.Locker
	bestWishes ScheduleEvent
	scores     map[event.Source]ScheduleEvent
}

func newScoreBoard(functionName string) scoreBoard {
	return scoreBoard{
		lock: &sync.Mutex{},
		bestWishes: ScheduleEvent{
			FunctionName: functionName,
			Target:       0,
			Trend:        Increase,
			Source:       ScheduleSource,
		},
		scores: make(map[event.Source]ScheduleEvent, 10),
	}
}

// scoreBoard will see all event and make a decision
func (board *scoreBoard) Decide(orders []event.Source) *ScheduleEvent {
	board.lock.Lock()
	defer board.lock.Unlock()
	zap.S().Debugw("board before decide", "wishes", board.bestWishes)
	origin := &ScheduleEvent{}
	err := common.DeepCopy(origin, &board.bestWishes)
	if err != nil {
		zap.S().Errorw("scoreboard decide error", "error", err)
		return nil
	}
	zap.S().Debugw("board get orders", "orders", orders)
	for _, order := range orders {
		event, existed := board.scores[order]
		if !existed {
			continue
		}
		zap.S().Debugw("scoreboard handle source with event", "source", order, "event", event)
		origin.Merge(&event)
	}
	board.bestWishes = *origin
	return origin
}

func (board *scoreBoard) Update(e ScheduleEvent) {
	board.lock.Lock()
	board.scores[e.Source] = e
	board.lock.Unlock() // no defer, performance not will for the hot code and just 3 line code
}

// ScheduleHandler will handle all upstream Event for schedule process
// all other handlers must convert their events to ScheduleEvent
type ScheduleHandler struct {
	event.Handler
	lock       sync.Locker
	upstream   chan ScheduleEvent
	orders     []event.Source
	scoreboard map[string]scoreBoard
}

func GetScheduleHandlerIns() event.Handler {
	return sh
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

func (sh *ScheduleHandler) GetSource() event.Source {
	return ScheduleSource
}

func (sh *ScheduleHandler) Start() error {
	zap.S().Debug("schedule handler start")
	go func() {
		for e := range sh.upstream {
			zap.S().Debugw("schedule handler get event", "event", e)
			board, existed := sh.scoreboard[e.FunctionName]
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
			decision := board.Decide(sh.orders)
			zap.S().Debugw("schedule handler get decision", "decision", decision)
			schedule.GetScheduler().Refresh(decision.FunctionName, decision.Target)
		}
	}()
	return nil
}