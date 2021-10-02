package schedule

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/event"
	"go.uber.org/zap"
)

// scoreBoard stores different Source suggestions for the function and the function expected status.
// scoreBoard is a function-level object
type scoreBoard struct {
	lock sync.Locker
	// bestWishes is the merged status of the schedule event,
	// which is the expected status for the Function
	bestWishes *event.ScheduleEvent
	// scores stores different Source suggestions for the Function
	scores map[event.Source]event.ScheduleEvent
}

// newScoreBoard returns a new ScoreBoard object.
func newScoreBoard(functionName string) scoreBoard {
	return scoreBoard{
		lock:       &sync.Mutex{},
		bestWishes: event.NewNoneScheduleEvent(functionName),
		scores:     make(map[event.Source]event.ScheduleEvent, 10),
	}
}

// scoreBoard records recent events and makes a decision.
// It iterates over the input oders which have been sorted,
// in each order, it takes a related item and do the merge action.
// Finally, it decides the final "bestWish".
func (board *scoreBoard) Decide(functionName string, orders []event.Source) *event.ScheduleEvent {
	board.lock.Lock()
	defer board.lock.Unlock()
	zap.S().Debugw("board before decide", "wishes", board.bestWishes)
	origin := event.NewNoneScheduleEvent(functionName)
	usedList := []event.Source{}
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
	*board.bestWishes = *origin
	zap.S().Debugw("board after decide", "wishes", board.bestWishes)
	return origin
}

// Update updates a new schedule event in a scoreboard
func (board *scoreBoard) Update(e event.ScheduleEvent) {
	board.lock.Lock()
	board.scores[e.Source] = e
	board.lock.Unlock()
}
