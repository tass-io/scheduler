package event

import "github.com/tass-io/scheduler/pkg/utils/common"

// ScheduleEvent is a resource schedule event sent by different middleware
type ScheduleEvent struct {
	FunctionName string
	Target       int // Target is the expected number of the process
	Trend        Trend
	Source       Source
}

// NewNoneScheduleEvent is used for initialization a new schedule event
func NewNoneScheduleEvent(functionName string) *ScheduleEvent {
	return &ScheduleEvent{
		FunctionName: functionName,
		Target:       0,
		Trend:        None,
		Source:       ScheduleSource,
	}
}

// Merge is a helper function to merge two schedule events,
// it assumes that the priorty of event is always higher than or equal to the priorty of target.
func (event *ScheduleEvent) Merge(target *ScheduleEvent) bool {
	if target == nil || target.Trend == None {
		return false
	}
	used := false
	switch event.Trend {
	case Increase:
		{
			if event.Target < target.Target {
				event.Target = target.Target
				used = true
			}
		}
	case Decrease:
		{
			if event.Target > target.Target {
				event.Target = target.Target
				used = true
			}
		}
	case None:
		{
			_ = common.DeepCopy(event, target)
			used = true
		}
	}
	return used
}
