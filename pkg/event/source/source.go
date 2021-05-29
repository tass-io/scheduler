package source

import "github.com/tass-io/scheduler/pkg/tools/common"

// Trend is a type that claims what the operation expects to be done.
// It's wrapped in a ScheduleEvent to show the meaning of this event.
// For example, a ScheduleEvent that Trend is "Increase" and Target is 2
// means that you wanna increase the function instance to 2
type Trend string

const (
	None     Trend = "None" // None for init
	Increase Trend = "Increase"
	Decrease Trend = "Decrease"
)

// Source show the ScheduleEvent's source, ScheduleHandler has a priority table to
type Source string

const (
	ScheduleSource Source = "Source"
	MetricsSource  Source = "Metrics"
	QPSSource      Source = "QPS"
	TTLSource      Source = "TTL"
)

type ScheduleEvent struct {
	FunctionName string
	Target       int
	Trend        Trend
	Source       Source
}

func NewNoneScheduleEvent(functionName string) *ScheduleEvent {
	return &ScheduleEvent{
		FunctionName: functionName,
		Target:       0,
		Trend:        None,
		Source:       ScheduleSource,
	}
}

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
			// 2 > 0
			if event.Target > target.Target {
				event.Target = target.Target // QPS 0
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