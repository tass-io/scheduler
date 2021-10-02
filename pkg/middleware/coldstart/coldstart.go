package coldstart

import (
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/schedule"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	fnschedule "github.com/tass-io/scheduler/pkg/schedule"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

var (
	// cs is the cold start middleware for local Scheduler
	// if no instance is available for the function, it sends a create event
	cs *coldstartMiddleware
)

// init initializes the cold start middleware
func init() {
	cs = newColdstartMiddleware()
}

// Register registers the lsds middleware as a priority of 2
func Register() {
	middleware.Register(middleware.ColdstartSource, cs, 2)
}

// coldstartMiddleware is responsible for sending a creating event for instance,
// if there is at least one request function process instance, coldstartMiddleware is skipped
type coldstartMiddleware struct{}

var _ middleware.Handler = &coldstartMiddleware{}

// newColdstartMiddleware returns a coldstart middleware instance
func newColdstartMiddleware() *coldstartMiddleware {
	return &coldstartMiddleware{}
}

// Handle receives a request and does cold start middleware logic.
// cold start middleware checks the function instance existence,
// if not exists, it sends a new instance creation event.
func (cs *coldstartMiddleware) Handle(
	sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {

	lsdsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	lsdsSpan.Start("coldstart")
	defer lsdsSpan.Finish()

	functionName := sp.GetFunctionName()
	instanceNum := helper.GetMasterRunner().FunctionStats(functionName)

	zap.S().Infow("status at coldstart middleware", "function", functionName, "number", instanceNum)

	// no running instances or the instanceSet not exists
	if instanceNum == 0 {
		fnschedule.GetScheduler().NewInstanceSetIfNotExist(functionName)

		// create an event, the scheduler tries to create an instance locally,
		// if still fails, it then goes to LSDS
		event := event.ScheduleEvent{
			FunctionName: functionName,
			Target:       1,
			Trend:        event.Increase,
			Source:       event.ScheduleSource,
		}
		zap.S().Infow("create event at coldstart middleware", "event", event)
		schedule.GetScheduleHandlerIns().AddEvent(event)

		// TODO: Now use notification directly, a policy is preferred here
		// TODO: Cold Start error handling
		fnschedule.GetScheduler().ColdStartDone(functionName)
	}

	return nil, middleware.Next, nil
}

// GetSource returns the middleware source
func (cs *coldstartMiddleware) GetSource() middleware.Source {
	return middleware.ColdstartSource
}
