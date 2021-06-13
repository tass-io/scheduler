package lsds

import (
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/event/schedule"
	"github.com/tass-io/scheduler/pkg/event/source"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	runnerlsds "github.com/tass-io/scheduler/pkg/runner/lsds"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

const (
	LSDSMiddlewareSource middleware.Source = "LSDS"
)

var (
	// lsdsmiddle is the basic middleware for local scheduler
	// lsds is the short name for "local scheduler discovery service"
	lsdsmiddle *LSDSMiddleware
)

// init initializes the lsds middleware
func init() {
	lsdsmiddle = newLSDSMiddleware()
}

// Register registers the lsds middleware as a priority of 2
func Register() {
	middleware.Register(LSDSMiddlewareSource, lsdsmiddle, 2)
}

// LSDSMiddleware checks fs status and uses some policies to handle requests,
// which make the requests have a chance to redirect to other Local Scheduler
type LSDSMiddleware struct{}

// newLSDSMiddleware return a lsds middleware
func newLSDSMiddleware() *LSDSMiddleware {
	return &LSDSMiddleware{}
}

// Handle receives a request and does lsds middleware logic.
// lsds middleware checks the function instance existance,
// if not exists, it sends a new instance creation event.
// lsds middleware then waits for a while and checks  the function instance existance again,
// if still not exists, it forwards the function request to LSDS Runner
func (lsds *LSDSMiddleware) Handle(
	sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {

	lsdsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	lsdsSpan.Start("lsds")
	defer lsdsSpan.Finish()

	// use runner api instead of workflow api to reduce coupling
	instanceStatus := helper.GetMasterRunner().Stats()
	zap.S().Infow("get master runner instance status at lsds", "stats", instanceStatus)
	instanceNum, existed := instanceStatus[sp.GetFunctionName()]

	if !existed || instanceNum == 0 {
		// create event and wait a period of time
		// the scheduler tries to create an instance locally,
		// if still fails, it then goes to LSDS
		event := source.ScheduleEvent{
			FunctionName: sp.GetFunctionName(),
			Target:       1,
			Trend:        source.Increase,
			Source:       source.ScheduleSource,
		}
		zap.S().Infow("create event at lsds", "event", event)
		schedule.GetScheduleHandlerIns().AddEvent(event)
		time.Sleep(viper.GetDuration(env.LSDSWait))
		instanceStatus = helper.GetMasterRunner().Stats()
		zap.S().Infow("get master runner stats at lsds", "stats", instanceStatus)
		instanceNum, existed = instanceStatus[sp.GetFunctionName()]
		if !existed || instanceNum == 0 {
			result, err := runnerlsds.GetLSDSIns().Run(sp, body)
			if err != nil {
				zap.S().Errorw("lsds middleware run error", "err", err)
				return nil, middleware.Abort, err
			}
			return result, middleware.Abort, nil
		}
	}
	return nil, middleware.Next, nil
}

// GetSource returns the middleware source
func (lsds *LSDSMiddleware) GetSource() middleware.Source {
	return LSDSMiddlewareSource
}
