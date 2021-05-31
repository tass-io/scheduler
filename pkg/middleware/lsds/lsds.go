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
	lsdsmiddle *LSDSMiddleware
)

func init() {
	lsdsmiddle = newLSDSMiddleware()
}

func Register() {
	middleware.Register(LSDSMiddlewareSource, lsdsmiddle, 2)
}

// LSDSMiddleware will check fs status and use some policy to handle request, which make the request have chance to redirect to other Local Scheduler
type LSDSMiddleware struct {
}

func newLSDSMiddleware() *LSDSMiddleware {
	return &LSDSMiddleware{}
}

func (lsds *LSDSMiddleware) Handle(sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {
	lsdsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	lsdsSpan.Start("lsds")
	defer lsdsSpan.Finish()
	stats := helper.GetMasterRunner().Stats() // use runner api instead of workflow api to reduce coupling
	zap.S().Infow("get master runner stats at lsds", "stats", stats)
	instanceNum, existed := stats[sp.GetFunctionName()]
	// todo use retry
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
		stats = helper.GetMasterRunner().Stats()
		zap.S().Infow("get master runner stats at lsds", "stats", stats)
		instanceNum, existed = stats[sp.GetFunctionName()]
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

func (lsds *LSDSMiddleware) GetSource() middleware.Source {
	return LSDSMiddlewareSource
}
