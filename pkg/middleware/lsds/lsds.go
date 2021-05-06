package lsds

import (
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/event/schedule"
	"github.com/tass-io/scheduler/pkg/middleware"
	runnerlsds "github.com/tass-io/scheduler/pkg/runner/lsds"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
	"time"
)

const (
	LSDSMiddlewareSource middleware.Source = "LSDS"
)

var (
	lsdsmiddle *LSDSMiddleware
)

func init() {
	lsdsmiddle = newLSDSMiddleware()
	middleware.Register(LSDSMiddlewareSource, lsdsmiddle, 1)
}

// LSDSMiddleware will check fs status and use some policy to handle request, which make the request have chance to redirect to other Local Scheduler
type LSDSMiddleware struct {
}

func newLSDSMiddleware() *LSDSMiddleware {
	return &LSDSMiddleware{}
}

func GetLSDSMiddleware() *LSDSMiddleware {
	return lsdsmiddle
}

func (lsds *LSDSMiddleware) Handle(body map[string]interface{}, sp *span.Span) (map[string]interface{}, middleware.Decision, error) {
	stats := workflow.GetManagerIns().GetRunner().Stats()
	instanceNum, existed := stats[sp.FunctionName]
	// todo use retry
	if !existed || instanceNum == 0 {
		// create event and wait a period of time
		event := schedule.ScheduleEvent{
			FunctionName: sp.FunctionName,
			Target:       0,
			Trend:        schedule.Increase,
			Source:       schedule.ScheduleSource,
		}
		schedule.GetScheduleHandlerIns().AddEvent(event)
		time.Sleep(viper.GetDuration(env.LSDSWait))
		stats = workflow.GetManagerIns().GetRunner().Stats()
		instanceNum, existed = stats[sp.FunctionName]
		if !existed || instanceNum == 0 {
			result ,err := runnerlsds.GetLSDSIns().Run(body, *sp)
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
