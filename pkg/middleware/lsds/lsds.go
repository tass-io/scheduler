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

func (lsds *LSDSMiddleware) Handle(body map[string]interface{}, sp *span.Span) (map[string]interface{}, middleware.Decision, error) {
	stats := helper.GetMasterRunner().Stats() // use runner api instead of workflow api to reduce coupling
	zap.S().Infow("get master runner stats at lsds", "stats", stats)
	instanceNum, existed := stats[sp.FunctionName]
	// todo use retry
	if !existed || instanceNum == 0 {
		// create event and wait a period of time
		event := source.ScheduleEvent{
			FunctionName: sp.FunctionName,
			Target:       1,
			Trend:        source.Increase,
			Source:       source.ScheduleSource,
		}
		zap.S().Infow("create event at lsds", "event", event)
		schedule.GetScheduleHandlerIns().AddEvent(event)
		time.Sleep(viper.GetDuration(env.LSDSWait))
		stats = helper.GetMasterRunner().Stats()
		zap.S().Infow("get master runner stats at lsds", "stats", stats)
		instanceNum, existed = stats[sp.FunctionName]
		if !existed || instanceNum == 0 {
			result, err := runnerlsds.GetLSDSIns().Run(body, *sp)
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
