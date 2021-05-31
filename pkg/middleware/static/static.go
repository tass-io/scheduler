package static

import (
	"github.com/tass-io/scheduler/pkg/middleware"
	runnerlsds "github.com/tass-io/scheduler/pkg/runner/lsds"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

const (
	StaticMiddlewareSource middleware.Source = "Static"
)

var (
	staticmiddle *StaticMiddleware
)

func init() {
	staticmiddle = newStaticMiddleware()
}

func Register() {
	middleware.Register(StaticMiddlewareSource, staticmiddle, 1)
}

// StaticMiddleware will check fs status and use some policy to handle request, which make the request have chance to redirect to other Local Scheduler
type StaticMiddleware struct {
}

func newStaticMiddleware() *StaticMiddleware {
	return &StaticMiddleware{}
}

func GetStaticMiddleware() *StaticMiddleware {
	return staticmiddle
}

func (static *StaticMiddleware) Handle(sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {
	staticSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	staticSpan.Start("static")
	defer staticSpan.Finish()
	stats := runnerlsds.GetLSDSIns().Stats() // use runner api instead of workflow api to reduce coupling
	zap.S().Infow("get master runner stats at static", "stats", stats)
	instanceNum, existed := stats[sp.GetFunctionName()]
	// todo use retry
	if !existed || instanceNum == 0 {
		result, err := runnerlsds.GetLSDSIns().Run(sp, body)
		if err != nil {
			zap.S().Errorw("static middleware run error", "err", err)
			return nil, middleware.Abort, err
		}
		return result, middleware.Abort, nil
	}
	return nil, middleware.Next, nil
}

func (static *StaticMiddleware) GetSource() middleware.Source {
	return StaticMiddlewareSource
}
