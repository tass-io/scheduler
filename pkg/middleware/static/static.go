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
	// staticmiddle is the static middleware that assumes some functions has been registered
	// this middleware is a middleware to be convenient to test
	staticmiddle *StaticMiddleware
)

// init initializes an instance of static middleware
func init() {
	staticmiddle = newStaticMiddleware()
}

// Register registers the static middleware as a priority of 1
func Register() {
	middleware.Register(StaticMiddlewareSource, staticmiddle, 1)
}

// StaticMiddleware is a middleware to help testing
type StaticMiddleware struct{}

// newStaticMiddleware returns a new static middleware
func newStaticMiddleware() *StaticMiddleware {
	return &StaticMiddleware{}
}

// GetStaticMiddleware returns a static middleware instance
func GetStaticMiddleware() *StaticMiddleware {
	return staticmiddle
}

// Handle receives a request and does static middleware logic,
// if it finds that the function not exists, it goes to lsds instance directly.
// Because the piority of static middleware is higher than the piority of lsds middleware,
// it can guarantee the function has been to other processes before the local execution.
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

// GetSource returns the static middleware source
func (static *StaticMiddleware) GetSource() middleware.Source {
	return StaticMiddlewareSource
}
