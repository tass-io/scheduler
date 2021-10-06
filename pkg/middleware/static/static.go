package static

import (
	"github.com/tass-io/scheduler/pkg/middleware"
	runnerlsds "github.com/tass-io/scheduler/pkg/runner/lsds"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

var (
	// static is the static middleware that assumes some functions has been registered
	// this middleware is a middleware to be convenient to test
	static *staticMiddleware
)

// init initializes an instance of static middleware
func init() {
	static = newStaticMiddleware()
}

// Register registers the static middleware as a priority of 1
func Register() {
	middleware.Register(middleware.StaticMiddlewareSource, static, 1)
}

// staticMiddleware is a middleware to help testing
type staticMiddleware struct{}

var _ middleware.Handler = &staticMiddleware{}

// newStaticMiddleware returns a new static middleware
func newStaticMiddleware() *staticMiddleware {
	return &staticMiddleware{}
}

// Handle receives a request and does static middleware logic,
// if it finds that the function not exists, it goes to lsds instance directly.
// Because the piority of static middleware is higher than the piority of lsds middleware,
// it can guarantee the function has been to other processes before the local execution.
func (static *staticMiddleware) Handle(
	sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {

	staticSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	staticSpan.Start("static")
	defer staticSpan.Finish()

	// use runner api instead of workflow api to reduce coupling
	instanceStatus := runnerlsds.GetLSDSIns().Stats()
	zap.S().Infow("get master runner stats at static", "stats", instanceStatus)
	instanceNum, existed := instanceStatus[sp.GetFunctionName()]
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
func (static *staticMiddleware) GetSource() middleware.Source {
	return middleware.StaticMiddlewareSource
}
