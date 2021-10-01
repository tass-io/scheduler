package qps

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

const (
	QPSMiddlewareSource middleware.Source = "QPS"
)

var (
	// qpsmiddle is the middleware for statisticsis
	qpsmiddle *Middleware
)

// init initializes the qps middleware
func init() {
	qpsmiddle = newQPSMiddleware()
}

// Register registers the qps middleware as a priority of 10
func Register() {
	middleware.Register(QPSMiddlewareSource, qpsmiddle, 10)
}

// Middleware is the qps middleware for statisticsis,
// it only does some statisticsis for different functions,
// it does nothing about Event adding
type Middleware struct {
	qpsManagers sync.Map
}

// newQPSMiddleware returns a new QPS middleware
func newQPSMiddleware() *Middleware {
	return &Middleware{
		qpsManagers: sync.Map{},
	}
}

// GetQPSMiddleware returns the QPS middleware instance
func GetQPSMiddleware() *Middleware {
	return qpsmiddle
}

// Handle records metrics when a request comes
func (qps *Middleware) Handle(
	sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {

	qpsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	qpsSpan.Start("qps")
	defer qpsSpan.Finish()

	functionName := qpsSpan.GetFunctionName()
	mgrRaw, _ := qps.qpsManagers.LoadOrStore(functionName, newQPSManager(1000))
	mgr := mgrRaw.(*Manager)
	mgr.Inc()
	zap.S().Debugw("qps handler inc", "functionName", functionName)
	return nil, middleware.Next, nil
}

// GetSource returns the QPS middleware source
func (qps *Middleware) GetSource() middleware.Source {
	return QPSMiddlewareSource
}

// GetStat returns the qps number of each function which is concurrency safe.
func (qps *Middleware) GetStat() map[string]int64 {
	stats := map[string]int64{}
	qps.qpsManagers.Range(func(key, value interface{}) bool {
		mgr := value.(*Manager)
		stats[key.(string)] = mgr.Get()
		return true
	})
	zap.S().Debugw("get stats at QPSMiddleware", "stats", stats)
	return stats
}
