package prestart

import (
	"context"
	"time"

	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	mgr *manager
)

const (
	updateMdlFreq   = time.Second * 10
	prestartDisable = "prestart mode is disabled, if you want to turn on prestart mode, set start flag -p or --prestart."
)

type Prestarter interface {
	Trigger(execMiddlewareFunc execMiddlewareFunc)
}

type execMiddlewareFunc func(*span.Span, map[string]interface{}) (map[string]interface{}, middleware.Decision, error)

type manager struct {
	ctx    context.Context
	cancel context.CancelFunc // cancel the context when the workflow is changed.
	wf     string             // name of workflow
	pm     []predictItem
}

var _ Prestarter = &manager{}

type predictItem struct {
	flow  string
	fn    string
	sleep time.Duration
}

// Init initializes a new Prestart singleton instance,
// if workflow changes, it will generate a new instance.
func Init(workflowName string) {
	if mgr == nil {
		newPrestarter(workflowName)
		return
	}
	if mgr.wf != workflowName {
		zap.S().Info("workflow changes, generate a new workflow prestarter")
		newPrestarter(workflowName)
	}
}

func GetPrestarter() Prestarter {
	return mgr
}

func newPrestarter(workflowName string) {
	// if workflow changes, cancel the old mgr goroutines
	if mgr != nil {
		mgr.cancel()
	}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	mgr = &manager{
		ctx:    ctx,
		cancel: cancel,
		wf:     workflowName,
		// FIXME: mock data
		pm: []predictItem{
			{flow: "start", fn: "function1", sleep: 0},
			{flow: "next", fn: "function2", sleep: 0},
			{flow: "end", fn: "function3", sleep: 0},
		}}
	go mgr.updatePredictionModel(updateMdlFreq)
}

// Trigger indicates a workflow request is called,
// it will then execute middleware handlers which will trigger the coldstart process.
// FIXME: if coldstart failed, it will go to lsds to run, which we may don't want to happen.
// we may need add more params to middleware handler to describe our needs.
func (m *manager) Trigger(execMiddlewareFunc execMiddlewareFunc) {
	if !viper.GetBool(env.Prestart) {
		zap.S().Info(prestartDisable)
		return
	}
	m.dryRun(execMiddlewareFunc)
}

func (m *manager) updatePredictionModel(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			// TODO: get prediction model from real prediction model service
			m.pm = []predictItem{
				{flow: "start", fn: "function1", sleep: 0},
				{flow: "next", fn: "function2", sleep: 0},
				{flow: "end", fn: "function3", sleep: 0},
			}
		}
	}
}

// dryRun runs the middleware handler functions without actually executing the workflow.
func (m *manager) dryRun(execMiddlewareFunc execMiddlewareFunc) {
	for _, item := range m.pm {
		go func(i predictItem) {
			zap.S().Info("prestarting", zap.String("function", i.fn))
			time.Sleep(i.sleep)
			sp := constructSpan(m.wf, i.flow, i.fn)
			execMiddlewareFunc(sp, nil)
		}(item)
	}
}

func constructSpan(wf, flow, fn string) *span.Span {
	return span.NewSpan(wf, flow, fn)
}
