package prestart

import (
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
	wf string // name of workflow
	pm []predictItem
}

var _ Prestarter = &manager{}

type predictItem struct {
	name  string
	sleep time.Duration
}

func GetPrestarter(workflowName string) Prestarter {
	if mgr == nil {
		newPrestarter(workflowName)
	}
	return mgr
}

func newPrestarter(workflowName string) {
	// FIXME: mock data
	mgr = &manager{wf: workflowName, pm: []predictItem{
		{name: "function1", sleep: 0},
		{name: "function2", sleep: 0},
		{name: "function3", sleep: 0},
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
		<-ticker.C
		// TODO: get prediction model from real prediction model service
		m.pm = []predictItem{
			{name: "function1", sleep: 0},
			{name: "function2", sleep: 0},
			{name: "function3", sleep: 0},
		}
	}
}

// dryRun runs the middleware handler functions without actually executing the workflow.
func (m *manager) dryRun(execMiddlewareFunc execMiddlewareFunc) {
	for _, item := range m.pm {
		go func(i predictItem) {
			zap.S().Info("prestarting", zap.String("function", i.name))
			time.Sleep(i.sleep)
			sp := constructSpan(m.wf, i.name)
			execMiddlewareFunc(sp, nil)
		}(item)
	}
}

func constructSpan(wf, fn string) *span.Span {
	return span.NewSpan(wf, "", fn)
}
