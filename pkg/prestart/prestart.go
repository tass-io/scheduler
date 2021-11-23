package prestart

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/predictmodel"
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
	mu     sync.Locker
	ctx    context.Context
	cancel context.CancelFunc      // cancel the context when the workflow is changed.
	wf     string                  // name of workflow
	model  *predictmodel.Model     // prediction model
	mlp    map[string]*flowRuntime // most likely path
}

var _ Prestarter = &manager{}

// flowRuntime is a struct using for calculating mlp
type flowRuntime struct {
	flow         string
	fn           string
	probability  float64
	avgColdStart time.Duration
	avgExec      time.Duration
	minWaiting   time.Duration // minimum total waiting time from the upstream flows
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
		mu:     &sync.Mutex{},
		wf:     workflowName,
		model:  predictmodel.GetPredictModelManager().GetModel(),
		mlp:    make(map[string]*flowRuntime),
	}
	mgr.calculateMLP()
	// mgr.printModel()
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
			m.mu.Lock()
			m.model = predictmodel.GetPredictModelManager().GetModel()
			m.mu.Unlock()
			m.calculateMLP()
			// mgr.printModel()
		}
	}
}

func (m *manager) calculateMLP() {
	mlp := make(map[string]*flowRuntime)
	modelStart := m.model.Flows[m.model.Start]
	if modelStart.TotalExec == 0 {
		m.mlp = mlp
		return
	}
	start := &flowRuntime{
		flow:         modelStart.Flow,
		fn:           modelStart.Fn,
		probability:  modelStart.Probability,
		avgColdStart: modelStart.AvgColdStart,
		avgExec:      modelStart.AvgExec,
	}
	mlp[m.model.Start] = start
	queue := []*flowRuntime{start}
	for len(queue) > 0 {
		newQueue := make([]*flowRuntime, 0)
		// for each flow in the queue, calculate the next most likely flows
		for _, fr := range queue {
			nextFlows := m.model.Flows[fr.flow].Nexts
			highestProbability := 0.0
			candidates := []string{}
			for _, flowName := range nextFlows {
				if m.model.Flows[flowName].Probability > highestProbability {
					highestProbability = m.model.Flows[flowName].Probability
					candidates = []string{flowName}
				} else if m.model.Flows[flowName].Probability == highestProbability {
					candidates = append(candidates, flowName)
				}
			}
			// for all chosen candidates, create a new flowRuntime ptr
			for _, candidate := range candidates {
				newFlowRuntime := &flowRuntime{
					flow:         candidate,
					fn:           m.model.Flows[candidate].Fn,
					probability:  m.model.Flows[candidate].Probability,
					avgColdStart: m.model.Flows[candidate].AvgColdStart,
					avgExec:      m.model.Flows[candidate].AvgExec,
				}
				// check wether other flowRuntimes have been added this flowRuntime in mlp
				if _, ok := mlp[candidate]; !ok {
					mlp[candidate] = newFlowRuntime
					// add the new flowRuntime to the queue
					newQueue = append(newQueue, newFlowRuntime)
				}
			}
		}
		queue = newQueue
	}
	calculateFlowWaiting(m.model, mlp)
	m.mlp = mlp
}

func calculateFlowWaiting(model *predictmodel.Model, mlp map[string]*flowRuntime) {
	helper := make(map[string]bool)
	queue := []string{}
	for key := range mlp {
		if key != model.Start {
			queue = append(queue, key)
			helper[key] = true
		}
	}
	for len(queue) > 0 {
		flow := queue[0]
		queue = queue[1:]
		parents := []string{}
		parents = append(parents, model.Flows[flow].Parents...)
		minWaiting := time.Duration(math.MaxInt64)
		shouldRequeue := false
		for _, parent := range parents {
			// check if parents is in mlp, if not, we don't need to calculate the waiting time
			_, ok := mlp[parent]
			// if parents is in mlp, calculate the minWaiting time based on the parent flowRuntime
			if ok {
				if _, helperOk := helper[parent]; helperOk {
					// parent doesn't finish yet, so we can't calculate the minWaiting time now, requeue it
					shouldRequeue = true
					break
				}
				pFlow := mlp[parent]
				// waiting = parent_waiting + parent_avg_exec + parent_avg_coldstart - flow_avg_coldstart
				waiting := pFlow.minWaiting + pFlow.avgColdStart + pFlow.avgExec - mlp[flow].avgColdStart
				if waiting < minWaiting {
					minWaiting = waiting
				}
				// min waiting cannot be smaller than 0, have found the minWaiting time
				if minWaiting <= 0 {
					minWaiting = 0
					break
				}
			}
		}
		if shouldRequeue {
			queue = append(queue, flow)
			continue
		}
		// if don't need to requeue, update the flowRuntime minWaiting time and delete the flowName in helper
		mlp[flow].minWaiting = minWaiting
		delete(helper, flow)
	}
}

// NOTE: Test help function for watching runtime status.
func (m *manager) PrintModel() {
	fmt.Println("===================Probability Model======================")
	for _, flow := range m.model.Flows {
		fmt.Println("flow:", flow.Flow)
		fmt.Println("fn", flow.Fn)
		fmt.Println("probability", flow.Probability)
		fmt.Printf("AvgColdstart: %v \n", flow.AvgColdStart)
		fmt.Printf("AvgExec: %v \n", flow.AvgExec)
	}
	fmt.Println("===========================MLP============================")
	for _, flowRuntime := range m.mlp {
		fmt.Println("flow:", flowRuntime.flow)
		fmt.Println("fn", flowRuntime.fn)
		fmt.Println("probability", flowRuntime.probability)
		fmt.Printf("minWaiting: %v \n", flowRuntime.minWaiting)
	}
	fmt.Println("==========================================================")
}

// dryRun runs the middleware handler functions without actually executing the workflow.
func (m *manager) dryRun(execMiddlewareFunc execMiddlewareFunc) {
	for _, item := range m.mlp {
		go func(i *flowRuntime) {
			time.Sleep(i.minWaiting)
			sp := constructPlainSpan(m.wf, i.flow, i.fn)
			execMiddlewareFunc(sp, nil)
		}(item)
	}
}

func constructPlainSpan(wf, flow, fn string) *span.Span {
	return span.NewSpan(wf, "", flow, fn)
}
