package workflow

import (
	"errors"
	"sync"

	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/common"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

var (
	NO_START_FOUND_ERROR    = errors.New("no start found")
	INVALID_STATEMENT_ERROR = errors.New("statement is invalid")
	FLOW_NOT_FOUND_ERROR    = errors.New("flow not found")
)

type Promise struct {
	wg   sync.WaitGroup
	name string
	f    func(map[string]interface{}, *serverlessv1alpha1.Workflow, span.Span) (map[string]interface{}, error)
	res  map[string]interface{}
	err  error
}

func NewPromise(f func(map[string]interface{}, *serverlessv1alpha1.Workflow, span.Span) (map[string]interface{}, error), name string) *Promise {
	p := &Promise{
		f:    f,
		name: name,
	}
	return p
}
func (p *Promise) Run(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, sp span.Span) {
	p.wg.Add(1)
	go func() {
		p.res, p.err = p.f(parameters, wf, sp)
		p.wg.Done()
	}()
}

func (p *Promise) GetResult() (map[string]interface{}, error) {
	p.wg.Wait()
	return p.res, p.err
}

func (m *Manager) executeSpec(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, sp span.Span) (map[string]interface{}, error) {
	zap.S().Debugw("executeSpec start", "parameters", wf)
	para, err := common.CopyMap(parameters)
	if err != nil {
		return nil, err
	}
	target, err := findFlowByName(wf, sp.FunctionName)
	if err != nil {
		zap.S().Debugw("executeSpec findFlowByName error", "err", err, "span", sp)
		return nil, err
	}
	result, err := m.executeRunFunction(para, wf, target)
	if err != nil {
		return nil, err
	}

	if isEnd(&wf.Spec.Spec[target]) {
		return result, nil
	}
	nexts, err := findNext(result, wf, target) // has serveral next functions
	if err != nil {
		return nil, err
	}
	promises := []*Promise{}
	for _, next := range nexts {
		// think about all next is like a new workflow
		newSp := span.Span{
			WorkflowName: sp.WorkflowName,
			FunctionName: wf.Spec.Spec[next].Function,
		}
		p := NewPromise(m.executeSpec, newSp.FunctionName)
		p.Run(result, wf, newSp)
		promises = append(promises, p)
	}

	finalResult := make(map[string]interface{}, len(promises))

	for _, p := range promises {
		resp, err := p.GetResult()
		if err != nil {
			return nil, err
		}
		if len(promises) > 1 {
			finalResult[p.name] = resp
		} else {
			finalResult = resp
		}
	}
	return finalResult, nil
}

// executeRunFunctionJust Run function no other logics
func (m *Manager) executeRunFunction(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, index int) (map[string]interface{}, error) {

	sp := span.Span{
		WorkflowName: wf.Name,
		FunctionName: wf.Spec.Spec[index].Function,
	}
	return m.runner.Run(parameters, sp)
}

func findStart(wf *serverlessv1alpha1.Workflow) (string, error) {
	for _, flow := range wf.Spec.Spec {
		if flow.Role == serverlessv1alpha1.Start {
			return flow.Function, nil
		}
	}
	return "", NO_START_FOUND_ERROR
}

func isEnd(flow *serverlessv1alpha1.Flow) bool {
	return flow.Role == serverlessv1alpha1.End
}

func findFlowByName(wf *serverlessv1alpha1.Workflow, name string) (int, error) {
	for i, flow := range wf.Spec.Spec {
		if flow.Name == name {
			return i, nil
		}
	}
	return -1, FLOW_NOT_FOUND_ERROR
}

func findNext(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, target int) ([]int, error) {
	now := wf.Spec.Spec[target]
	switch now.Statement {
	case serverlessv1alpha1.Direct:
		{
			nexts := make([]int, 0, len(now.Outputs))
			for _, name := range now.Outputs {
				n, err := findFlowByName(wf, name)
				if err != nil {
					zap.S().Debugw("find next findFlowByName error", "err", err, "name", name)
					return nil, err
				}
				nexts = append(nexts, n)
			}
			return nexts, nil
		}
	case serverlessv1alpha1.Switch:
		{
		}
	default:
		{
			return []int{-1}, INVALID_STATEMENT_ERROR
		}
	}
	return nil, nil
}

func executeCondition(condition *serverlessv1alpha1.Condition, parameters map[string]interface{}) {
	
}
