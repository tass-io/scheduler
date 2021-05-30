package workflow

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/span"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
)

// FlowPromise is an abstraction like javascript Promise
type FlowPromise struct {
	wg   sync.WaitGroup
	name string
	f    func(map[string]interface{}, *serverlessv1alpha1.Workflow, *span.Span) (map[string]interface{}, error)
	res  map[string]interface{}
	err  error
}

func NewFlowPromise(f func(map[string]interface{}, *serverlessv1alpha1.Workflow, *span.Span) (map[string]interface{}, error), name string) *FlowPromise {
	p := &FlowPromise{
		f:    f,
		name: name,
	}
	return p
}

func (p *FlowPromise) Run(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, sp *span.Span) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.res, p.err = p.f(parameters, wf, sp)
	}()
}

func (p *FlowPromise) GetResult() (map[string]interface{}, error) {
	p.wg.Wait()
	return p.res, p.err
}

// CondPromise is an abstraction like javascript Promise
type CondPromise struct {
	wg   sync.WaitGroup
	name string
	f    func(condition *serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow, target int, functionResult map[string]interface{}) (map[string]interface{}, error)
	res  map[string]interface{}
	err  error
}

func NewCondPromise(f func(condition *serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow, target int, functionResult map[string]interface{}) (map[string]interface{}, error), name string) *CondPromise {
	p := &CondPromise{
		f:    f,
		name: name,
	}
	return p
}

func (p *CondPromise) Run(condition *serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow, target int, functionResult map[string]interface{}) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.res, p.err = p.f(condition, wf, target, functionResult)
	}()
}

func (p *CondPromise) GetResult() (map[string]interface{}, error) {
	p.wg.Wait()
	return p.res, p.err
}
