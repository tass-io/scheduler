package workflow

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/span"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
)

// execFunc is the function that executes the business logic
type execFunc func(*span.Span, map[string]interface{}, *serverlessv1alpha1.Workflow) (map[string]interface{}, error)

// FlowPromise is an abstraction like javascript Promise
type FlowPromise struct {
	wg   sync.WaitGroup
	name string
	f    execFunc
	res  map[string]interface{}
	err  error
}

// NewFlowPromise initializes a new FlowPromise
func NewFlowPromise(f execFunc, name string) *FlowPromise {
	p := &FlowPromise{
		f:    f,
		name: name,
	}
	return p
}

// Run runs the function in FlowPromise and the result is recorded in res
func (p *FlowPromise) Run(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, sp *span.Span) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.res, p.err = p.f(sp, parameters, wf)
	}()
}

// GetResult waits for all Run functions
func (p *FlowPromise) GetResult() (map[string]interface{}, error) {
	p.wg.Wait()
	return p.res, p.err
}

// execCondFunc is the function that executes the business logic with workflow conditions
type execCondFunc func(sp *span.Span, condition *serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow,
	target int, functionResult map[string]interface{}) (map[string]interface{}, error)

// CondPromise is an abstraction like javascript Promise
type CondPromise struct {
	wg   sync.WaitGroup
	name string
	f    execCondFunc
	res  map[string]interface{}
	err  error
}

// NewCondPromise initializes a new CondPromise
func NewCondPromise(f execCondFunc, name string) *CondPromise {
	p := &CondPromise{
		f:    f,
		name: name,
	}
	return p
}

// Run runs the function in CondPromise and the result is recorded in res
func (p *CondPromise) Run(sp *span.Span, condition *serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow,
	target int, functionResult map[string]interface{}) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.res, p.err = p.f(sp, condition, wf, target, functionResult)
	}()
}

// GetResult waits for all Run functions
func (p *CondPromise) GetResult() (map[string]interface{}, error) {
	p.wg.Wait()
	return p.res, p.err
}
