package span

import (
	"net/http"
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

// Span is a context info for a request
// scheduler plan to use jaeger implement tracing
// may be the struct will put context.Context into
// all flows are the same level
// all conditions in the "conditions" are the same level
type Span struct {
	workflowName string
	flowName     string
	functionName string
	root         opentracing.SpanContext
	parent       opentracing.SpanContext
	sp           opentracing.Span
	startOnce    *sync.Once
	finishOnce   *sync.Once
}

// NewSpan returns a new span with the input function.
// Note that you should set root and parent by yourself.
func NewSpan(workflowName string, flowName string, functioName string) *Span {
	return &Span{
		workflowName: workflowName,
		flowName:     flowName,
		functionName: functioName,
		startOnce:    &sync.Once{},
		finishOnce:   &sync.Once{},
	}
}

// NewSpanFromTheSameFlowSpanAsParent returns a new Span which is the input Span is the parent.
// As these two Span are in the same Workflow, they have the same root Span.
func NewSpanFromTheSameFlowSpanAsParent(sp *Span) *Span {
	var parent opentracing.SpanContext
	if sp.sp != nil {
		parent = sp.sp.Context()
	} else {
		zap.L().Warn("sp.sp is nil")
		// panic(sp) // for some test, there are no span
	}
	return &Span{
		workflowName: sp.workflowName,
		flowName:     sp.flowName,
		functionName: sp.functionName,
		root:         sp.root,
		parent:       parent,
		startOnce:    &sync.Once{},
		finishOnce:   &sync.Once{},
	}
}

// NewSpanFromSpanSibling returns a new Span which is in the same level as the input Span.
func NewSpanFromSpanSibling(sp *Span) *Span {
	return &Span{
		workflowName: sp.workflowName,
		root:         sp.root,
		// FIXME: Can this be `sp.parent` ?
		parent:     sp.root,
		startOnce:  &sync.Once{},
		finishOnce: &sync.Once{},
	}
}

func (span *Span) GetFunctionName() string {
	return span.functionName
}

func (span *Span) GetWorkflowName() string {
	return span.workflowName
}

func (span *Span) GetFlowName() string {
	return span.flowName
}

func (span *Span) GetRoot() opentracing.SpanContext {
	return span.root
}

func (span *Span) GetParent() opentracing.SpanContext {
	return span.parent
}

func (span *Span) SetFunctionName(functionName string) {
	span.functionName = functionName
}

func (span *Span) SetFlowName(flowName string) {
	span.flowName = flowName
}

func (span *Span) SetRoot(root opentracing.SpanContext) {
	span.root = root
}

func (span *Span) SetParent(parent opentracing.SpanContext) {
	span.parent = parent
}

// Start starts a Span, use sync.Once to make sure it only start once
func (span *Span) Start(name string) {
	if span.sp != nil {
		zap.S().Panic(span)
	}

	span.startOnce.Do(func() {
		spanName := name
		if span.parent == nil {
			return
		}
		if spanName == "" {
			spanName = span.flowName
		}
		span.sp = opentracing.StartSpan(spanName, opentracing.ChildOf(span.parent))
	})
}

// StartFromRoot starts a Span,
// this a special case that the span is child of the root
func (span *Span) StartFromRoot(name string) {
	spanName := name
	if span.root == nil {
		return
	}
	if spanName == "" {
		spanName = span.workflowName
	}
	span.sp = opentracing.StartSpan(spanName, opentracing.ChildOf(span.root))
}

// Finish ends a span, use sync.Once to make sure it only finish once
func (span *Span) Finish() {
	if span.sp == nil {
		// todo check
		return
	}
	span.finishOnce.Do(func() {
		span.sp.Finish()
	})
}

// InjectRoot injects root span into http header
func (span *Span) InjectRoot(header http.Header) {
	if span.root == nil {
		return
	}
	carrier := opentracing.HTTPHeadersCarrier(header)
	err := opentracing.GlobalTracer().Inject(span.root, opentracing.HTTPHeaders, carrier)
	if err != nil {
		zap.S().Errorw("err at inject jaeger header", "err", err)
	}
}
