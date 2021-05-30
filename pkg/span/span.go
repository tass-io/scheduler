package span

// Span is a context info for a request
// scheduler plan to use jaeger implement tracing
// may be the struct will put context.Context into
type Span struct {
	workflowName string
	flowName     string
	functionName string
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

func (span *Span) SetFunctionName(functionName string) {
	span.functionName = functionName
}

func (span *Span) SetFlowName(flowName string) {
	span.flowName = flowName
}

func NewSpan(workflowName string, flowName string, functioName string) *Span {
	return &Span{
		workflowName: workflowName,
		flowName: flowName,
		functionName: functioName,
	}
}
