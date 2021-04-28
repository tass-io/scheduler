package span

// Span is a context info for a request
// scheduler plan to use jaeger implement tracing
// may be the struct will put context.Context into
type Span struct {
	WorkflowName string
	FunctionName string
}
