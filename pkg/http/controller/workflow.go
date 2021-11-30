package controller

import (
	"net/http"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/trace"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
)

// Invoke is called when a http request is received
func Invoke(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var request dto.WorkflowRequest
	request.WorkflowName = r.URL.Query().Get("workflow")
	request.FlowName = r.URL.Query().Get("flow")
	request.UpstreamFlowName = r.URL.Query().Get("upstream")

	// 1. mapping into JSON format
	request.Parameters = map[string]interface{}{"body": r.Body}

	// 2. record opentracing span
	var root opentracing.Span
	spanContext, err := trace.GetSpanContextFromHeaders(request.WorkflowName, r.Header)
	if err != nil {
		if err == opentracing.ErrSpanContextNotFound {
			// when the workflow executes the first Flow, it has no span context
			root = opentracing.GlobalTracer().StartSpan(request.WorkflowName)
			spanContext = root.Context()
		} else {
			zap.S().Errorw("trace get spanContext error", err)
		}
	}
	defer func() {
		if root != nil {
			root.Finish()
			zap.S().Debugw("root finish", "root", root)
		}
	}()
	zap.S().Debugw("get spanContext", "context", spanContext)
	// functionName can be found in the Workflow model
	sp := span.NewSpan(request.WorkflowName, request.UpstreamFlowName, request.FlowName, "")
	sp.SetRoot(spanContext)
	sp.SetParent(spanContext)

	// 3. invoke the busniess logic
	result, err := workflow.GetManager().Invoke(sp, request.Parameters)
	dto := dto.WorkflowResponse{}
	if err != nil {
		dto.Success = false
		dto.Message = err.Error()
		// 500
	} else {
		dto.Success = true
		dto.Result = result
		dto.Message = "ok"
		// 200
	}
	dto.Time = time.Since(start).String()
	// write
}
