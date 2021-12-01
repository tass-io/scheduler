package controller

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/trace"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
)

// Invoke is called when a http request is received
func Invoke(w http.ResponseWriter, r *http.Request) {
	var request dto.WorkflowRequest
	request.WorkflowName = r.URL.Query().Get("workflow")
	request.FlowName = r.URL.Query().Get("flow")
	request.UpstreamFlowName = r.URL.Query().Get("upstream")

	zap.S().Info("a request is just coming.")
	out, err := io.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		zap.S().Panicw("request reading body error", "err", err)
	}
	zap.S().Infow("request body reading done", "readed:", len(out))

	start := time.Now()
	// 1. mapping into JSON format
	request.Parameters = map[string]interface{}{"body": out}

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
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	dto.Success = true
	dto.Result = result
	dto.Message = "ok"
	dto.Time = time.Since(start).String()
	h := w.Header()
	h["tass-success"] = []string{fmt.Sprintf("%v", dto.Success)}
	h["tass-message"] = []string{fmt.Sprintf("%v", dto.Message)}
	h["tass-start"] = []string{fmt.Sprintf("%v", start)}
	wf := func(input []byte) {
		n, err := w.Write(input)
		if n != len(input) || err != nil {
			zap.S().Error(fmt.Sprintf("WTF?! n %v err %v", n, err))
		}
	}
	wf([]byte(fmt.Sprintf(`{"resultsLen":%v,"time":"%v"}`, instance.MmapBytes, dto.Time)))
}
