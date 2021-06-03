package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/opentracing/opentracing-go"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/trace"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
)

// Invoke is called when a http request is received
func Invoke(c *gin.Context) {
	var request dto.InvokeRequest
	if err := c.BindJSON(&request); err != nil {
		zap.S().Errorw("invoke bind json error", "err", err)
		c.JSON(400, dto.InvokeResponse{
			Success: false,
			Message: err.Error(),
		})
	}

	var root opentracing.Span
	spanContext, err := trace.GetSpanContextFromHeaders(request.WorkflowName, c.Request.Header)
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
	sp := span.NewSpan(request.WorkflowName, request.FlowName, "") // functionName can find in the Workflow model
	sp.SetRoot(spanContext)
	sp.SetParent(spanContext)
	result, err := workflow.GetManagerIns().Invoke(sp, request.Parameters)
	if err != nil {
		c.JSON(500, dto.InvokeResponse{
			Success: false,
			Message: err.Error(),
		})
		return
	}
	c.JSON(200, dto.InvokeResponse{
		Success: true,
		Message: "ok",
		Result:  result,
	})
}
