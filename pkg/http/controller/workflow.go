package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/workflow"
)

func Invoke(c *gin.Context) {
	var request dto.InvokeRequest
	if err := c.BindJSON(&request); err != nil {
		c.JSON(400, dto.InvokeResponse{
			Success: false,
			Message: err.Error(),
		})
	}
	result, err := workflow.GetManagerIns().Invoke(request.Parameters, request.WorkflowName, request.StepName)
	if err != nil {
		c.JSON(500, dto.InvokeResponse{
			Success: false,
			Message: err.Error(),
		})
	}
	c.JSON(200, dto.InvokeResponse{
		Success: true,
		Message: "ok",
		Result:  result,
	})
}
