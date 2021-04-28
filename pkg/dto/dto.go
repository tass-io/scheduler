package dto

type InvokeRequest struct {
	WorkflowName string                 `json:"workflowName"`
	FunctionName string                 `json:"functionName"`
	Parameters   map[string]interface{} `json:"parameters"`
}

type InvokeResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Result  map[string]interface{} `json:"result"`
}

type WorkFlowResult struct {
	Success bool `json:"success"`
}
