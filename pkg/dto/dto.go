package dto

type InvokeRequest struct {
	WorkflowName string                 `json:"workflowName"`
	StepName     string                 `json:"stepName"`
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
