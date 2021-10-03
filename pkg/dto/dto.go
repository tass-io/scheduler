package dto

type WorkflowRequest struct {
	WorkflowName string                 `json:"workflowName"`
	FlowName     string                 `json:"flowName"`
	Parameters   map[string]interface{} `json:"parameters"`
}

type WorkflowResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Result  map[string]interface{} `json:"result"`
}

type WorkFlowResult struct {
	Success bool `json:"success"`
}
