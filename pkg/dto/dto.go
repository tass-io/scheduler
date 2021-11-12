package dto

type WorkflowRequest struct {
	WorkflowName     string                 `json:"workflowName"`
	UpstreamFlowName string                 `json:"upstreamFlowName"`
	FlowName         string                 `json:"flowName"`
	Parameters       map[string]interface{} `json:"parameters"`
}

type WorkflowResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Time    string                 `json:"time"`
	Result  map[string]interface{} `json:"result"`
}

type WorkFlowResult struct {
	Success bool `json:"success"`
}
