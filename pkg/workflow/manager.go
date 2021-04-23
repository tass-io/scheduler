package workflow

import (
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
)

var manager *Manager

func ManagerInit(runner runner.Runner) {
	manager = NewManager(runner)
	manager.start()
}

type Manager struct {
	workflows map[string]workflow
	runner    runner.Runner
}

func GetManagerIns() *Manager {
	return manager
}

// NewManager will use path to init workflow from file
func NewManager(runner runner.Runner) *Manager {
	return &Manager{
		workflows: map[string]workflow{},
		runner:    runner,
	}
}

// Start will watch Workflow related CRD
func (m *Manager) start() {

}

func (m *Manager) Invoke(parameters map[string]interface{}, workflowName string, stepName string) (result map[string]interface{}, err error) {
	sp := span.Span{
		WorkflowName: workflowName,
		StepName:     stepName,
		FunctionName: "",
	}
	return m.runner.Run(parameters, sp)
}
