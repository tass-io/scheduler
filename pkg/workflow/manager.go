package workflow

import (
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/runner"
)

var manager *Manager

func ManagerInit(runner runner.Runner) {
	manager = NewManager(viper.GetString(env.WORKFLOW_PATH), runner)
}

type Manager struct {
	workflows map[string]workflow
	runner    runner.Runner
}

func GetManagerIns() *Manager {
	return manager
}

// NewManager will use path to init workflow from file
func NewManager(path string, runner runner.Runner) *Manager {
	return nil
}

func (m *Manager) Invoke(parameters map[string]interface{}, workflowName string, stepName string) (result map[string]interface{}, err error) {
	return nil, nil
}
