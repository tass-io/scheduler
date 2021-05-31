package workflow

import (
	"context"
	"errors"
	"sync"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/event/source"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

// unlike lsds and other things, Manager should not lazy start
func ManagerInit() {
	manager = NewManager()
}

var (
	manager               *Manager
	once                  = &sync.Once{}
	ErrWorkflowNotFound = errors.New("workflow not found")
)

type Manager struct {
	ctx             context.Context
	runner          runner.Runner
	stopCh          chan struct{}
	events          map[source.Source]event.Handler
	middlewareOrder []middleware.Source
	middlewares     map[middleware.Source]middleware.Handler
}

func GetManagerIns() *Manager {
	return manager
}

// help function for event upstream, most of times will use like `GetManagerIns().GetEventHandlerBySource(source)`
func (m *Manager) GetEventHandlerBySource(source source.Source) event.Handler {
	return m.events[source]
}

// help function for event upstream, most of times will use like `GetManagerIns().GetMiddlewareBySource(source)`
func (m *Manager) GetMiddlewareBySource(source middleware.Source) middleware.Handler {
	return m.middlewares[source]
}

// NewManager will use path to init workflow from file
func NewManager() *Manager {
	m := &Manager{
		ctx:             context.Background(),
		runner:          helper.GetMasterRunner(),
		stopCh:          make(chan struct{}),
		events:          nil,
		middlewareOrder: nil,
		middlewares:     nil,
	}
	return m
}

// Start links all middleware and events to the manager itsself,
// it then starts all events handlers
func (m *Manager) Start() {
	m.middlewares = middleware.Middlewares()
	m.events = event.Events()
	err := m.startEvents()
	if err != nil {
		panic(err)
	}
}

// startEvents iterates the event handlers and starts them
func (m *Manager) startEvents() error {
	errstr := ""
	for src, h := range m.events {
		err := h.Start()
		if err != nil {
			errstr += ";" + err.Error()
		}
		zap.S().Debugf("%s event starts with error %v\n", src, err)
	}
	if errstr != "" {
		return errors.New(errstr)
	}
	return nil
}

// getWorkflowByName get workflow by name via k8s apiserver
func (m *Manager) getWorkflowByName(name string) (*serverlessv1alpha1.Workflow, bool, error) {
	return k8sutils.GetWorkflowByName(name)
}

// handleWorkflow is the core function at manager, it will execute Workflow defined logic, call runner.Run and return the final result
func (m *Manager) handleWorkflow(parameters map[string]interface{}, sp span.Span) (map[string]interface{}, error) {
	workflow, existed, err := m.getWorkflowByName(sp.WorkflowName)
	if err != nil {
		return nil, err
	}
	if !existed {
		zap.S().Errorw("workflow not found", "workflowname", sp.WorkflowName)
		return nil, ErrWorkflowNotFound
	}
	if sp.FlowName == "" {
		sp.FlowName, sp.FunctionName, err = findStart(workflow)
		if err != nil {
			return nil, err
		}
	}
	return m.executeSpec(parameters, workflow, sp)
}

func (m *Manager) GetRunner() runner.Runner {
	return m.runner
}

func (m *Manager) Invoke(parameters map[string]interface{}, workflowName string, flowName string) (result map[string]interface{}, err error) {
	sp := span.Span{
		WorkflowName: workflowName,
		FlowName:     flowName,
		FunctionName: "",
	}
	return m.handleWorkflow(parameters, sp)
}
