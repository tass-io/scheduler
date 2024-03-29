package workflow

import (
	"context"
	"errors"
	"sync"

	"github.com/tass-io/scheduler/pkg/collector"
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/predictmodel"
	"github.com/tass-io/scheduler/pkg/prestart"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

var (
	manager             *Manager
	_                   = &sync.Once{}
	ErrWorkflowNotFound = errors.New("workflow not found")
)

// InitManager inits a workflow manager,
// unlike lsds and other things, Manager should not lazy start
func InitManager() {
	manager = NewManager()
}

// Manager is a workflow manager, it owns a runner and middlewares
type Manager struct {
	ctx                      context.Context
	runner                   runner.Runner
	stopCh                   chan struct{}
	events                   map[event.Source]event.Handler
	middlewares              map[middleware.Source]middleware.Handler
	orderedMiddlewareSources []middleware.Source // in increasing order
}

// GetManager returns a Manager instance
func GetManager() *Manager {
	return manager
}

// GetEventHandlerBySource returns a EventHandler by the given source,
// it's a help function for event upstream,
// an example: `GetManagerIns().GetEventHandlerBySource(source)`
func (m *Manager) GetEventHandlerBySource(source event.Source) event.Handler {
	return m.events[source]
}

// GetMiddlewareBySource returns a middleware handler by the given source
// it's a help function for event upstream,
// an example: `GetManagerIns().GetMiddlewareBySource(source)`
func (m *Manager) GetMiddlewareBySource(source middleware.Source) middleware.Handler {
	return m.middlewares[source]
}

// NewManager uses path to init workflow from file
func NewManager() *Manager {
	m := &Manager{
		ctx:                      context.Background(),
		runner:                   helper.GetMasterRunner(),
		stopCh:                   make(chan struct{}),
		events:                   nil,
		middlewares:              nil,
		orderedMiddlewareSources: nil,
	}
	return m
}

// Start links all middleware and events to the manager itsself,
// it then starts all events handlers
func (m *Manager) Start() {
	m.middlewares = middleware.GetHandlers()
	m.orderedMiddlewareSources = middleware.GetOrderedSources()
	m.events = event.GetHandlers()
	err := m.startEvents()
	if err != nil {
		zap.S().Panic(err)
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

// getWorkflowByName gets workflow by name via k8s apiserver
func (m *Manager) getWorkflowByName(name string) (*serverlessv1alpha1.Workflow, bool, error) {
	return k8sutils.GetWorkflowByName(name)
}

// handleWorkflow is the core function in manager,
// it executes Workflow defined logic, calls runner.Run and returns the final result
func (m *Manager) handleWorkflow(sp *span.Span, parameters map[string]interface{}) (map[string]interface{}, error) {
	// sp is root as parent
	workflowName := sp.GetWorkflowName()
	workflow, existed, err := m.getWorkflowByName(workflowName)
	if err != nil {
		return nil, err
	}
	if !existed {
		zap.S().Errorw("workflow not found", "workflow", workflowName)
		return nil, ErrWorkflowNotFound
	}
	if sp.GetFlowName() == "" {
		flowName, functionName, err := findStart(workflow)
		sp.SetFlowName(flowName)
		sp.SetFunctionName(functionName)
		if err != nil {
			return nil, err
		}
	}
	sp.Start("")
	err = m.preparePrescheduleSuite(workflowName)
	if err != nil {
		return nil, err
	}
	// flow level span here
	return m.executeSpec(sp, parameters, workflow) // Start and Finish not symmetric
}

// GetRunner returns the manager runner
func (m *Manager) GetRunner() runner.Runner {
	return m.runner
}

// Invoke invokes the workflow and does the difined workflow logic
func (m *Manager) Invoke(sp *span.Span, parameters map[string]interface{}) (result map[string]interface{}, err error) {
	return m.handleWorkflow(sp, parameters)
}

// preparePrescheduleSuite prepares the preschedule suite, including
// 1. a prediction model manager to load the workflow prediction model
// 2. a collector to collect the process instance lifecycle cost data and upload the record;
// 3. a prestartar to create a process instance based on the pridiction model;
// Note that the order is important
func (m *Manager) preparePrescheduleSuite(workflowName string) error {
	err := predictmodel.Init(workflowName)
	if err != nil {
		zap.S().Error("prepare workflow preschedule suite failed", err, "workflow", workflowName)
		return err
	}
	collector.Init(workflowName)
	prestart.Init(workflowName)
	prestart.GetPrestarter().Trigger(m.middleware)
	return nil
}
