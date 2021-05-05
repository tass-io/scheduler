package workflow

import (
	"context"
	"errors"
	"time"

	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

func ManagerInit() {
	manager = NewManager()
}

var (
	WorkflowResource = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "workflows",
	}
	manager               *Manager
	WorkflowNotFoundError = errors.New("workflow not found")
)

type Manager struct {
	ctx             context.Context
	runner          runner.Runner
	stopCh          chan struct{}
	informer        cache.SharedInformer
	events          map[event.Source]event.Handler
	middlewareOrder []middleware.Source
	middlewares     map[middleware.Source]middleware.Handler
}

func GetManagerIns() *Manager {
	return manager
}

// help function for event upstream, most of times will use like `GetManagerIns().GetEventHandlerBySource(source)`
func (m *Manager) GetEventHandlerBySource(source event.Source) event.Handler {
	return m.events[source]
}

// NewManager will use path to init workflow from file
func NewManager() *Manager {
	runner.LDSinit()
	runner.FunctionSchedulerInit()
	m := &Manager{
		ctx:         context.Background(),
		runner:      runner.NewRunner(),
		stopCh:      make(chan struct{}),
		events:      event.Events(),
		middlewareOrder: nil,
		middlewares: middleware.Middlewares(),
	}
	m.start()
	return m
}

// Start will watch Workflow related CRD
func (m *Manager) start() {
	err := m.startListen()
	if err != nil {
		panic(err)
	}
	err = m.startEvents()
	if err != nil {
		panic(err)
	}
}

func (m *Manager) startListen() error {
	zap.S().Debug("in the manager start listen")
	listAndWatch := k8sutils.CreateUnstructuredListWatch(m.ctx, k8sutils.GetSelfNamespace(), WorkflowResource)
	informer := cache.NewSharedInformer(
		listAndWatch,
		&serverlessv1alpha1.WorkflowRuntime{},
		1*time.Second,
	)
	m.informer = informer
	go informer.Run(make(<-chan struct{}))
	return nil
}

func (m *Manager) startEvents() error {
	errstr := ""
	for _, h := range m.events {
		err := h.Start()
		if err != nil {
			errstr += ";" + err.Error()
		}
	}
	if errstr != "" {
		return errors.New(errstr)
	}
	return nil
}

func (m *Manager) getWorkflowByName(name string) (*serverlessv1alpha1.Workflow, bool, error) {
	zap.S().Debugw("manager hold workflow", "workflow", m.informer.GetStore().ListKeys())
	key := k8sutils.GetSelfNamespace() + "/" + name
	obj, existed, err := m.informer.GetStore().GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		return nil, false, nil
	}
	ust := obj.(*unstructured.Unstructured)
	wf := &serverlessv1alpha1.Workflow{}
	runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wf)
	return wf, true, nil
}

// handleWorkflow is the core function at manager, it will execute Workflow defined logic, call runner.Run and return the final result
func (m *Manager) handleWorkflow(parameters map[string]interface{}, sp span.Span) (map[string]interface{}, error) {
	workflow, existed, err := m.getWorkflowByName(sp.WorkflowName)
	if err != nil {
		return nil, err
	}
	if !existed {
		zap.S().Errorw("workflow not found", "workflowname", sp.WorkflowName)
		return nil, WorkflowNotFoundError
	}
	if sp.FunctionName == "" {
		sp.FunctionName, err = findStart(workflow)
		if err != nil {
			return nil, err
		}
	}
	return m.executeSpec(parameters, workflow, sp)
}

func (m *Manager) Invoke(parameters map[string]interface{}, workflowName string, functionName string) (result map[string]interface{}, err error) {
	sp := span.Span{
		WorkflowName: workflowName,
		FunctionName: functionName,
	}
	return m.handleWorkflow(parameters, sp)
}
