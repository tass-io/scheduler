package workflow

import (
	"context"
	"time"

	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
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
	manager *Manager
)

type Manager struct {
	ctx      context.Context
	runner   runner.Runner
	stopCh   chan struct{}
	informer cache.SharedInformer
}

func GetManagerIns() *Manager {
	return manager
}

// NewManager will use path to init workflow from file
func NewManager() *Manager {
	m := &Manager{
		ctx:    context.Background(),
		runner: runner.NewRunner(),
		stopCh: make(chan struct{}),
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

func (m *Manager) getWorkflowByName(name string) (*serverlessv1alpha1.Workflow, bool, error) {
	key := k8sutils.GetSelfNamespace() + "/" + name
	obj, existed, err := m.informer.GetStore().GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		return nil, false, nil
	}
	wf, ok := obj.(*serverlessv1alpha1.Workflow)
	if !ok {
		panic(obj)
	}
	return wf, true, nil
}

// handleWorkflow is the core function at manager, it will execute Workflow defined logic, call runner.Run and return the final result
func (m *Manager) handleWorkflow(parameters map[string]interface{}, sp span.Span) (map[string]interface{}, error) {
	_, _, _ = m.getWorkflowByName(sp.WorkflowName)
	return nil, nil
}

func (m *Manager) Invoke(parameters map[string]interface{}, workflowName string, stepName string) (result map[string]interface{}, err error) {
	sp := span.Span{
		WorkflowName: workflowName,
		StepName:     stepName,
		FunctionName: "",
	}
	return m.handleWorkflow(parameters, sp)
}
