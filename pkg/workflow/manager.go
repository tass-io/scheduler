package workflow

import (
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	api "github.com/tass-io/tass-operator/api/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"time"
)

var (
	WorkflowResource = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "Workflow",
	}
	manager *Manager
)

func ManagerInit(runner runner.Runner) {
	manager = NewManager(runner)
	manager.start()
}

type Manager struct {
	runner   runner.Runner
	stopCh   chan struct{}
	informer informers.GenericInformer
}

func GetManagerIns() *Manager {
	return manager
}

// NewManager will use path to init workflow from file
func NewManager(runner runner.Runner) *Manager {
	return &Manager{
		runner: runner,
		stopCh: make(chan struct{}),
	}
}

// Start will watch Workflow related CRD
func (m *Manager) start() {
	err := m.startListen()
	if err != nil {
		panic(err)
	}
}

func (m *Manager) startListen() error {
	k8sclient := k8sutils.GetInformerClientIns()
	factory := informers.NewSharedInformerFactoryWithOptions(k8sclient, 1*time.Second, informers.WithTweakListOptions(func(options *v1.ListOptions) {
		options.LabelSelector = labels.Set(
			map[string]string{
				"type": "workflow",
				"name": k8sutils.GetWorkflowName(),
			}).String()
	}), informers.WithNamespace(k8sutils.GetWorkflowName()))
	informer, err := factory.ForResource(WorkflowResource)
	if err != nil {
		panic(err)
	}
	m.informer = informer
	go informer.Informer().Run(m.stopCh)
	return nil
}

func (m *Manager) getWorkflowByName(name string) (*api.Workflow, error) {
	obj, err := m.informer.Lister().Get(name)
	if err != nil {
		return nil, err
	}
	wf, ok := obj.(*api.Workflow)
	if !ok {
		panic(obj)
	}
	return wf, nil
}

// handleWorkflow is the core function at manager, it will execute Workflow defined logic, call runner.Run and return the final result
func (m *Manager) handleWorkflow(parameters map[string]interface{}, sp span.Span) (map[string]interface{}, error) {
	_, _ = m.getWorkflowByName(sp.WorkflowName)
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
