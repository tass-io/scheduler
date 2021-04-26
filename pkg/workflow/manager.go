package workflow

import (
	"time"

	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
)

func init() {
	manager = NewManager()
}

var (
	WorkflowResource = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "Workflow",
	}
	manager *Manager
)

type Manager struct {
	runner   runner.Runner
	stopCh   chan struct{}
	informer informers.GenericInformer
}

func GetManagerIns() *Manager {
	return manager
}

// NewManager will use path to init workflow from file
func NewManager() *Manager {
	m := &Manager{
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

func (m *Manager) getWorkflowByName(name string) (*serverlessv1alpha1.Workflow, error) {
	obj, err := m.informer.Lister().Get(name)
	if err != nil {
		return nil, err
	}
	wf, ok := obj.(*serverlessv1alpha1.Workflow)
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
