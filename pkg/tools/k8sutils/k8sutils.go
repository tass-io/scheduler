package k8sutils

import (
	"context"
	"errors"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"os"
	"strings"
)

var (
	selfName      string
	workflowName  string
	dynamicClient dynamic.Interface
	scheme        = runtime.NewScheme()
)
var WithInjectData = func(objects *[]runtime.Object) {

}

func Prepare() {
	if local := viper.GetBool(env.Local); local {
		workflowName = viper.GetString(env.WorkflowName)
		workflowRuntimeFilePath := viper.GetString(env.WorkflowRuntimeFilePath)
		selfName = viper.GetString(env.SelfName)
		workflowruntimes := generateWorkflowRuntimeObjectsByFile(workflowRuntimeFilePath)
		workflowFilePath := viper.GetString(env.WorkflowPath)
		workflows := generateWorkflowObjectsByFile(workflowFilePath)
		objects := append(workflowruntimes, workflows...)
		WithInjectData(&objects)
		zap.S().Infow("get objects", "objects", objects)
		if err := serverlessv1alpha1.AddToScheme(scheme); err != nil {
			panic(err)
		}
		dynamicClient = dynamicfake.NewSimpleDynamicClient(scheme, objects...)
	} else {
		// use real k8s
		hostName, _ := os.Hostname()
		sli := strings.Split(hostName, "-")
		if len(sli) < 3 {
			panic(sli)
		}
		selfName = strings.Join(sli[:2], "-")
		workflowName = strings.Join(sli[2:], "-")

		config, err := rest.InClusterConfig()
		if err != nil {
			panic(err)
		}
		dynamicClient = dynamic.NewForConfigOrDie(config)
	}
}

var GetSelfName = func() string {
	return selfName
}

var GetWorkflowName = func() string {
	return workflowName
}

var GetPatchClientIns = func() dynamic.Interface {
	return dynamicClient
}

// todo get namespace from file
var GetSelfNamespace = func() string {
	return "default"
}

func generateWorkflowObjectsByFile(fileName string) (result []runtime.Object) {
	f, err := os.Open(fileName)
	if err != nil {
		return []runtime.Object{}
	}
	defer f.Close()
	d := yaml.NewDecoder(f)

	for {
		// create new spec here
		spec := new(serverlessv1alpha1.Workflow)
		// pass a reference to spec reference
		err := d.Decode(&spec)
		// check it was parsed
		if spec == nil {
			continue
		}
		// break the loop in case of EOF
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			zap.S().Errorw("generate workflow from file error", "err", err)
		}
		result = append(result, spec)
	}
	return
}

func generateWorkflowRuntimeObjectsByFile(fileName string) (result []runtime.Object) {
	f, err := os.Open(fileName)
	if err != nil {
		return []runtime.Object{}
	}
	defer f.Close()
	d := yaml.NewDecoder(f)

	for {
		// create new spec here
		spec := new(serverlessv1alpha1.WorkflowRuntime)
		// pass a reference to spec reference
		err := d.Decode(&spec)
		// check it was parsed
		if spec == nil {
			continue
		}
		// break the loop in case of EOF
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			zap.S().Errorw("generate workflow runtime from file error", "err", err)
		}
		result = append(result, spec)
	}
	return
}

func CreateUnstructuredListWatch(ctx context.Context, namespace string, resource schema.GroupVersionResource) *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {

			return dynamicClient.Resource(resource).Namespace(namespace).List(ctx, opts)

		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			// Watch needs to be set to true separately
			opts.Watch = true
			return dynamicClient.Resource(resource).Namespace(namespace).Watch(ctx, opts)
		},
	}
}
