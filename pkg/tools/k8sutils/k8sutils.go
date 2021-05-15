package k8sutils

import (
	"bytes"
	"context"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
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
		objects := []runtime.Object{}
		workflowName = viper.GetString(env.WorkflowName)
		workflowRuntimeFilePath := viper.GetString(env.WorkflowRuntimeFilePath)
		selfName = viper.GetString(env.SelfName)
		err := generateWorkflowRuntimeObjectsByFile(workflowRuntimeFilePath, &objects)
		if err != nil {
			zap.S().Warnw("generate WorkflowRuntime error", "err", err)
		}
		workflowFilePath := viper.GetString(env.WorkflowPath)
		err = generateWorkflowObjectsByFile(workflowFilePath, &objects)
		if err != nil {
			zap.S().Warnw("generate Workflow error", "err", err)
		}
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

func generateWorkflowObjectsByFile(fileName string, objects *[]runtime.Object) error {
	filebytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(filebytes), 100)
	for {
		var rawObj runtime.RawExtension
		if err = decoder.Decode(&rawObj); err != nil {
			break
		}

		obj, _, err := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme).Decode(rawObj.Raw, nil, nil)
		if err != nil {
			return err
		}
		ust := obj.(*unstructured.Unstructured)
		workflow := new(serverlessv1alpha1.Workflow)
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), workflow)
		if err != nil {
			return err
		}
		zap.S().Debugw("get Workflow", "workflow", workflow)
		*objects = append(*objects, workflow)
	}
	return err
}

func generateWorkflowRuntimeObjectsByFile(fileName string, objects *[]runtime.Object) error {
	filebytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(filebytes), 100)
	for {
		var rawObj runtime.RawExtension
		if err = decoder.Decode(&rawObj); err != nil {
			break
		}

		obj, _, err := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme).Decode(rawObj.Raw, nil, nil)
		if err != nil {
			return err
		}
		ust := obj.(*unstructured.Unstructured)
		wfrt := new(serverlessv1alpha1.WorkflowRuntime)
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wfrt)
		if err != nil {
			return err
		}
		zap.S().Debugw("get WorkflowRuntime", "workflow", wfrt)
		*objects = append(*objects, wfrt)
	}
	return err
}

func CreateUnstructuredListWatch(ctx context.Context, namespace string, resource schema.GroupVersionResource) *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {

			result, err := dynamicClient.Resource(resource).Namespace(namespace).List(ctx, opts)
			zap.S().Debugw("get result at list and watch", "result", result)
			return result, err
		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			// Watch needs to be set to true separately
			opts.Watch = true
			return dynamicClient.Resource(resource).Namespace(namespace).Watch(ctx, opts)
		},
	}
}
