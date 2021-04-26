package k8sutils

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
	"strings"
)

var (
	selfName      string
	workflowName  string
	k8sclient     *kubernetes.Clientset
	dynamicClient dynamic.Interface
)

func init() {
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
	k8sclient, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	dynamicClient = dynamic.NewForConfigOrDie(config)
}

func GetSelfName() string {
	return selfName
}

func GetWorkflowName() string {
	return workflowName
}

func GetInformerClientIns() *kubernetes.Clientset {
	return k8sclient
}

func GetPatchClientIns() dynamic.Interface {
	return dynamicClient
}
