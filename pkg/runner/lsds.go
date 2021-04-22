package runner

import (
	"github.com/tass-io/scheduler/pkg/span"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type LSDS struct {
	Runner
	client dynamic.Interface
}

// client is a parameter because we will use mockclient to test
func NewLSDS() *LSDS {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	return &LSDS{
		client: dynamicClient,
	}
}

func (l *LSDS) chooseTarget(functionName string) {

}

// LDS Start to watch other Local Scheduler Info
func (l *LSDS) Start() {

}

// find a suitable pod to send http request with
func (l *LSDS) Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error) {
	return
}
