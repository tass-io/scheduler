package store

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"

	"github.com/tass-io/tass-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
)

const (
	yamlSuffix = ".yaml"
)

// localstore stores statistics files in local file system
type localstore struct {
	path string
}

func NewLocalstore() Store {
	return &localstore{
		path: env.TassFileRoot + "model/",
	}
}

var _ Store = &localstore{}

func (s *localstore) GetStatistics(name string) (*Statistics, error) {
	var sts *Statistics
	// the stored model file path
	filename := s.path + name + yamlSuffix
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		err := os.MkdirAll(s.path, os.ModePerm)
		if err != nil {
			return nil, err
		}
		sts, err = s.newStatistics(name)
		if err != nil {
			return nil, err
		}
		err = s.marshalStatistics(filename, sts)
		if err != nil {
			return nil, err
		}
		return sts, nil
	}
	sts, err := s.unmarshalStatistics(filename)
	if err != nil {
		return nil, err
	}
	return sts, nil
}

func (s *localstore) newStatistics(name string) (*Statistics, error) {
	wf, exist, err := k8sutils.GetWorkflowByName(name)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, errors.New("workflow not found")
	}
	sts := genStsTemplate(wf)
	return sts, nil
}

func genStsTemplate(wf *v1alpha1.Workflow) *Statistics {
	sts := &Statistics{
		Version: 1,
		Flows:   make(map[string]*Object),
	}
	// recording flow parents, key is the flow name, value is the parent flows name
	flowParents := make(map[string][]string)
	for i, flow := range wf.Spec.Spec {
		if i == 0 {
			sts.Start = flow.Name
		}
		sts.Flows[flow.Name] = &Object{
			Flow: flow.Name,
			Fn: flow.Function,
		}
		var nexts []string
		nexts = append(nexts, flow.Outputs...)
		for _, condition := range flow.Conditions {
			nexts = append(nexts, condition.Destination.IsTrue.Flows...)
			nexts = append(nexts, condition.Destination.IsFalse.Flows...)
		}
		sts.Flows[flow.Name].Nexts = nexts
		// recording parents
		for _, next := range nexts {
			flowParents[next] = append(flowParents[next], flow.Name)
		}
	}
	for k, v := range flowParents {
		sts.Flows[k].Parents = v
	}
	return sts
}

func (s *localstore) unmarshalStatistics(filename string) (*Statistics, error) {
	sts := Statistics{}
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal([]byte(buf), &sts)
	if err != nil {
		return nil, err
	}
	return &sts, nil
}

func (s *localstore) MarshalStatistics(workflowName string, statistics *Statistics) error {
	filename := s.path + workflowName + yamlSuffix
	return s.marshalStatistics(filename, statistics)
}

// NOTE: marshalStatistics cannot prevent the concurrent write
func (s *localstore) marshalStatistics(filename string, sts *Statistics) error {
	data, err := yaml.Marshal(sts)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, data, 0)
	if err != nil {
		return err
	}
	return nil
}
