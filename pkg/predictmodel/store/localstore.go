package store

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"

	"github.com/tass-io/tass-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
)

const (
	yamlSuffix       = ".yaml"
	modelDir         = env.TassFileRoot + "model/"
	dataDir          = env.TassFileRoot + "data/"
	coldstartDataDir = dataDir + "%s/coldstart/"
	execDataDir      = dataDir + "%s/exec/"
)

// localstore stores statistics files in local file system
type localstore struct{}

func NewLocalstore() Store {
	return &localstore{}
}

var _ Store = &localstore{}

func (s *localstore) GetStatistics(name string) (*Statistics, error) {
	var sts *Statistics
	// the stored model file path
	filename := modelDir + name + yamlSuffix
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		err := os.MkdirAll(modelDir, os.ModePerm)
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
	if err := s.genStsDependencies(name, sts); err != nil {
		return nil, err
	}
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
			Fn:   flow.Function,
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

func (s *localstore) genStsDependencies(wfName string, sts *Statistics) error {
	err := mkdir(fmt.Sprintf(coldstartDataDir, wfName), fmt.Sprintf(execDataDir, wfName))
	if err != nil {
		return err
	}
	for _, obj := range sts.Flows {
		// handle workflow start
		if obj.Flow == sts.Start {
			mkfileForColdstartAndExec(wfName, sts.Start, "")
			continue
		}
		mkfileForColdstartAndExec(wfName, obj.Flow, obj.Parents...)
	}
	return nil
}

func mkdir(paths ...string) error {
	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			err := os.MkdirAll(path, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func mkfileForColdstartAndExec(wfName, flow string, parents ...string) error {
	if err := mkfile(fmt.Sprint(coldstartDataDir, wfName), flow); err != nil {
		return err
	}
	for _, parent := range parents {
		if err := mkfile(fmt.Sprintf(execDataDir, wfName), parent+"=>"+flow); err != nil {
			return err
		}
	}
	return nil
}

func mkfile(base, filename string) error {
	if _, err := os.Stat(base + filename); os.IsNotExist(err) {
		f, err := os.Create(base + filename)
		if err != nil {
			return err
		}
		defer f.Close()
	}
	return nil
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
	filename := modelDir + workflowName + yamlSuffix
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

func (s *localstore) AppendColdstartAndExecHistory(workflowName string, statistics *Statistics) error {
	err := s.appendColdstart(workflowName, statistics)
	if err != nil {
		return err
	}
	err = s.appendExec(workflowName, statistics)
	if err != nil {
		return err
	}
	return nil
}

func (s *localstore) appendColdstart(wfName string, statistics *Statistics) error {
	for _, obj := range statistics.Flows {
		if len(obj.Coldstart) == 0 {
			continue
		}
		f, err := os.OpenFile(fmt.Sprintf(coldstartDataDir, wfName)+obj.Flow, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		var data string
		for _, val := range obj.Coldstart {
			data += fmt.Sprintf("%s,", val.String())
		}
		if _, err = f.WriteString(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *localstore) appendExec(wfName string, statistics *Statistics) error {
	for _, obj := range statistics.Flows {
		for _, path := range obj.Paths {
			if len(path.Exec) == 0 {
				continue
			}
			fileName := path.From + "=>" + obj.Flow
			f, err := os.OpenFile(fmt.Sprintf(execDataDir, wfName)+fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			defer f.Close()
			var data string
			for _, val := range path.Exec {
				data += fmt.Sprintf("%s,", val.String())
			}
			if _, err = f.WriteString(data); err != nil {
				return err
			}
		}
	}
	return nil
}
