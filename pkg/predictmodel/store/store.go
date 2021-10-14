package store

import "time"

type Store interface {
	// GetStatistics returns the workflow predition model entry item
	GetStatistics(workflowName string) (*Statistics, error)
	// MarshalStatistics stores the workflow model
	MarshalStatistics(workflowName string, statistics *Statistics) error
}

type Statistics struct {
	Version int64              `yaml:"version"`
	Start   string             `yaml:"start"`
	Flows   map[string]*Object `yaml:"flows,omitempty"`
}

type Object struct {
	Flow      string          `yaml:"flow"`           // flow name
	Fn        string          `yaml:"fn"`             // function name
	Parents   []string        `yaml:"parents,flow"`   // parent flow name
	Coldstart []time.Duration `yaml:"coldstart,flow"` // history of coldstart time
	Exec      []time.Duration `yaml:"exec,flow"`      // history of execution time
	Nexts     []string        `yaml:"nexts,flow"`     // downstream flows
}

func (s *Statistics) Merge(data map[string]*Object) {
	for flowName, obj := range data {
		if s.Flows[flowName] == nil {
			s.Flows[flowName] = obj
			continue
		}
		s.Flows[flowName].Coldstart = append(s.Flows[flowName].Coldstart, obj.Coldstart...)
		s.Flows[flowName].Exec = append(s.Flows[flowName].Exec, obj.Exec...)
	}
	s.Version++
}
