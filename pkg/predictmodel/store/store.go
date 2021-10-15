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
	Flow         string          `yaml:"flow"`           // flow name
	Fn           string          `yaml:"fn"`             // function name
	Coldstart    []time.Duration `yaml:"coldstart,flow"` // history of coldstart time
	Paths        []Path          `yaml:"paths,flow"`     // history of execution time from the specific path
	Parents      []string        `yaml:"parents,flow"`   // parent flows name
	Nexts        []string        `yaml:"nexts,flow"`     // downstream flows
	AvgColdStart time.Duration   `yaml:"avgColdStart"`   // average coldstart time
	AvgExec      time.Duration   `yaml:"avgExec"`        // average execution time
	Total        int64           `yaml:"total"`          // total count of all paths
	Probability  float64         `yaml:"-"`              // probability of the flow
}

type Path struct {
	From        string          `yaml:"from"`      // upstream flow name
	Count       int64           `yaml:"count"`     // available count
	Exec        []time.Duration `yaml:"exec,flow"` // history of execution time
	Probability float64         `yaml:"-"`         // probability of the flow for this specific execution path
}

func (s *Statistics) Merge(data map[string]*Object) {
	for flowName, obj := range data {
		if s.Flows[flowName] == nil {
			s.Flows[flowName] = obj
			continue
		}
		// update flow coldstart
		s.Flows[flowName].Coldstart = append(s.Flows[flowName].Coldstart, obj.Coldstart...)
		s.Flows[flowName].AvgColdStart = avg(s.Flows[flowName].Coldstart)
		// update flow execution time for each path
		for _, objPath := range obj.Paths {
			objExistInRaw := false
			for index, rawPath := range s.Flows[flowName].Paths {
				if rawPath.From == objPath.From {
					rawPath.Count += objPath.Count
					rawPath.Exec = append(rawPath.Exec, objPath.Exec...)
					s.Flows[flowName].Paths[index] = rawPath
					objExistInRaw = true
					break
				}
			}
			if !objExistInRaw {
				s.Flows[flowName].Paths = append(s.Flows[flowName].Paths, objPath)
			}
		}
		execSlice := make([]time.Duration, 0)
		for _, path := range s.Flows[flowName].Paths {
			execSlice = append(execSlice, path.Exec...)
		}
		s.Flows[flowName].AvgExec = avg(execSlice)
		// update flow total
		total := int64(0)
		for _, path := range s.Flows[flowName].Paths {
			total += path.Count
		}
		s.Flows[flowName].Total = total
	}
	s.Version++
}

func avg(sli []time.Duration) time.Duration {
	if len(sli) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range sli {
		sum += d
	}
	return sum / time.Duration(len(sli))
}
