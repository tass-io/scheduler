package store

import "time"

type Store interface {
	// GetStatistics returns the workflow predition model entry item
	GetStatistics(workflowName string) (*Statistics, error)
	// MarshalStatistics stores the workflow model
	MarshalStatistics(workflowName string, statistics *Statistics) error
	AppendColdstartAndExecHistory(workflowName string, statistics *Statistics) error
}

type Statistics struct {
	Version int64              `yaml:"version"`
	Start   string             `yaml:"start"`
	Flows   map[string]*Object `yaml:"flows,omitempty"`
}

type Object struct {
	Flow           string          `yaml:"flow"`           // flow name
	Fn             string          `yaml:"fn"`             // function name
	Paths          []Path          `yaml:"paths,flow"`     // history of execution time from the specific path
	Parents        []string        `yaml:"parents,flow"`   // parent flows name
	Nexts          []string        `yaml:"nexts,flow"`     // downstream flows
	AvgColdStart   time.Duration   `yaml:"avgColdStart"`   // average coldstart time
	AvgExec        time.Duration   `yaml:"avgExec"`        // average execution time
	TotalExec      int64           `yaml:"totalExec"`      // total count of all paths of exec
	TotalColdStart int64           `yaml:"totalColdStart"` // total count of coldstart
	Probability    float64         `yaml:"-"`              // probability of the flow
	Coldstart      []time.Duration `yaml:"-"`              // temp field to store recorded coldstart time
}

type Path struct {
	From        string          `yaml:"from"`  // upstream flow name
	Count       int64           `yaml:"count"` // available count
	Probability float64         `yaml:"-"`     // probability of the flow for this specific execution path
	Exec        []time.Duration `yaml:"-"`     // temp field to store recorded execution time
}

func (s *Statistics) Merge(data map[string]*Object) {
	for flowName, obj := range data {
		if s.Flows[flowName] == nil {
			s.Flows[flowName] = obj
			continue
		}
		// update flow coldstart
		s.Flows[flowName].Coldstart = obj.Coldstart
		s.Flows[flowName].AvgColdStart =
			avg(s.Flows[flowName].AvgColdStart, s.Flows[flowName].TotalColdStart, s.Flows[flowName].Coldstart)
		// update flow execution time for each path
		for _, objPath := range obj.Paths {
			objExistInRaw := false
			for index, rawPath := range s.Flows[flowName].Paths {
				if rawPath.From == objPath.From {
					rawPath.Exec = objPath.Exec
					rawPath.Count += objPath.Count
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
		s.Flows[flowName].AvgExec = avg(s.Flows[flowName].AvgExec, s.Flows[flowName].TotalExec, execSlice)
		// update flow total
		total := int64(0)
		for _, path := range s.Flows[flowName].Paths {
			total += path.Count
		}
		s.Flows[flowName].TotalExec = total
	}
	s.Version++
}

func avg(oldAvg time.Duration, count int64, sli []time.Duration) time.Duration {
	if len(sli) == 0 {
		return oldAvg
	}
	var sum time.Duration
	for _, d := range sli {
		sum += d
	}
	return (sum + oldAvg*time.Duration(count)) / time.Duration(int64(len(sli))+count)
}
