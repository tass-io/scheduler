package predictmodel

import (
	"fmt"
	"time"

	"github.com/tass-io/scheduler/pkg/predictmodel/store"
	"go.uber.org/zap"
)

type markov struct{}

func NewMarkovPolicy() Policy {
	return &markov{}
}

func (p *markov) GetModel(s *store.Statistics) *Model {
	sts := copyStatistics(s)
	setFlowsProbability(sts)
	return &Model{
		Version: sts.Version,
		Start:   sts.Start,
		Flows:   sts.Flows,
	}
}

func copyStatistics(s *store.Statistics) *store.Statistics {
	flows := make(map[string]*store.Object)
	for key, obj := range s.Flows {
		coldstart := []time.Duration{}
		coldstart = append(coldstart, obj.Coldstart...)
		paths := []store.Path{}
		paths = append(paths, obj.Paths...)
		parents := []string{}
		parents = append(parents, obj.Parents...)
		nexts := []string{}
		nexts = append(nexts, obj.Nexts...)
		flows[key] = &store.Object{
			Flow:           obj.Flow,
			Fn:             obj.Fn,
			Coldstart:      coldstart,
			Paths:          paths,
			Parents:        parents,
			Nexts:          nexts,
			AvgColdStart:   obj.AvgColdStart,
			AvgExec:        obj.AvgExec,
			TotalExec:      obj.TotalExec,
			TotalColdStart: obj.TotalColdStart,
			Probability:    obj.Probability,
		}
	}
	return &store.Statistics{
		Version: s.Version,
		Start:   s.Start,
		Flows:   flows,
	}
}

func setFlowsProbability(s *store.Statistics) {
	s.Flows[s.Start].Probability = 1
	unfinished := getUnfinishedFlows(s)
	queue := []string{}
	queue = append(queue, s.Flows[s.Start].Nexts...)
	for len(queue) > 0 {
		flowName := queue[0]
		newQueue := make([]string, 0)
		newQueue = append(newQueue, queue[1:]...)
		queue = newQueue
		obj := s.Flows[flowName]
		// case 0: redundant calculation, drop it directly
		if _, ok := unfinished[flowName]; !ok {
			continue
		}
		// case 1: flow has no request, drop it and its nexts
		if obj.TotalExec == 0 {
			continue
		}
		// case 2: normal case, try calculating the probability
		shouldRequeue := false
		for index, path := range obj.Paths {
			if path.Count == 0 {
				continue
			}
			// unexpected error case
			if _, ok := s.Flows[path.From]; !ok {
				zap.S().Error("generate markov probability model failed",
					"err", fmt.Sprintf("flow %s's path from %s not found", flowName, path.From))
				continue
			}
			// case 2.1: if upstream flow probility is 0, drop it
			if s.Flows[path.From].Probability == 0 && s.Flows[path.From].TotalExec == 0 {
				continue
			}
			// case 2.2: if upstream flow don't finish calculation, calculate later and put upstream flow into queue
			if s.Flows[path.From].Probability == 0 && s.Flows[path.From].TotalExec != 0 {
				queue = append(queue, path.From)
				shouldRequeue = true
				continue
			}
			// case 2.3: if upstream flow have finished calculation, calculate the probability
			// probability = upstream_probability * (path_count / upstream_total)
			path.Probability = (float64(path.Count) / float64(s.Flows[path.From].TotalExec)) * s.Flows[path.From].Probability
			obj.Paths[index] = path
		}
		// requeue the current flow to wait for its upstream flow to finish calculation
		if shouldRequeue {
			queue = append(queue, flowName)
			continue
		}
		// all paths have finished calculation, calculate the probability
		obj.Probability = 0 // reset the probability
		for _, path := range obj.Paths {
			obj.Probability += path.Probability
		}
		// append nexts to the queue
		queue = append(queue, obj.Nexts...)
		delete(unfinished, flowName)
	}
}

func getUnfinishedFlows(s *store.Statistics) map[string]bool {
	unfinished := make(map[string]bool)
	for flowName := range s.Flows {
		unfinished[flowName] = true
	}
	delete(unfinished, s.Start)
	return unfinished
}
