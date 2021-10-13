package predictmodel

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/predictmodel/store"
	"go.uber.org/zap"
)

var p *Manager

// Manager is the prediction model manager, which is responsible for offering prediction model result and
// updating the statistics.
type Manager struct {
	mu    sync.Locker
	store store.Store // storage for prediction model
	wf    string      // workflow name
	sts   *store.Statistics
}

func Init(workflowName string) error {
	if p == nil {
		err := newPredictionModel(workflowName)
		if err != nil {
			return err
		}
	}
	if p.wf != workflowName {
		zap.S().Info("workflow changes, generate a new workflow prediction manager", "old", p.wf, "new", workflowName)
		err := newPredictionModel(workflowName)
		if err != nil {
			return err
		}
	}
	return nil
}

func newPredictionModel(workflowName string) error {
	p = &Manager{
		wf:    workflowName,
		mu:    &sync.Mutex{},
		store: store.NewLocalstore(),
	}
	sts, err := p.store.GetStatistics(p.wf)
	if err != nil {
		zap.S().Error("get workflow statistics error", "err", err, "workflow", workflowName)
		return err
	}
	p.sts = sts
	return nil
}

func GetPredictModelManager() *Manager {
	return p
}

func (pm *Manager) PatchRecords(records map[string]*store.Object) error {
	if len(records) == 0 {
		return nil
	}
	sts, err := pm.store.GetStatistics(pm.wf)
	if err != nil {
		return err
	}
	sts.Merge(records)
	err = pm.store.MarshalStatistics(pm.wf, sts)
	if err != nil {
		return err
	}
	pm.updateStatistics(sts)
	return nil
}

func (pm *Manager) updateStatistics(sts *store.Statistics) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.sts = sts
}
