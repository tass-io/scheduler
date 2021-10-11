package predictmodel

import (
	"context"

	"go.uber.org/zap"
)

var p *PredictModel

type PredictModel struct {
	ctx    context.Context
	cancel context.CancelFunc
	store  Store              // storage for prediction model
	wf     string             // workflow name
	items  map[string]*Object // flows, the key is flow name
	start  *Object
}

func Init(workflowName string) {
	if p == nil {
		newPredictionModel(workflowName)
		return
	}
	if p.wf != workflowName {
		p.cancel()
		zap.S().Info("workflow changes, generate a new workflow prediction manager")
		newPredictionModel(workflowName)
	}
}

func newPredictionModel(workflowName string) *PredictModel {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	return &PredictModel{
		ctx:    ctx,
		cancel: cancel,
		wf:     workflowName,
		items:  make(map[string]*Object),
	}
}

func (pm *PredictModel) Patch(obj Object) {
	pm.store.PatchModel(pm.wf, obj)
}
