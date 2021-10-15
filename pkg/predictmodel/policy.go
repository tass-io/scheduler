package predictmodel

import (
	"github.com/tass-io/scheduler/pkg/predictmodel/store"
)

type Policy interface {
	GetModel(*store.Statistics) *Model
}

type Model struct {
	Version int64
	Start   string
	Flows   map[string]*store.Object `yaml:"flows,omitempty"`
}
