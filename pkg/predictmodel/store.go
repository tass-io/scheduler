package predictmodel

import "github.com/tass-io/scheduler/pkg/env"

type Store interface {
	// GetModel returns the workflow predition model entry item
	GetModel(name string) (*Model, error)
	// PatchModel updates the workflow observation data
	PatchModel(name string, data Object) error
}

type Model struct {
	Version int64              `yaml:"version"`
	start   string             `yaml:"start"`
	flows   map[string]*Object `yaml:"flows,omitempty"`
}

type Object struct {
	flow      string  // flow name
	fn        string  // function name
	coldstart []int64 // history of coldstart time
	exec      []int64 // history of execution time
	nexts     []*Object
}

type localstore struct {
	path string
}

func newLocalstore() *localstore {
	return &localstore{
		path: env.TassFileRoot + "model/",
	}
}

// func (s *localstore) GetModel(name string) (*Model, error) {
// 	modelPath := s.path + name
// }
