package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"plugin"

	"github.com/tass-io/scheduler/pkg/runner/instance"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Wrapper will handle all lifecycle
// here the Consumer and Producer role exchanged.
type Wrapper struct {
	consumer *instance.Consumer
	producer *instance.Producer
	handler  func(map[string]interface{}) (map[string]interface{}, error)
}

func loadPlugin(codePath string, entrypoint string) (func(map[string]interface{}) (map[string]interface{}, error), error) {
	info, err := os.Stat(codePath)
	if err != nil {
		return nil, fmt.Errorf("error checking plugin path: %v", err)
	}
	if info.IsDir() {
		files, err := ioutil.ReadDir(codePath)
		if err != nil {
			return nil, fmt.Errorf("error reading directory: %v", err)
		}
		if len(files) == 0 {
			return nil, fmt.Errorf("no files to load: %v", codePath)
		}
		fi := files[0]
		codePath = filepath.Join(codePath, fi.Name())
	}

	log.Printf("loading plugin from %v", codePath)
	p, err := plugin.Open(codePath)
	if err != nil {
		return nil, fmt.Errorf("error loading plugin: %v", err)
	}
	sym, err := p.Lookup(entrypoint)
	if err != nil {
		return nil, fmt.Errorf("entry point not found: %v", err)
	}

	return sym.(func(map[string]interface{}) (map[string]interface{}, error)), nil

}

func NewWrapper() *Wrapper {
	requestFile := os.NewFile(uintptr(3), "pipe")  // 3 is the fd of request channel
	producerFile := os.NewFile(uintptr(4), "pipe") // 4 is the fd of response channel
	handler, err := loadPlugin(os.Args[1], "Handler")
	if err != nil {
		zap.S().Warnw("user code puglin load error", "err", err)
	}
	wrapper := &Wrapper{
		consumer: instance.NewConsumer(requestFile, &instance.FunctionRequest{}),
		producer: instance.NewProducer(producerFile, &instance.FunctionResponse{}),
		handler:  handler,
	}
	return wrapper
}

func (w *Wrapper) Start() {
	w.consumer.Start()
	w.producer.Start()
	for reqRaw := range w.consumer.GetChannel() {
		req := reqRaw.(*instance.FunctionRequest)
		zap.S().Debugw("get req", "req", req)
		w.producer.GetChannel() <- w.invoke(*req)
	}
}

// todo implementation by go plugin
func (w *Wrapper) invoke(request instance.FunctionRequest) instance.FunctionResponse {
	if w.handler == nil {
		request.Parameters["motto"] = "Veni Vidi Vici"
		return instance.FunctionResponse{
			Id:     request.Id,
			Result: request.Parameters,
		}
	} else {
		resp, err := w.handler(request.Parameters)
		if err != nil {
			return instance.FunctionResponse{
				Id:     request.Id,
				Result: map[string]interface{}{"err": err.Error()},
			}
		} else {
			return instance.FunctionResponse{
				Id:     request.Id,
				Result: resp,
			}
		}
	}

}

func init() {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "message",

			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,

			TimeKey:    "time",
			EncodeTime: zapcore.ISO8601TimeEncoder,

			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	logger, err := cfg.Build()
	if err != nil {
		fmt.Println(err.Error())
	}
	zap.ReplaceGlobals(logger)
}

func main() {
	currentUser, err := user.Current()
	if err != nil {
		zap.S().Warnw("unable to get current user: %s", err)
	}
	zap.S().Infow("run the binary user", "user", currentUser.Name)
	w := NewWrapper()
	w.Start()
}
