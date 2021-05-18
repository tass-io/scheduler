package main

import (
	"fmt"
	"os"
	"os/user"

	"github.com/tass-io/scheduler/pkg/runner/instance"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Wrapper will handle all lifecycle
// here the Consumer and Producer role exchanged.
type Wrapper struct {
	consumer *instance.Consumer
	producer *instance.Producer
}

func NewWrapper() *Wrapper {
	requestFile := os.NewFile(uintptr(3), "pipe")  // 3 is the fd of request channel
	producerFile := os.NewFile(uintptr(4), "pipe") // 4 is the fd of response channel
	wrapper := &Wrapper{
		consumer: instance.NewConsumer(requestFile, &instance.FunctionRequest{}),
		producer: instance.NewProducer(producerFile, &instance.FunctionResponse{}),
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
	request.Parameters["motto"] = "Veni Vidi Vici"
	return instance.FunctionResponse{
		Id:     request.Id,
		Result: request.Parameters,
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
		fmt.Printf("[isRoot] Unable to get current user: %s", err)
	}
	fmt.Println(currentUser.Name)
	w := NewWrapper()
	w.Start()
}
