package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"plugin"
	"sync"
	"syscall"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	sigtermChan      = make(chan os.Signal, 1)
	w                = NewWrapper()
	closeChannelOnce = &sync.Once{}
)

// Wrapper handles all lifecycle of a function
// here the Consumer and Producer role exchanged.
type Wrapper struct {
	requestMap      cmap.ConcurrentMap // Now use a counter is also ok, but I think it is more convenient to debug.
	consumer        *instance.Consumer
	producer        *instance.Producer
	handler         handlerFn
	receiveShutdown bool
}

// handlerFn is the user function signature
// all user defined functions should be declared as this type
type handlerFn func(map[string]interface{}) (map[string]interface{}, error)

// loadPlugin takes the codePath, loads the plugin code and returns the handler function
func loadPlugin(codePath string, entrypoint string) (handlerFn, error) {
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

// NewWrapper creates a new wrapper for a process
func NewWrapper() *Wrapper {
	// 3 is the fd of request channel
	requestFile := os.NewFile(uintptr(3), "pipe")
	// 4 is the fd of response channel
	producerFile := os.NewFile(uintptr(4), "pipe")
	// the instruction that local scheduler runs the runtime is: main ${PLUGIN_PATH}
	// so the value of os.Args[1] is the location of plugin.so
	handler, err := loadPlugin(os.Args[1], "Handler")
	if err != nil {
		zap.S().Warnw("user code puglin load error", "err", err)
	}
	m := cmap.New()
	wrapper := &Wrapper{
		requestMap:      m,
		consumer:        instance.NewConsumer(requestFile, &instance.FunctionRequest{}),
		producer:        instance.NewProducer(producerFile, &instance.FunctionResponse{}),
		handler:         handler,
		receiveShutdown: false,
	}
	return wrapper
}

// Start starts the wrapped process waiting for requests
func (w *Wrapper) Start() {
	w.consumer.Start()
	w.producer.Start()
	reqChan := w.consumer.GetChannel()

	for reqRaw := range reqChan {
		req := reqRaw.(*instance.FunctionRequest)
		zap.S().Debugw("get req", "req", req)
		// do the invocation
		go func() {
			w.requestMap.Set(req.ID, "")
			result := w.invoke(*req)
			w.producer.GetChannel() <- result
			w.requestMap.Remove(req.ID)
		}()
	}
}

// invoke invokes the requests and returns the response
// todo implementation by go plugin
func (w *Wrapper) invoke(request instance.FunctionRequest) instance.FunctionResponse {
	if w.handler == nil {
		request.Parameters["motto"] = "Veni Vidi Vici"
		return instance.FunctionResponse{
			ID:     request.ID,
			Result: request.Parameters,
		}
	}
	resp, err := w.handler(request.Parameters)
	if err != nil {
		return instance.FunctionResponse{
			ID:     request.ID,
			Result: map[string]interface{}{"err": err.Error()},
		}
	}
	return instance.FunctionResponse{
		ID:     request.ID,
		Result: resp,
	}
}

// Shutdown sets Warpper `receiveShutdown` field as true
func (w *Wrapper) Shutdown() {
	w.receiveShutdown = true
}

// init initializes the process of the golang runtime
func init() {
	// log config
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
	// let the sigtermChan receive SIGTERM signal
	signal.Notify(sigtermChan, syscall.SIGTERM)
	go func() {
		<-sigtermChan
		w.Shutdown()
		w.handleTerminate()
	}()
}

// handleTerminate checks the process status, and terminates when it is idle
// w.receiveShutdown shows whether the process receives SIGTERM.
// w.consumer.NoNewInfo and w.producer.NoNewInfo check whether the IPC is empty.
// w.requestMap.IsEmpty() checks whether the process is handling a request.
func (w *Wrapper) handleTerminate() {
	for {
		if w.receiveShutdown && w.consumer.NoNewInfo() {
			zap.S().Debug("more requests")
			if w.requestMap.IsEmpty() {
				zap.S().Debug("all requests have been handled and put responses into channel")
				closeChannelOnce.Do(func() {
					w.producer.Terminate()
				})
				if w.producer.NoNewInfo() {
					zap.S().Info("function shutdown after no requests and all responses have been sent")
					os.Exit(0)
				}
			}
		}
	}
}

func main() {
	currentUser, err := user.Current()
	if err != nil {
		zap.S().Warnw("unable to get current user: %s", err)
	}
	zap.S().Infow("Hi", "user", currentUser.Name)
	zap.S().Infow("run the binary user", "user", currentUser.Name)
	w.Start()
}
