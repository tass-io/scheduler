package instance

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/tass-io/scheduler/pkg/utils/common"
	"go.uber.org/zap"
)

// The data stream of the instance model:
//
//           |   +-----------------------------+    ^
//           |   |       Instance Model        |    |
//           |   +-----+-----------------------+    |
//           |         |                  ^         |
//           |         |                  |         |
//           |         v                  |         |
//           |   +----------+       +-----+----+    |
//           |   | producer |       | consumer |    |
//   request |   +----------+       +----------+    | response
//           |         |                  ^         |
//           |         |                  |         |
//           |         |            bytes |         |
//           |         |                  |         |
//           |         |            +-----+----+    |
//           |         +----------->| process  |    |
//           v              bytes   +----------+    |
//

var (
	// splitByte is just for functionRequest package delimiter.
	// For example, `{"a": "b"}littledrizzle{"a": "c"}` maybe in the pipe,
	// so to split two request, we need a delimiter.
	splitByte = []byte("littledrizzle")

	// ping is a information to show the producer is ready to send requests,
	// indicating the process is ready
	ping = []byte("ping")
)

// FunctionRequest will be put into the producer and send it to request pipe
type FunctionRequest struct {
	ID         string                 `json:"id"`
	Parameters map[string]interface{} `json:"parameters"`
}

// NewFunctionRequest returns a new function request with unique id and parameters
func NewFunctionRequest(id string, parameters map[string]interface{}) *FunctionRequest {
	transfer, err := common.CopyMap(parameters)
	if err != nil {
		zap.S().Errorw("functionRequest marshal error", "err", err)
		return nil
	}
	return &FunctionRequest{
		ID:         id,
		Parameters: transfer,
	}
}

// FunctionResponse will be put into channel to be consume by instance.
type FunctionResponse struct {
	ID     string
	Result map[string]interface{}
}

// Producer waits for requests from request channel and put the data into the process
type Producer struct {
	f                  *os.File
	data               interface{}
	requestChannel     chan interface{}
	startRoutineExited bool
}

// Consumer waits for responses and put the data into response channel
type Consumer struct {
	f               *os.File
	data            interface{}
	responseChannel chan interface{}
	initDoneChannel chan struct{}
	noNewInfo       bool
}

// NewConsumer creates a new consumer data structure cantains a channel and an unnamed pipe
func NewConsumer(f *os.File, data interface{}) *Consumer {
	return &Consumer{
		f:               f,
		data:            data,
		responseChannel: make(chan interface{}, 10),
		initDoneChannel: make(chan struct{}, 1),
	}
}

// consumer Start gets the function response and sends it to the channel
func (c *Consumer) Start() {
	go func() {
		typ := reflect.TypeOf(c.data)
		zap.S().Debugw("consumer get type", "type", typ)
		data := make([]byte, 4<<20)
		reader := bufio.NewReader(c.f)
		tail := ""
		processInitDone := false
		for {
			n, err := reader.Read(data)
			if n == 0 {
				zap.S().Debug("read nothing")
				if err == nil {
					continue
				} else if err == io.EOF {
					zap.S().Info("consumer read EOF")
					break
				}
			}

			rawString := string(data[:n])
			rawString = tail + rawString
			responseSlice := strings.Split(rawString, string(splitByte))

			if !processInitDone {
				// this is sent when the producer starts at the first time
				if responseSlice[0] == string(ping) {
					zap.S().Info("receive a 'ping' signal")
					processInitDone = true
					c.initDoneChannel <- struct{}{}
					responseSlice = responseSlice[1:]
				} else {
					zap.S().Panic("Should receive ping from pipe but received other info")
				}
			}

			for _, item := range responseSlice {
				if item == "" {
					continue
				}
				resp := reflect.New(typ.Elem())
				newP := resp.Interface()
				err = json.Unmarshal([]byte(item), newP)
				if err != nil {
					zap.S().Infow("consumer unmarshal error", "err", err, "item", item)
					// take care of `{"s":"b"}littledrizzle{"n":`
					// the item must be the last one
					tail = item
					continue
				}
				zap.S().Debugw("get resp with in consumer ", "resp", newP)
				c.responseChannel <- newP
			}
		}

		zap.S().Debug("no more requests")
		c.noNewInfo = true
		c.f.Close()
	}()
}

// GetChannel returns a consumer response channel
func (c *Consumer) GetChannel() chan interface{} {
	return c.responseChannel
}

// GetInitDoneChannel returns a consumer initDone channel,
// which notifies the channel when the process producer pipe sends a "ping" signal
func (c *Consumer) GetInitDoneChannel() chan struct{} {
	return c.initDoneChannel
}

// NoNewInfo returns wether the pipe have data waiting for dealing with in the response channel
func (c *Consumer) NoNewInfo() bool {
	return c.noNewInfo
}

// Terminate closes the fd of the consumer response pipe
// Consumer do nothing about channel when Terminate
// because the resp will still be gotten after kill desicion
func (c *Consumer) Terminate() {
	c.f.Close()
}

// NewProducer creates a new consumer data structure cantains a channel and an unnamed pipe
func NewProducer(f *os.File, data interface{}) *Producer {
	return &Producer{
		f:                  f,
		data:               data,
		requestChannel:     make(chan interface{}, 10),
		startRoutineExited: false,
	}
}

// producer listens the channel and gets a request, writes the data to pipe
func (p *Producer) Start() {
	var signal []byte
	signal = append(signal, ping...)
	signal = append(signal, splitByte...)

	n, err := p.f.Write(signal)
	if err != nil || n != len(signal) {
		zap.S().Panicw("instance producer starts error", "err", err, "reqByteLen", len(signal), "n", n)
	}

	go func() {
		for req := range p.requestChannel {
			reqByte, err := json.Marshal(&req)
			zap.S().Debugw("producer get object", "object", string(reqByte))
			if err != nil {
				zap.S().Errorw("producer marshal error", "err", err)
			}
			// add delimiter
			reqByte = append(reqByte, splitByte...)
			n, err := p.f.Write(reqByte)
			if err != nil || n != len(reqByte) {
				zap.S().Errorw("instance request error", "err", err, "reqByteLen", len(reqByte), "n", n)
			}
		}
		zap.S().Debug("producer close")
		p.startRoutineExited = true
		p.f.Close()
	}()
}

// Terminate closes producer request channel
func (p *Producer) Terminate() {
	close(p.requestChannel)
}

// GetChannel returns request producer channel
func (p *Producer) GetChannel() chan interface{} {
	return p.requestChannel
}

// NoNewInfo returns wether the producer channel is closed or not
func (p *Producer) NoNewInfo() bool {
	return p.startRoutineExited
}
