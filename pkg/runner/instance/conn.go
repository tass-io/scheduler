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
	splitByte = []byte("Z")

	// ping is a information to show the producer is ready to send requests,
	// indicating the process is ready
	ping = []byte("ping")
)

const bufSize = uint64(2 << 28)
const rMask = (bufSize - 1)

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
		data := make([]byte, 4096+1024)
		reader := bufio.NewReader(c.f)
		tail := make([]byte, bufSize)
		buffStart, buffEnd := uint64(0), uint64(0)
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

			for _, b := range data[:n] {
				tail[buffEnd&rMask] = b
				buffEnd++
				if buffEnd&rMask == buffStart&rMask {
					zap.S().Panic("Consumer buffer: `tail` all used up")
				}
			}

			// tail = append(tail, data[:n]...)

			if !processInitDone {
				// this is sent when the producer starts at the first time
				responseSlice := strings.Split(string(tail[buffStart&rMask:buffEnd&rMask]), string(splitByte))
				zap.S().Info("receive a 'ping' signal", "", responseSlice)
				if responseSlice[0] == string(ping) {
					zap.S().Info("receive a 'ping' signal")
					processInitDone = true
					c.initDoneChannel <- struct{}{}
					buffStart += uint64(len(ping)) + uint64(len(splitByte))
				} else {
					zap.S().Panic("Should receive ping from pipe but received other info")
				}
			}

			for i := buffStart; i < buffEnd; i++ {
				if tail[i&rMask] != 'Z' {
					continue
				}
				// tail[i] == 'Z'
				resp := reflect.New(typ.Elem())
				newP := resp.Interface()
				if buffStart&rMask < i&rMask {
					err = json.Unmarshal(tail[buffStart&rMask:i&rMask], newP)
				} else {
					tmp := make([]byte, bufSize-buffStart&rMask+i&rMask)
					for j, b := range tail[buffStart&rMask:] {
						tmp[j] = b
					}
					for j, b := range tail[:i&rMask] {
						tmp[bufSize-buffStart&rMask+uint64(j)] = b
					}
					err = json.Unmarshal(tmp, newP)
				}
				if err != nil {
					zap.S().Errorw("consumer unmarshal error", "err", err)
				}
				zap.S().Debugw("get resp with in consumer ")
				c.responseChannel <- newP
				buffStart = i + 1
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
			// zap.S().Debugw("producer get object", "object", string(reqByte))
			if err != nil {
				zap.S().Errorw("producer marshal error", "err", err)
			}
			// add delimiter
			n, err := p.f.Write(reqByte)
			if err != nil || n != len(reqByte) {
				zap.S().Errorw("instance request error", "err", err, "reqByteLen", len(reqByte), "n", n)
			}
			n, err = p.f.Write(splitByte)
			if err != nil || n != 1 {
				zap.S().Errorw("instance request error", "err", err, "reqByteLen", 1, "n", n)
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
