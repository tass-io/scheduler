package instance

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"reflect"

	// "github.com/tass-io/scheduler/pkg/utils/common"
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
	// transfer, err := common.CopyMap(parameters)
	// if err != nil {
	// 	zap.S().Errorw("functionRequest marshal error", "err", err)
	// 	return nil
	// }
	return &FunctionRequest{
		ID:         id,
		Parameters: parameters,
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
		var data []byte
		reader := bufio.NewReader(c.f)
		processInitDone := false
		for {
			// 1. read the upstream contents length first
			data = make([]byte, 8) // 8 bytes for int64 length
			n, err := io.ReadFull(reader, data)
			if n == 0 {
				zap.S().Debug("read nothing")
				if err == nil {
					continue
				} else if err == io.EOF {
					zap.S().Info("consumer read EOF")
					break
				}
			}
			// 2. read the upstream contents with the fixed sieze
			data = make([]byte, bytesToInt64(data))
			n, err = io.ReadFull(reader, data)
			if n == 0 || err == io.EOF {
				zap.S().Panic("consumer should get contents but gets nothing")
			}

			// 2.1 check wether the process init done
			if !processInitDone {
				if string(data) == string(ping) {
					zap.S().Debug("receive a 'ping' signal and init done")
					processInitDone = true
					c.initDoneChannel <- struct{}{}
					continue
				} else {
					zap.S().Panic("Should receive ping from pipe but received other info", "got", string(data))
				}
			}

			// 2.2 normal case: read contents from pipe and unmatshal
			response := reflect.New(typ.Elem()).Interface() // cannot decalre a interface{} directly
			err = json.Unmarshal(data, response)
			if err != nil {
				zap.S().Panic("consumer unmarshal error", "err", err)
			}
			c.responseChannel <- response
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
	n, err := p.f.Write(int64ToBytes(int64(4)))
	if err != nil || n != 8 {
		zap.S().Panicw("instance producer starts error", "err", err, "reqByteLen", len(ping), "n", n)
	}
	n, err = p.f.Write(ping)
	if err != nil || n != len(ping) {
		zap.S().Panicw("instance producer starts error", "err", err, "reqByteLen", len(ping), "n", n)
	}

	go func() {
		for req := range p.requestChannel {
			reqByte, err := json.Marshal(&req)
			if err != nil {
				zap.S().Errorw("producer marshal error", "err", err)
			}
			// write length of the request
			reqByteLen := int64ToBytes(int64(len(reqByte)))
			n, err = p.f.Write(reqByteLen)
			if err != nil || n != 8 {
				zap.S().Errorw("instance request error", "err", err, "reqByteLen", 8, "n", n)
			}
			// write the request
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

func int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func bytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
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
