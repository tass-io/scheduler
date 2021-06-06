package instance

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/tass-io/scheduler/pkg/tools/common"
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
)

// FunctionRequest will be put into the producer and send it to request pipe
type FunctionRequest struct {
	Id         string                 `json:"id"`
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
		Id:         id,
		Parameters: transfer,
	}
}

// FunctionResponse will be put into channel to be consume by instance.
type FunctionResponse struct {
	Id     string
	Result map[string]interface{}
}

// Producer waits for requests from request channel and put the data into the process
type Producer struct {
	f                  *os.File
	demo               interface{}
	requestChannel     chan interface{}
	startRoutineExited bool
}

// Consumer waits for responses and put the data into response channel
type Consumer struct {
	f               *os.File
	demo            interface{}
	responseChannel chan interface{}
	noNewInfo       bool
}

// NewConsumer creates a new consumer data structure cantains a channel and an unnamed pipe
func NewConsumer(f *os.File, demo interface{}) *Consumer {
	return &Consumer{
		f:               f,
		demo:            demo,
		responseChannel: make(chan interface{}, 10),
	}
}

// consumer Start gets the function response and sends it to the channel
func (c *Consumer) Start() {
	go func() {
		typ := reflect.TypeOf(c.demo)
		zap.S().Debugw("consumer get type", "type", typ)
		data := make([]byte, 4<<20)
		reader := bufio.NewReader(c.f)
		tail := ""
		for {
			// todo fixed error read
			n, err := reader.Read(data)
			if n == 0 {
				zap.S().Debug("read 0")
				if err == nil {
					continue
				} else {
					if err == io.EOF {
						zap.S().Info("consumer read EOF")
						break
					}
				}
			}
			s := string(data[:n])
			s = tail + s
			pkg := strings.Split(s, string(splitByte))
			if len(pkg) == 0 {
				// when the case is only one response,
				// for example, `{"hello":"world"}`
				pkg = append(pkg, s)
			}
			for _, item := range pkg {
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
					//
					// NOTE: this case may never happen. In linux kernel implementation,
					// a lock is required during read/write operations.
					// Since all tests are passed, the code remains now.
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
func NewProducer(f *os.File, demo interface{}) *Producer {
	return &Producer{
		f:                  f,
		demo:               demo,
		requestChannel:     make(chan interface{}, 10),
		startRoutineExited: false,
	}
}

// producer listens the channel and gets a request, writes the data to pipe
func (p *Producer) Start() {
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
