package instance

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"reflect"

	// "github.com/tass-io/scheduler/pkg/utils/common"
	"syscall"

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

const (
	mmapThreshold     = 32 * (1 << 10)
	defaultMemMapSize = 128 * (1 << 20) // 映射的内存大小为 128M
	mmapFilePath      = "/tass/mmap"
)

var (
	// ping is a information to show the producer is ready to send requests,
	// indicating the process is ready
	ping = []byte("ping")
	// mmap is a information to show the producer is sending the next request in a mmap area
	mmap = []byte("mmap")
	// TODO: try to fix this
	ID = ""
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
	//  zap.S().Errorw("functionRequest marshal error", "err", err)
	//  return nil
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
	dataRef            []byte
	data               interface{}
	requestChannel     chan interface{}
	startRoutineExited bool
	Role               int
}

// Consumer waits for responses and put the data into response channel
type Consumer struct {
	f               *os.File
	dataRef         []byte
	data            interface{}
	responseChannel chan interface{}
	initDoneChannel chan struct{}
	noNewInfo       bool
	Role            int
}

// NewConsumer creates a new consumer data structure cantains a channel and an unnamed pipe
func NewConsumer(f *os.File, data interface{}) *Consumer {
	consumer := Consumer{
		f:               f,
		data:            data,
		responseChannel: make(chan interface{}, 10),
		initDoneChannel: make(chan struct{}, 1),
	}
	m, err := os.OpenFile(mmapFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		zap.S().Panicw(fmt.Sprintln(err))
	}
	if info, err := m.Stat(); info.Size() < defaultMemMapSize {
		if err != nil {
			zap.S().Panicw(fmt.Sprintln(err))
		}
		if err := m.Truncate(defaultMemMapSize); err != nil {
			zap.S().Panicw(fmt.Sprintln(err))
		}
	}
	if consumer.dataRef, err = syscall.Mmap(int(m.Fd()), 0, defaultMemMapSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		zap.S().Panicw(fmt.Sprintln(err))
	}
	return &consumer
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

			response := reflect.New(typ.Elem()).Interface() // cannot decalre a interface{} directly

			if string(data) == string(mmap) {
				// 2.2 check if the data is transfered by mmap
				zap.S().Debug("receive a 'mmap' signal")
				data = make([]byte, 8) // 8 bytes for int64 length
				n, err = io.ReadFull(reader, data)
				if n == 0 {
					zap.S().Debug("read nothing")
					if err == nil {
						continue
					} else if err == io.EOF {
						zap.S().Info("consumer read EOF")
						break
					}
				}
				switch c.Role {
				case 0:
					{
						// scheduler consumer get response data len in mmap from wrapper(runtime)
						response = &FunctionResponse{
							ID:     ID,
							Result: map[string]interface{}{"mmapBytes": bytesToInt64(data)},
						}
					}
				case 1:
					{
						// wrapper(runtime) get input data in mmap from ls
						response = c.dataRef[:bytesToInt64(data)]
					}
				default:
					zap.S().Panicw("not supported consumer role", "role", c.Role)
				}
				// err = json.Unmarshal(c.dataRef[:bytesToInt64(data)], response)
			} else {
				// 2.3 normal case: read contents from pipe and unmatshal
				// TODO: format code
				zap.S().Panicw("no mmap?? wtf???")
				// err = json.Unmarshal(data, response)
			}

			// if err != nil {
			// 	zap.S().Panic("consumer unmarshal error", "err", err)
			// }
			c.responseChannel <- response
		}

		zap.S().Debug("no more requests")
		c.noNewInfo = true
		c.f.Close()
		syscall.Munmap(c.dataRef)
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
	producer := Producer{
		f:                  f,
		data:               data,
		requestChannel:     make(chan interface{}, 10),
		startRoutineExited: false,
	}
	m, err := os.OpenFile(mmapFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		zap.S().Panicw(fmt.Sprintln(err))
	}
	if info, err := m.Stat(); info.Size() < defaultMemMapSize {
		if err != nil {
			zap.S().Panicw(fmt.Sprintln(err))
		}
		if err := m.Truncate(defaultMemMapSize); err != nil {
			zap.S().Panicw(fmt.Sprintln(err))
		}
	}
	if producer.dataRef, err = syscall.Mmap(int(m.Fd()), 0, defaultMemMapSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		zap.S().Panicw(fmt.Sprintln(err))
	}
	return &producer
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
			// FIXME: handle the parallel request condition
			switch p.Role {
			case 0:
				{
					// The producer is just initialted by scheduler
					p.Role = 1
					rn, err := req.(map[string]interface{})["parameters"].(map[string]interface{})["body"].(io.ReadCloser).Read(p.dataRef)
					ID = req.(map[string]interface{})["id"].(string)
					if err != nil {
						zap.S().Panicw("instance producer reading body error", "err", err, "rn", rn)
					}
					zap.S().Infow("instance producer is just creating. read", rn, "bytes")
					n, err = p.f.Write(mmap)
					if err != nil || n != len(mmap) {
						zap.S().Errorw("instance producer creating mmap request error", "err", err, "reqByteLen", len(mmap), "n", n)
					}
					n, err = p.f.Write(int64ToBytes(int64(rn)))
					if err != nil || n != 8 {
						zap.S().Errorw("instance producer creating mmap request error", "err", err, "reqByteLen", 8, "n", n)
					}
				}
			case 1:
				{
					// The producer is called once again by scheduler to transfer intermediate data
					mmapBytes := int(req.(map[string]interface{})["parameters"].(map[string]interface{})["mmapBytes"].(float64))
					n, err = p.f.Write(mmap)
					if err != nil || n != len(mmap) {
						zap.S().Errorw("instance producer creating mmap request error", "err", err, "reqByteLen", len(mmap), "n", n)
					}
					n, err = p.f.Write(int64ToBytes(int64(mmapBytes)))
					if err != nil || n != 8 {
						zap.S().Errorw("instance producer creating mmap request error", "err", err, "reqByteLen", 8, "n", n)
					}
				}
			case 2:
				{
					// The producer is called by the wrapper(runtime) to send data info in mmap dataRef
					results := req.([]byte)
					copy(p.dataRef, results)
					n, err = p.f.Write(mmap)
					if err != nil || n != len(mmap) {
						zap.S().Errorw("instance producer creating mmap request error", "err", err, "reqByteLen", len(mmap), "n", n)
					}
					n, err = p.f.Write(int64ToBytes(int64(len(results))))
					if err != nil || n != 8 {
						zap.S().Errorw("instance producer creating mmap request error", "err", err, "reqByteLen", 8, "n", n)
					}

				}
			default:
				zap.S().Panicw("Unknown producer role!!", "role", p.Role)
			}
		}
		zap.S().Debug("producer close")
		p.startRoutineExited = true
		p.f.Close()
		syscall.Munmap(p.dataRef)
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
