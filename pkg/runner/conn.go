package runner

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"

	"go.uber.org/zap"
)

/*

				   Instance model
				    |          ^
functionRequest     |          |   functionResponse
				    v          |
			     producer   consumer
			        |           ^
	bytes	        |           |     bytes
					|           ----------------------|
					|----------------------------> process

*/

var (
	// split byte just for functionRequest package delimiter
	splitByte = []byte("littledrizzle")
)
// functionRequest will be put into the producer and send it to request pipe
type functionRequest struct {
	Id         string `json:"id"`
	Parameters string `json:"parameters"`
}

func NewFunctionRequest(id string, parameters map[string]interface{}) *functionRequest {
	parameterStr, err := json.Marshal(parameters)
	if err != nil {
		zap.S().Errorw("functionRequest marshal error", "err", err)
		return nil
	}
	return &functionRequest{
		Id:         id,
		Parameters: string(parameterStr),
	}
}

// functionResponse will be put into channel to be consume by instance.
type functionResponse struct {
	Id     string
	Result map[string]interface{}
}

type producer struct {
	f              *os.File
	requestChannel chan functionRequest
}

type consumer struct {
	f               *os.File
	responseChannel chan functionResponse
}

func (c *consumer) GetChannel() chan functionResponse {
	return c.responseChannel
}

func NewConsumer(f *os.File) *consumer {
	return &consumer{
		f:               f,
		responseChannel: make(chan functionResponse, 10),
	}
}

// consumer Start will get function response and send it to the channel
func (c *consumer) Start() {
	go func() {
		var buff bytes.Buffer
		for {
			_, err := io.Copy(&buff, c.f)
			if err != nil {
				zap.S().Errorw("consumer error", "err", err)
			}
			s := buff.String()
			pkg := strings.Split(s, string(splitByte))
			for _, item := range pkg {
				resp := &functionResponse{}
				err = json.Unmarshal([]byte(item), resp)
				if err != nil {
					zap.S().Errorw("consumer unmarshal error", "err", err, "item", item)
					continue
				}
				c.responseChannel <- *resp
			}
		}
	}()
}

func (c *consumer) Terminate() {
	c.f.Close()
	close(c.responseChannel)
}

func (p *producer) GetChannel() chan functionRequest {
	return p.requestChannel
}


func NewProducer(f *os.File) *producer {
	return &producer{
		f:              f,
		requestChannel: make(chan functionRequest, 10),
	}
}

// producer will listen channel and get request, write to pipe
func (p *producer) Start() {
	go func() {
		for req := range p.requestChannel {
			reqByte, _ := json.Marshal(&req)
			reqByte = append(reqByte, splitByte...)
			n, err := p.f.Write(reqByte)
			if err != nil || n != len(reqByte) {
				zap.S().Errorw("instance request error", "err", err, "reqByteLen", len(reqByte), "n", n)
			}
		}
	}()
}

func (p *producer) Terminate() {
	// pay attention!!! producer will not close the channel, FunctionScheduler will close it
	p.f.Close()
}
