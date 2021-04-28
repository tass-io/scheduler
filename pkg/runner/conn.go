package runner

import (
	"bufio"
	"encoding/json"
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
	// `{"a": "b"}littledrizzle{"a": "c"}` maybe in the pipe, so to split two request, we need a delimiter
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
		data := make([]byte, 4<<20)
		reader := bufio.NewReader(c.f)
		tail := ""
		for {
			// todo fixed error read
			_, err := reader.Read(data)
			if err != nil {
				zap.S().Errorw("consumer error", "err", err)
			}
			s := string(data)
			s = tail + s
			tail = ""
			pkg := strings.Split(s, string(splitByte))
			if len(pkg) == 0 {
				// take care of `{"s":"b"}`
				pkg = append(pkg, s)
			}
			for _, item := range pkg {
				resp := &functionResponse{}
				err = json.Unmarshal([]byte(item), resp)
				if err != nil {
					zap.S().Infow("consumer unmarshal error", "err", err, "item", item)
					// take care of `{"s":"b"}littledrizzle{"n":`
					// the item must be the last one
					tail = item
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
