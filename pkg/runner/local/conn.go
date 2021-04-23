package local

import (
	"encoding/json"
	"os"

	"go.uber.org/zap"
)

type functionRequest struct {
	Id         string
	Parameters string
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

func NewProducer(f *os.File) *producer {
	return &producer{
		f:              f,
		requestChannel: make(chan functionRequest, 10),
	}
}

func (c *consumer) GetChannel() chan functionResponse {
	return c.responseChannel
}

// consumer Start will get function response and send it to the channel
func (c *consumer) Start() {

}

func NewConsumer(f *os.File) *consumer {
	return &consumer{
		f:               f,
		responseChannel: make(chan functionResponse, 10),
	}
}

func (p *producer) GetChannel() chan functionRequest {
	return p.requestChannel
}

// producer will listen channel and get request, write to pipe
func (p *producer) Start() {

}
