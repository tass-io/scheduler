package local

import "os"

type functionRequest struct {
	Id         string
	Parameters string
}

func NewFunctionRequest(parameters map[string]interface{}) *functionRequest {
	return &functionRequest{}
}

type functionResponse struct {
	Id     string
	Result map[string]interface{}
}

type consumer struct {
	f              *os.File
	requestChannel chan functionRequest
}

type producer struct {
	f               *os.File
	responseChannel chan functionResponse
}

func NewConsumer(f *os.File) *consumer {
	return &consumer{
		f:              f,
		requestChannel: make(chan functionRequest, 10),
	}
}

func (c *consumer) GetChannel() chan functionRequest {
	return c.requestChannel
}

// consumer Start will get function response and send it to the channel
func (c *consumer) Start() {

}

func NewProducer(f *os.File) *producer {
	return &producer{
		f:               f,
		responseChannel: make(chan functionResponse, 10),
	}
}

func (p *producer) GetChannel() chan functionResponse {
	return p.responseChannel
}

// producer will listen channel and get request, write to pipe
func (p *producer) Start() {

}
