package local

import (
	"os"
	"sync"
)

type Status int32

const (
	Init        Status = 0
	Running     Status = 1
	Terminating Status = 2
)

type instance struct {
	sync.Locker
	functionName    string
	Status          Status
	producer        *producer
	consumer        *consumer
	responseMapping map[string]chan map[string]interface{}
}

func NewInstance(functionName string) *instance {
	return &instance{
		Locker:          &sync.Mutex{},
		functionName:    functionName,
		responseMapping: make(map[string]chan map[string]interface{}, 10),
	}
}

func NewPipe() (*os.File, *os.File, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	return read, write, nil

}

// instance start will init and start producer/consumer, and start the real work process
func (i *instance) Start() (err error) {
	producerRead, producerWrite, err := NewPipe()
	if err != nil {
		return
	}
	consumerRead, consumerWrite, err := NewPipe()
	if err != nil {
		return
	}
	i.producer = NewProducer(producerWrite)
	i.producer.Start()
	i.consumer = NewConsumer(consumerRead)
	i.consumer.Start()
	i.startProcess(producerRead, consumerWrite)
	return
}

// start function process and use pipe create connection
func (i *instance) startProcess(request *os.File, response *os.File) (err error) {
	return
}

// Invoke will generate a uuid for functionRequest and will block until the function return the result
func (i *instance) Invoke(parameters map[string]interface{}) (result map[string]interface{}, err error) {
	return
}
