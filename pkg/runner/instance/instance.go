package instance

type Instance interface {
	Invoke(parameters map[string]interface{}) (map[string]interface{}, error)
	Score() int
	Release()
	Start() error
}
