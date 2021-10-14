package predictmodel

type mlp struct {}

func NewMLPPolicy() Policy {
	return &mlp{}
}

func (p *mlp) GetPath() {
	
}