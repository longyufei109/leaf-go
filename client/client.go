package client

type Config struct {
	Endpoints   []string
	RequestPath string
	Query       string
}

type Client interface {
	GetId(key string) (int64, error)
}
