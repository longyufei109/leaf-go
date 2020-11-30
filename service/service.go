package service

type IdGenerator interface {
	Init() error
	Gen(key string) (id int64, err error)
	Shutdown()
}
