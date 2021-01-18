package server

import (
	"fmt"
	"leaf-go/config"
	"leaf-go/log"
	"leaf-go/repo"
	"leaf-go/server/http"
	"leaf-go/service"
	"leaf-go/service/segment"
	"leaf-go/service/snowflake"
	"os"
	"os/signal"
	"syscall"
)

var g service.IdGenerator

func Start() {
	if config.Global.Mode == config.Mode_Snowflake {
		g = newSnowflake()
	} else if config.Global.Mode == config.Mode_Segment {
		g = newSegment()
	} else {
		panic("not support mode")
	}
	if err := g.Init(); err != nil {
		panic(err)
	}

	go http.Start(g)

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	<-sig
	g.Shutdown()
	log.Print("server stopped")
}

func newSnowflake() service.IdGenerator {
	conf := snowflake.Config{
		Twepoch: 0,
		WorkerIdGetter: func() int64 {
			return 0
		},
	}
	return snowflake.New(conf)
}

func newSegment() service.IdGenerator {
	r, err := repo.NewRepo()
	if err != nil {
		panic(fmt.Sprintf("init repo failed. err:%s", err.Error()))
	}
	return segment.New(r)
}
