package server

import (
	"fmt"
	"github.com/zzonee/leaf-go/config"
	"github.com/zzonee/leaf-go/log"
	"github.com/zzonee/leaf-go/repo"
	"github.com/zzonee/leaf-go/server/http"
	"github.com/zzonee/leaf-go/service"
	"github.com/zzonee/leaf-go/service/segment"
	"github.com/zzonee/leaf-go/service/snowflake"
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
