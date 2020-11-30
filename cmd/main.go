package main

import (
	"github.com/zzonee/leaf-go/config"
	"github.com/zzonee/leaf-go/server"
)

func main() {
	if err := config.Init(); err != nil {
		panic(err)
	}
	server.Start()
}
