package main

import (
	"github.com/longyufei109/leaf-go/config"
	"github.com/longyufei109/leaf-go/server"
)

func main() {
	if err := config.Init(); err != nil {
		panic(err)
	}
	server.Start()
}
