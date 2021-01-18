package main

import (
	"leaf-go/config"
	"leaf-go/server"
)

func main() {
	if err := config.Init(); err != nil {
		panic(err)
	}
	server.Start()
}
