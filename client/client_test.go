package client

import (
	"fmt"
	"sync"
	"testing"
)

func TestHttpClient_GetId(t *testing.T) {
	conf := Config{
		Endpoints:   []string{"localhost:8080"},
		RequestPath: "/api/id",
		Query:       "key",
	}
	c := NewHttpClient(conf)
	wg := sync.WaitGroup{}
	num := 100
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			defer wg.Done()
			id, err := c.GetId("test")
			fmt.Println("Id:", id, "err:", err)
		}()
	}
	wg.Wait()
}
