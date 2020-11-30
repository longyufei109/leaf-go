package snowflake

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnowflake_Gen(t *testing.T) {
	conf := Config{
		Twepoch:        0,
		WorkerIdGetter: getworkerId,
	}
	g := New(conf)
	_ = g.Init()

	wg := sync.WaitGroup{}
	wg.Add(100)

	var count int64
	stop := make(chan bool)

	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = g.Gen("")
				atomic.AddInt64(&count, 1)
			}
		}(i)
	}
	time.Sleep(time.Second * 10)
	close(stop)
	fmt.Println("total id: ", count) // total id:  40466125，单机100并发，10s，QPS 400w+
	wg.Wait()
}

func getworkerId() int64 {
	return 0
}
