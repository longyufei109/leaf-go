// 1bit not used | 41 bits for timestamp(ms) | 10 bits for workerId | 12 bits for sequence
package snowflake

import (
	"fmt"
	"leaf-go/service"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	defaultTewpoch int64 = 1603509071000 // 北京时间 2020-10-24 11:11:11
	workerIdBits   int64 = 10
	maxWorkerId    int64 = 1<<uint64(workerIdBits) - 1 // workerId: [0, 1023]
	sequenceBits   int64 = 12
	sequenceMask   int64 = 1<<uint64(sequenceBits) - 1 // 0x0FFF
	workerIdShift        = uint64(sequenceBits)
	timestampShift       = uint64(workerIdBits + sequenceBits)
)

type Config struct {
	Twepoch        int64
	WorkerIdGetter func() int64
}

type snowflake struct {
	conf          Config
	workerId      int64
	lock          sync.Locker
	sequence      int64
	lastTimestamp int64
}

func New(conf Config) service.IdGenerator {
	if conf.Twepoch <= 0 || conf.Twepoch > curMilliseconds() {
		conf.Twepoch = defaultTewpoch
	}

	workerId := conf.WorkerIdGetter()
	if workerId < 0 || workerId > maxWorkerId {
		panic(fmt.Sprintf("invalid workerId:%d", workerId))
	}

	g := &snowflake{
		conf:     conf,
		workerId: workerId,
		lock:     new(caslock),
	}
	return g
}

func (s *snowflake) Init() error {
	return nil
}

func (s *snowflake) Gen(_ string) (id int64, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	now := curMilliseconds()
	if now < s.lastTimestamp { // 时钟回拨
		offset := s.lastTimestamp - now
		if offset < 5 { // 回拨不超过5毫秒
			time.Sleep(time.Duration(offset << 1))
			now = curMilliseconds()
			if now < s.lastTimestamp { // 通常 now > s.lastTimestamp
				id = -1
				err = fmt.Errorf("1. machine time changed extremely")
				return
			}
		} else {
			// 后续的请求都将返回-2，因为不再更新s.lastTimestamp
			// 重启后程序恢复正常，但仍可能生成重复id(机器当前时间可能小于重启前的s.lastTimestamp)
			id = -2
			err = fmt.Errorf("2. machine time changed extremely")
			return
		}
	}

	if s.lastTimestamp == now {
		s.sequence = (s.sequence + 1) & sequenceMask
		if s.sequence == 0 { // 当前这1毫秒内的序列号用尽了
			for now <= s.lastTimestamp { // 循环 直到 下一毫秒
				now = curMilliseconds()
			}
			s.sequence = randomSequence(100)
		}
	} else { // 全新的时间戳
		s.sequence = randomSequence(100)
	}
	s.lastTimestamp = now
	id = ((now - s.conf.Twepoch) << timestampShift) | (s.workerId << workerIdShift) | s.sequence
	return
}

func (s *snowflake) Shutdown() {

}

func curMilliseconds() int64 {
	return time.Now().UnixNano() / 1e6
}

func randomSequence(max int64) int64 {
	return rand.Int63n(max) // [0, max)
}

type caslock int32

func (cas *caslock) Lock() {
	for !atomic.CompareAndSwapInt32((*int32)(cas), 0, 1) {
		runtime.Gosched()
	}
}

func (cas *caslock) Unlock() {
	atomic.StoreInt32((*int32)(cas), 0)
}
