package segment

import (
	"fmt"
	"leaf-go/repo"
	"leaf-go/service"
	"sync"
	"time"
)

type segmentGen struct {
	repo  repo.Repo
	cache sync.Map
	stop  chan struct{}
}

func New(repo repo.Repo) service.IdGenerator {
	g := &segmentGen{
		repo: repo,
		stop: make(chan struct{}),
	}
	return g
}

func (s *segmentGen) Init() error {
	// 初始化时先加载一次
	if err := s.updateCacheFromRepo(); err != nil {
		return err
	}
	go s.updatePeriodically(time.Minute)
	return nil
}

func (s *segmentGen) updatePeriodically(interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-s.stop:
			s.cache.Range(func(_, value interface{}) bool {
				sb := value.(*segmentBuf)
				sb.store()
				return true
			})
			return
		case <-tick.C:
			_ = s.updateCacheFromRepo()
		}
	}
}

func (s *segmentGen) updateCacheFromRepo() error {
	allKeys, err := s.repo.GetAllKeys()
	if err != nil {
		return err
	}

	allKeysSet := map[string]struct{}{}
	for _, key := range allKeys {
		allKeysSet[key] = struct{}{}
		if _, ok := s.cache.Load(key); !ok { // 新增的key
			sb := newSegmentBuf(key, s.repo) // 在这里初始化segmentBuf比较好
			s.cache.Store(key, sb)
		}
	}
	s.cache.Range(func(key, value interface{}) bool {
		if _, ok := allKeysSet[key.(string)]; !ok { // repo中已删除的key
			s.cache.Delete(key)
		}
		return true
	})
	return nil
}

func (s *segmentGen) Gen(key string) (id int64, err error) {
	select {
	case <-s.stop:
		return -1, fmt.Errorf("server closed")
	default:
	}
	sb, ok := s.cache.Load(key)
	if !ok {
		id = -1
		err = fmt.Errorf("not support key:%s", key)
		return
	}
	return sb.(*segmentBuf).nextId()
}

func (s *segmentGen) Shutdown() {
	close(s.stop)
}
