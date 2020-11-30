package segment

import (
	"github.com/zzonee/leaf-go/util"
)

type segment struct {
	max   int64
	step  int64
	value util.AtomicInt64
}

func (s *segment) reset(max, step int64) {
	s.max = max
	s.step = step
	s.value = util.AtomicInt64(max - step)
}

// 剩余多少值
func (s *segment) idle() int64 {
	v := s.value.Value()
	return s.max - v
}

// 消耗掉一个值
func (s *segment) incr() (newValue int64) {
	newValue = s.value.Add(1) - 1
	return
}

func (s *segment) valid(v int64) bool {
	return v < s.max
}
