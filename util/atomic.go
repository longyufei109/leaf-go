package util

import "sync/atomic"

type AtomicBool int32

func (b *AtomicBool) True() bool {
	return atomic.LoadInt32((*int32)(b)) == 1
}

// 从 false 变成 true
func (b *AtomicBool) False2True() bool {
	return atomic.CompareAndSwapInt32((*int32)(b), 0, 1)
}

func (b *AtomicBool) Set(v bool) {
	if v {
		atomic.StoreInt32((*int32)(b), 1)
	} else {
		atomic.StoreInt32((*int32)(b), 0)
	}
}

type AtomicInt64 int64

func (ai *AtomicInt64) Add(num int64) (newValue int64) {
	newValue = atomic.AddInt64((*int64)(ai), num)
	return
}

func (ai *AtomicInt64) Value() int64 {
	return atomic.LoadInt64((*int64)(ai))
}
