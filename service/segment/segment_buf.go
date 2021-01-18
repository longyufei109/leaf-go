package segment

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"leaf-go/config"
	"leaf-go/entity"
	"leaf-go/log"
	"leaf-go/repo"
	"leaf-go/util"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	MaxStep                = 1e6     // 最大步长不超过 100w
	SegmentDurationSeconds = 60 * 15 // 900s, 15分钟
)

// 双缓冲
type segmentBuf struct {
	key                 string // 通常为业务名,biz_tag（见repo/db.go注释）
	repo                repo.Repo
	segments            []*segment      // len = 2
	pos                 int             // 当前使用的segment索引
	initok              bool            // 是否初始化成功过
	step                int64           // 当前step，[minStep, MaxStep]，动态变化
	minStep             int64           // 最小步长，等于初始时从repo中加载到的step值。后续不会变化
	lastUpdateTimestamp int64           // 用来动态改变step，单位秒
	mu                  sync.RWMutex    // 读多写少场景
	isNextReady         util.AtomicBool // 下一个segment是否准备好了
	isLoadingNext       util.AtomicBool // 是否正在加载下一个segment，避免并发加载
	stopped             util.AtomicBool
}

func newSegmentBuf(key string, r repo.Repo) *segmentBuf {
	sb := &segmentBuf{
		key:      key,
		repo:     r,
		segments: []*segment{{}, {}},
	}
	if err := sb.load(); err != nil {
		log.Print("load segment buf from file failed. buf:%s. err:%s. try load from repo", sb.key, err.Error())
		if err := sb.updateSegment(sb.curSegment()); err == nil {
			sb.initSuccess()
			log.Print("load segment buf from repo success. buf:%s", sb.key)
		} else {
			log.Print("[newSegmentBuf] updateSegment err:%v", err)
		}
	} else {
		log.Print("load segment buf from file success. buf:%s", sb.key)
	}
	return sb
}

func (sb *segmentBuf) initSuccess() {
	sb.initok = true
}

func (sb *segmentBuf) nextPos() int {
	return (sb.pos + 1) % 2
}

// 写锁保护时调用
func (sb *segmentBuf) switchPos() {
	sb.pos = sb.nextPos()
}

// 至少得读锁保护时使用
func (sb *segmentBuf) curSegment() *segment {
	return sb.segments[sb.pos]
}

func (sb *segmentBuf) nextSegment() *segment {
	return sb.segments[sb.nextPos()]
}

func (sb *segmentBuf) nextId() (int64, error) {
	if !sb.initok { // todo remove this, always init segmentBuf in newSegmentBuf()
		sb.mu.Lock()
		if !sb.initok {
			err := sb.updateSegment(sb.curSegment())
			if err != nil {
				sb.mu.Unlock()
				return -1, err
			}
			sb.initSuccess()
		}
		sb.mu.Unlock()
	}
	return sb.getIdFromSegment()
}

// 除了在Init中调用(实际上Init中也可以不调用，Init中加载是为了减少nextId时的锁竞争)
// 其它地方都需要写锁保护
func (sb *segmentBuf) updateSegment(s *segment) (err error) {
	var seg entity.Segment
	newStep := sb.step
	if !sb.initok || sb.lastUpdateTimestamp == 0 { // 如果还未初始化
		if seg, err = sb.repo.UpdateMaxIdAndGetSegment(sb.key); err != nil {
			return err
		}
		newStep = seg.Step
	} else { // 如果已初始化，动态调整step
		// 计算新的step
		newStep = sb.step
		duration := curTimeInSecond() - sb.lastUpdateTimestamp // 距离上次更新的间隔
		if duration < SegmentDurationSeconds {                 // 如果间隔过小(小于指定值)则2倍速增加步长，但不超过最大步长
			if newStep*2 < MaxStep {
				newStep *= 2
			}
		} else if duration > SegmentDurationSeconds*2 { // 如果间隔过大(大于指定值)则2倍速减少步长，但不小于最小步长
			if newStep/2 > sb.minStep {
				newStep /= 2
			}
		}
		// 更新repo中maxId
		if seg, err = sb.repo.UpdateMaxIdByStepAndGetSegment(sb.key, newStep); err != nil {
			return err
		}
	}
	sb.step = newStep
	sb.minStep = seg.Step
	sb.lastUpdateTimestamp = curTimeInSecond()

	s.reset(seg.MaxId, newStep)
	sb.dump()
	return nil
}

func (sb *segmentBuf) getIdFromSegment() (int64, error) {
	sb.mu.RLock()
	if sb.stopped.True() {
		sb.mu.RUnlock()
		return -1, fmt.Errorf("server closed")
	}
	seg := sb.curSegment()
	// 如果已经消耗了10% 且 下一个segment尚未加载，则预加载下一个segment
	if float64(seg.idle()) < 0.9*float64(seg.step) && !sb.isNextReady.True() {
		// 如果已经在加载了则不进行加载，避免并发加载，造成浪费
		if sb.isLoadingNext.False2True() {
			go sb.loadNextSegment()
		}
	}
	if id := seg.incr(); seg.valid(id) {
		sb.mu.RUnlock()
		return id, nil
	}
	sb.mu.RUnlock()
	// 当前segment已用完
	var count int64
	for sb.isLoadingNext.True() { // 尽可能等待加载完成，但循环退出时不一定完成了加载
		count += 1
		if count > 10000 {
			time.Sleep(time.Millisecond * 10)
			break
		}
	}
	_ = count
	sb.mu.Lock() // 有可能多个go routine阻塞在这里
	defer sb.mu.Unlock()
	if sb.stopped.True() {
		return -1, fmt.Errorf("server closed")
	}
	seg = sb.curSegment() // 这里是为了后面(第2、3...个)进来的协程获取id，因为第1个协程将isNextReady置为false
	if id := seg.incr(); seg.valid(id) {
		return id, nil
	}

	if sb.isNextReady.True() { // 第一个拿到锁的协程进入if，并负责切换segment
		sb.switchPos()
		sb.dump()
		sb.isNextReady.Set(false)

		seg = sb.curSegment()
		if id := seg.incr(); seg.valid(id) {
			return id, nil
		} else {
			return -1, fmt.Errorf("new segment exhausted, buf:%s", sb.key)
		}
	}
	return -1, fmt.Errorf("both two segments not ready, buf:%s", sb.key)
}

func (sb *segmentBuf) loadNextSegment() {
	if sb.isNextReady.True() {
		return
	}
	sb.isLoadingNext.Set(true)
	if err := sb.updateSegment(sb.nextSegment()); err == nil {
		sb.isNextReady.Set(true)
	} else {
		log.Print("[loadNextSegment] updateSegment err:%v", err)
	}
	sb.isLoadingNext.Set(false)
}

func curTimeInSecond() int64 {
	return time.Now().Unix()
}

func (sb *segmentBuf) dump() {
	//log.Print("init:%v, lastTS:%d", sb.initok, sb.lastUpdateTimestamp)
	//seg := sb.curSegment()
	//log.Print("pos:%d, seg:{max:%d, step:%d, value:%d}", sb.pos, seg.max, seg.step, seg.value.Value())
	//seg = sb.nextSegment()
	//log.Print("pos:%d, seg:{max:%d, step:%d, value:%d}", sb.nextPos(), seg.max, seg.step, seg.value.Value())
}

type segBufCache struct {
	Key     string `json:"key"`
	Step    int64  `json:"step"`
	MinStep int64  `json:"min_step"`
	Pos     int    `json:"pos"`
	Segs    []struct {
		Max   int64 `json:"max"`
		Step  int64 `json:"step"`
		Value int64 `json:"value"`
	} `json:"segs"`
}

func (sb *segmentBuf) store() {
	sb.stopped.Set(true) // 先标记为true，等拿到锁后，再存文件

	sb.mu.Lock()
	defer sb.mu.Unlock()

	if !sb.initok {
		log.Print("segment buf not init. buf:%s", sb.key)
		return
	}
	for sb.isLoadingNext.True() { // 等待完成加载
		time.Sleep(100 * time.Millisecond)
	}
	// 存文件
	cache := &segBufCache{
		Key:     sb.key,
		Step:    sb.step,
		MinStep: sb.minStep,
		Pos:     sb.pos,
		Segs: []struct {
			Max   int64 `json:"max"`
			Step  int64 `json:"step"`
			Value int64 `json:"value"`
		}{
			{sb.curSegment().max, sb.curSegment().step, sb.curSegment().value.Value()},
			{sb.nextSegment().max, sb.nextSegment().step, sb.nextSegment().value.Value()},
		},
	}
	// 检查路径是否存在 不存在则创建
	fi, err := os.Stat(config.Global.Segment.CacheDir)
	if err == nil && !fi.IsDir() {
		log.Print("[segmentBuf] store() path `%s` is not a dir ", config.Global.Segment.CacheDir)
		return
	}
	if fi == nil {
		if err = os.MkdirAll(config.Global.Segment.CacheDir, os.ModeDir); err != nil {
			log.Print("[segmentBuf] store() mkdir failed, path: `%s`", config.Global.Segment.CacheDir)
			return
		}
	}

	filename := sb.key + ".json"
	fp := filepath.Join(config.Global.Segment.CacheDir, filename)
	f, err := os.Create(fp)
	if err != nil {
		log.Print("[segmentBuf] store create fiale failed. key:%s", sb.key)
		return
	}
	data, _ := json.Marshal(cache)
	_, _ = f.Write(data)
	_ = f.Close()
	log.Print("[segmentBuf] store success. key:%s, path:%s", sb.key, fp)
}

func (sb *segmentBuf) load() error {
	filename := sb.key + ".json"
	fp := filepath.Join(config.Global.Segment.CacheDir, filename)
	f, err := os.Open(fp)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	sbCache := &segBufCache{}
	if err = json.Unmarshal(data, sbCache); err != nil {
		return err
	}
	sb.pos = sbCache.Pos
	sb.step = sbCache.MinStep
	sb.minStep = sbCache.MinStep
	sb.lastUpdateTimestamp = curTimeInSecond()

	sb.curSegment().max = sbCache.Segs[sb.pos].Max
	sb.curSegment().step = sbCache.Segs[sb.pos].Step
	sb.curSegment().value = util.AtomicInt64(sbCache.Segs[sb.pos].Value)

	sb.nextSegment().max = sbCache.Segs[sb.nextPos()].Max
	sb.nextSegment().step = sbCache.Segs[sb.nextPos()].Step
	sb.nextSegment().value = util.AtomicInt64(sbCache.Segs[sb.nextPos()].Value)
	sb.isNextReady.Set(sb.nextSegment().idle() > 0) // or sb.nextSegment().idle()==sb.nextSegment().step
	sb.initSuccess()
	return nil
}
