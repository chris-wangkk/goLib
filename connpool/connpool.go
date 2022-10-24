package connpool

import (
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	poolMap  = make(map[string]*Pool)
	poolLock sync.RWMutex
)

// 连接实例
type item struct {
	data interface{}
	hb   time.Time
}

// 连接池操作
type Pool struct {
	New   func() interface{}
	Ping  func(interface{}) bool
	Close func(interface{})
	Idle  time.Duration //空闲时长
	store chan *item
	mu    sync.Mutex
}

func (p *Pool) create() (interface{}, error) {
	if nil == p.New {
		return nil, fmt.Errorf("Pool.New is nil, can not create connection")
	}
	return p.New(), nil
}

func (p *Pool) Get() (interface{}, error) {
	if nil == p.store {
		//pool可能已经销毁，此时返回新创建的连接
		return p.create()
	}
	for {
		i, more := <-p.store
		if !more {
			//连接池关闭，新建连接
			return p.create()
		} else {
			v := i.data
			if p.Idle > 0 && time.Now().Sub(i.hb) > p.Idle {
				//开启了空闲限制
				if nil != p.Close {
					p.Close(v)
				}
				continue
			}
			if nil != p.Ping && !p.Ping(v) {
				//有效性校验
				continue
			}
			return v, nil
		}
	}
}

func (p *Pool) Put(v interface{}) {
	if nil == p.store {
		return
	}
	select {
	case p.store <- &item{data: v, hb: time.Now()}:
		return
	default:
		if nil != p.Close {
			p.Close(v)
		}
		return
	}
}

func (p *Pool) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if nil == p.store {
		return
	}
	close(p.store)
	for i := range p.store {
		if nil != p.Close {
			p.Close(i.data)
		}
	}
	p.store = nil
}

func (p *Pool) Len() int {
	return len(p.store)
}

func NewPool(initCap, maxCap int, newFunc func() interface{}) (*Pool, error) {
	if 0 == maxCap || initCap > maxCap {
		return nil, fmt.Errorf("invalid cap")
	}
	p := new(Pool)
	p.store = make(chan *item, maxCap)
	if nil != newFunc {
		p.New = newFunc
	}
	for i := 0; i < initCap; i++ {
		v, err := p.create()
		if nil != err {
			return p, err
		}
		p.store <- &item{data: v, hb: time.Now()}
	}
	return p, nil
}

// tcp连接池
func GetTCPConnPool(key, addr, newwork string, timeout time.Duration) *Pool {
	var (
		pool *Pool
		ok   bool
	)
	poolLock.RLock()
	pool, ok = poolMap[key]
	poolLock.RUnlock()
	if ok {
		return pool
	}
	poolLock.Lock()
	defer poolLock.Unlock()
	pool, ok = poolMap[key]
	if ok {
		//double lock check
		return pool
	}

	pool, _ = NewPool(1, 10000, func() interface{} {
		c, e := net.DialTimeout(newwork, addr, timeout)
		if nil != e {
			return nil
		}
		return c
	})
	pool.Idle = 3 * time.Minute

	pool.Ping = func(conn interface{}) bool {
		if nil == conn {
			return false
		}
		if _, ok := conn.(net.Conn); !ok {
			return false
		}
		return true
	}

	pool.Close = func(conn interface{}) {
		if nil == conn {
			return
		}
		if c, ok := conn.(net.Conn); ok {
			c.Close()
		}
	}

	poolMap[key] = pool
	return pool
}

// 定时检测
func init() {
	go func() {
		for {
			time.Sleep(time.Minute)

			var (
				poolNum int
				connNum int
			)
			poolLock.RLock()
			poolNum = len(poolMap)
			for _, p := range poolMap {
				connNum += p.Len()
			}
			poolLock.RUnlock()
			fmt.Printf("poolNum=%d, connNum=%d \n",
				poolNum, connNum)
		}
	}()
}
