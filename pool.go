package zredis

import (
	"sync"
	"fmt"
)

var DieClient = make(chan *RedisClient)
var pMapKey = make(chan *RedisClient)

type Pool struct {
	network string
	addr    string
	syn     sync.Mutex
	Clients chan *RedisClient
	IdIe    int
	Size    int
}

func NewRedisPool(network, addr string, Size int) (*Pool) {
	network = network
	address = addr
	pool := &Pool{Clients: make(chan *RedisClient, Size), Size: Size}
	pool.initPool()
	return pool
}

func (p *Pool) InitPool() {
	currentSize := len(p.Clients)

	for i := 0; i < (p.Size - currentSize); i++ {
		c, err := DialRedis(p.network, p.addr)
		if err != nil {
			fmt.Println("redis connection erro :", err)
			break
		}
		p.Clients <- c
	}
}

func (p *Pool) GetOneRedisConn() (*RedisClient) {
	for {
		select {
		case c := <-p.Clients:
			if !c.Ping() {
				c.Close()
				continue
			}
			return c
		default:
			p.InitPool()
			continue
		}
	}
}

func (p *Pool) ReturnPool(c *RedisClient) {
	p.Clients <- c
	return
}
