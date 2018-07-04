package zredis

import (
	"sync"
	"fmt"
)

var DieClient = make(chan *RedisClient)
var pMapKey = make(chan *RedisClient)
var cli *RedisClient
var e   error

type Pool struct {
	network string
	addr    string
	syn     sync.Mutex
	Clients chan *RedisClient
	IdIe    int
	Size    int
}

func NewRedisPool(network, addr string, Size int) (*Pool) {
	pool := &Pool{network: network, addr: addr, Clients: make(chan *RedisClient, Size), Size: Size}
	pool.InitPool()
	return pool
}

func (p *Pool) InitPool() {
	currentSize := len(p.Clients)

	for i := 0; i < (p.Size - currentSize); i++ {
		cli, e = DialRedis(p.network, p.addr)
		if e != nil {
			fmt.Println("redis connection erro :", e)
			break
		}
		cli.Id = i
		p.Clients <- cli
	}
}

func (p *Pool) IdieSize() (int) {
	return len(p.Clients)
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

			if len(p.Clients) == 0 {
				p.InitPool()
			}
			continue
		}
	}
}

func (p *Pool) ReturnPool(c *RedisClient) {

	if p.IdieSize() < p.Size{
		 p.Clients <- c
		 return
	 }
	 c.Close()
	 return
}
