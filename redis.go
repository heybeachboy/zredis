package zredis

import (
	"net"
	"time"
	"bufio"
	"sync"
	"strconv"
	"fmt"
	//"errors"
	)

var (
	delimiter = []byte{'\r', '\n'}
	end       = byte('\n')
)
var buffer []byte
var convertBuf []byte
var readBuf []byte
var (
	responseNormalPrefix  = byte('+')
	responseErrorPrefix   = byte('-')
	responseIntegerPrefix = byte(':')
	responseBlockPrefix   = byte('$')
	responseLinePrefix    = byte('*')
)

type RedisClient struct {
	Conn         net.Conn
	syn          sync.Mutex
	buffer       []byte
	WriteTimeOut time.Duration
	WriteBuffer  *bufio.Writer
	ReadTimeOut  time.Duration
	ReadBuffer   *bufio.Reader
	Id           int
	pending      byte
}

type options struct {
	db        int
	password  string
	dialer    *net.Dialer
	keepAlive time.Duration
	rTimeOut  time.Duration
	wTimeOut  time.Duration
}
type SetAttr struct {
	f func(options *options)
}

func SetReadTimeout(t time.Duration) (SetAttr) {
	return SetAttr{func(p *options) {
		p.rTimeOut = t
	}}
}

func SetWriteTimeout(t time.Duration) (SetAttr) {
	return SetAttr{func(p *options) {
		p.wTimeOut = t
	}}
}

func SetPassword(password string) (SetAttr) {
	return SetAttr{func(p *options) {
		p.password = password
	}}
}

func SetDbbase(db int) (SetAttr) {
	return SetAttr{func(p *options) {
		p.db = db
	}}
}

func SetKeepAlive(t time.Duration) (SetAttr) {
	return SetAttr{func(p *options) {
		p.keepAlive = t
	}}
}

func DialRedis(network, addr string, ops ...SetAttr) (*RedisClient, error) {
	op := options{dialer: &net.Dialer{Timeout: time.Minute * 5}}

	for _, o := range ops {
		o.f(&op)
	}
	if network == "" {
		network = "tcp"
	}

	if addr == "" {
		addr = "localhost:6379"
	}

	conn, err := op.dialer.Dial(network, addr)
	if err != nil {
		conn.Close()
		return nil, err
	}

	cli := &RedisClient{Conn: conn,
		WriteTimeOut: op.wTimeOut,
		WriteBuffer: bufio.NewWriter(conn),
		ReadTimeOut: op.rTimeOut,
		ReadBuffer: bufio.NewReader(conn),
	}

	if op.password != "" {
		_, err := cli.Cmd("AUTH", op.password)

		if err != nil {
			return nil, err
		}

	}

	if op.db > 0 {
		_, err := cli.Cmd("SELECT", op.db)
		if err != nil {
			return nil, err
		}
	}
	return cli, nil
}

func NewConnection(network, addr string) (*RedisClient, error) {
	return DialRedis(network,addr)
}

func (c *RedisClient) SetString(key, value string, expire int64) (*Resp, error) {

	if expire != 0 {
		return c.writeInterface("SET", key, value, "EX", expire)
	} else {

		return c.writeInterface("SET", key, value)
	}


}

func (c *RedisClient) GetString(key string) (string, error) {
	resp, err := c.writeInterface("GET", key)
	return resp.String(),err

}

func (c *RedisClient) PushList(name string,args... interface{})(error) {
	_, err := c.writeInterface("LPUSH",args)
	return err
}

func (c *RedisClient)resetBuffer() {
	if len(c.buffer) != 0 {
		c.buffer = c.buffer[:0]
	}
}

func (c *RedisClient) writeHeadFlag(prefix byte, size int) {
	c.resetBuffer()
	c.buffer = append(c.buffer, prefix)
	c.buffer = append(c.buffer, strconv.AppendInt(convertBuf[:0], int64(size), 10)...)
	c.buffer = append(c.buffer, delimiter...)

}

func (c *RedisClient) writeInterface(args ...interface{}) (*Resp, error) {
	c.writeHeadFlag(responseLinePrefix, len(args))
	for _, arg := range args {
		c.argOperation(arg)
	}
	  if c.WriteTimeOut > 0 {
	  	c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeOut))
	  }
	  _, err := c.WriteBuffer.Write(c.buffer)


	  if err != nil {
		return &Resp{0, nil}, err
	}

	err2 := c.WriteBuffer.Flush()
	//c.syn.Unlock()
	if err2 != nil {
		return &Resp{0, nil}, err2
	}

	if c.ReadTimeOut > 0 {
		c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeOut))
	}
	resp, err := Response(c.ReadBuffer)

	return resp, err

}

func (c *RedisClient) Ping() (bool) {
	resp, _ := c.writeInterface("PING")
	if resp.RespType == 1 {
		return true
	}
	return false
}

func (c *RedisClient) Close() {
	 c.Conn.Close()
}

func (c *RedisClient) Cmd(args ...interface{}) (*Resp, error) {
	return c.writeInterface(args...)
}

func resetReadBuf() {
	if len(readBuf) > 0 {
		readBuf = readBuf[:0]
	}
}


func (c *RedisClient) argOperation(arg interface{}) {
	switch arg := arg.(type) {
	case []byte:
		c.writeByte(arg)
	case string:
		c.writeString(arg)
	case int:
		c.writeInt(arg)
	case int64:
		c.writeInt64(arg)
	case float64:
		c.writeFloat64(arg)
	case bool:
		if arg {
			c.writeString("1")
		} else {
			c.writeString("0")
		}

	case nil:
		c.writeString("")
	case interface{}:


	default:
		fmt.Println("exception status:", arg)
	}
}

func (c *RedisClient) writeByte(arg []byte) {
	c.writeLength(responseBlockPrefix, len(arg))
	c.buffer = append(c.buffer, arg...)
	c.writeEndFlag()
}

func (c *RedisClient)writeLength(prefix byte, n int) {
	c.buffer = append(c.buffer, responseBlockPrefix)
	c.buffer = append(c.buffer, strconv.AppendInt(convertBuf[:0], int64(n), 10)...)
	c.writeEndFlag()
}

func (c *RedisClient) writeString(arg string) {
	c.buffer = append(c.buffer, []byte("$"+strconv.Itoa(len(arg))+"\r\n"+arg+"\r\n")...)
}

func (c *RedisClient) writeInt(arg int) {
	byteInt := strconv.AppendInt(convertBuf[:0], int64(arg), 10)
	c.writeLength(responseBlockPrefix, len(byteInt))
	c.buffer = append(c.buffer, byteInt...)
	c.writeEndFlag()

}

func (c *RedisClient)writeEndFlag() {
	c.buffer = append(c.buffer, delimiter...)
}
func (c *RedisClient) writeInt64(arg int64) {
	bytesInt := strconv.AppendInt(convertBuf[:0], arg, 10)
	c.writeLength(responseBlockPrefix, len(bytesInt))
	c.buffer = append(c.buffer, bytesInt...)
	c.writeEndFlag()
}

func (c *RedisClient) writeFloat64(arg float64) {
	bytes := strconv.AppendFloat(c.buffer[:0], arg, 'g', -1, 64)
	c.writeLength(responseBlockPrefix, len(bytes))
	c.buffer = append(c.buffer, bytes...)
	c.writeEndFlag()
}
