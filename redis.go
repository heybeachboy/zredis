package tools

import (
	"net"
	"time"
	"bufio"
	"sync"
	"strconv"
	"fmt"
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
	WriteTimeOut time.Duration
	WriteBuffer  *bufio.Writer
	ReadTimeOut  time.Duration
	ReadBuffer   *bufio.Reader
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

	if op.password != "" {

	}

	if op.keepAlive > 0 {

	}

	if op.db > 0 {

	}

	return &RedisClient{Conn: conn,
		WriteTimeOut: op.wTimeOut,
		WriteBuffer: bufio.NewWriter(conn),
		ReadTimeOut: op.rTimeOut,
		ReadBuffer: bufio.NewReader(conn),
	}, nil
}

func (c *RedisClient) SetString(key, value string, expire int64) {
	if expire != 0 {
		c.writeInterface("SET", key, value, "EX", expire)
	} else {
		c.writeInterface("SET", key, value)
	}

}

func resetBuffer() {
	if len(buffer) != 0 {
		buffer = buffer[:0]
	}
}

func (c *RedisClient) writeHeadFlag(prefix byte, size int) {
	resetBuffer()
	buffer = append(buffer, prefix)
	buffer = append(buffer, strconv.AppendInt(convertBuf[:0], int64(size), 10)...)
	buffer = append(buffer, delimiter...)

}

func (c *RedisClient) writeInterface(args ...interface{}) (error) {
	c.writeHeadFlag(responseLinePrefix, len(args))
	for _, arg := range args {
		c.argOperation(arg)
	}
	_, err := c.WriteBuffer.Write(buffer)

	if err != nil {
		return err
	}
	fmt.Println("write:", string(buffer))
	err2 := c.WriteBuffer.Flush()
	if err2 != nil {
		return err2
	}
	resp, err := Response(c.ReadBuffer)

	if err != nil {
		return err
	}

	fmt.Println("reply:", resp.Val)
	return nil

}

func (c *RedisClient) Cmd(args ...interface{}) {
	c.writeInterface(args...)
}

func resetReadBuf() {
	if len(readBuf) > 0 {
		readBuf = readBuf[:0]
	}
}

func (c *RedisClient) readResponse() (error) {
	resp, err := c.ReadBuffer.ReadBytes(end)
	fmt.Println("resp:", string(resp))
	return err
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
	default:
		fmt.Println("exception status:", arg)
	}
}

func (c *RedisClient) writeByte(arg []byte) {
	writeLength(responseBlockPrefix, len(arg))
	buffer = append(buffer, arg...)
	writeEndFlag()
}

func writeLength(prefix byte, n int) {
	buffer = append(buffer, responseBlockPrefix)
	buffer = append(buffer, strconv.AppendInt(convertBuf[:0], int64(n), 10)...)
	writeEndFlag()
}

func (c *RedisClient) writeString(arg string) {
	buffer = append(buffer, []byte("$"+strconv.Itoa(len(arg))+"\r\n"+arg+"\r\n")...)
}

func (c *RedisClient) writeInt(arg int) {
	byteInt := strconv.AppendInt(convertBuf[:0], int64(arg), 10)
	writeLength(responseBlockPrefix, len(byteInt))
	buffer = append(buffer, byteInt...)
	writeEndFlag()

}

func writeEndFlag() {
	buffer = append(buffer, delimiter...)
}
func (c *RedisClient) writeInt64(arg int64) {
	bytesInt := strconv.AppendInt(convertBuf[:0], arg, 10)
	writeLength(responseBlockPrefix, len(bytesInt))
	buffer = append(buffer, bytesInt...)
	writeEndFlag()
}

func (c *RedisClient) writeFloat64(arg float64) {
	bytes := strconv.AppendFloat(buffer[:0], arg, 'g', -1, 64)
	writeLength(responseBlockPrefix, len(bytes))
	buffer = append(buffer, bytes...)
	writeEndFlag()
}

func (c *RedisClient) GetString(key string) (string) {
	return ""
}
