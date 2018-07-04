package zredis

import (
	"bufio"
	"github.com/pkg/errors"
	"strconv"
)

type Resp struct {
	RespType int
	Val      interface{}
}

func (r *Resp) String() (string) {
	switch rsp := r.Val.(type) {
	case string:
		return rsp
	case []byte:
		return string(rsp)
	case int:
		return strconv.Itoa(rsp)
	case float64:
		return string(strconv.AppendFloat(buffer[:0], rsp, 'g', -1, 64))
	default:
		return TypeException

	}

}

func (r *Resp) Btyes() ([]byte) {
	switch rsp := r.Val.(type) {
	case string:
		return []byte(rsp)
	case []byte:
		return rsp
	case int:
		return strconv.AppendInt(convertBuf[:0], int64(rsp), 10)
	case float64:
		return strconv.AppendFloat(buffer[:0], rsp, 'g', -1, 64)
	default:
		return []byte(TypeException)

	}
}

func Response(r *bufio.Reader) (*Resp, error) {
	peek, err := r.Peek(2)
	if err != nil {
		return &Resp{0, nil}, err
	}
	switch peek[0] {
	case responseNormalPrefix:
		return NormalReply(r)
	case responseErrorPrefix:
		return ErrorReply(r)
	case responseIntegerPrefix:
		return IntegerReply(r)
	case responseBlockPrefix:
		return BlockReply(r)
	case responseLinePrefix:
		return ArrayReply(r)
	default:
		return &Resp{0, peek}, errors.New("unknow reply:" + string(peek))
	}
	return &Resp{0, peek}, errors.New("unknow reply:" + string(peek))
}
func NormalReply(r *bufio.Reader) (*Resp, error) {
	str, err := r.ReadBytes(end)

	if err != nil {
		return &Resp{1, nil}, err
	}
	return &Resp{1, string(str[1:])}, nil
}

func ErrorReply(r *bufio.Reader) (*Resp, error) {
	str, err := r.ReadBytes(end)
	if err != nil {
		return &Resp{2, nil}, err
	}
	return &Resp{2, str[1:]}, nil
}

func IntegerReply(r *bufio.Reader) (*Resp, error) {
	bytes, err := r.ReadBytes(end)
	if err != nil {
		return &Resp{3, nil}, err
	}
	i, err := strconv.ParseInt(string(bytes[1:len(bytes)-2]), 10, 64)

	if err != nil {
		return &Resp{3, nil}, err
	}

	return &Resp{3, i}, nil

}

func BlockReply(r *bufio.Reader) (*Resp, error) {
	bytes, err := r.ReadBytes(end)
	if err != nil {
		return &Resp{4, nil}, err
	}
	size, err := strconv.ParseInt(string(bytes[1:len(bytes)-2]), 10, 64)
	if err != nil {
		return &Resp{4, nil}, err
	}

	if size <= 0 {
		return &Resp{4, nil}, errors.New("read bite size is 0")
	}

	total := make([]byte, size)
	b2 := total
	var n int
	for len(b2) > 0 {
		n, err = r.Read(b2)
		if err != nil {
			return &Resp{4, nil}, err
		}
		b2 = b2[n:]
	}

	trail := make([]byte, 2)
	for i := 0; i < 2; i++ {
		c, err := r.ReadByte()
		if err != nil {
			return &Resp{4, nil}, err
		}
		trail[i] = c
	}

	return &Resp{4, total}, nil
}

func ArrayReply(r *bufio.Reader) (*Resp, error) {
	peer, err := r.ReadBytes(end)

	if err != nil {
		return &Resp{5, nil}, err
	}

	size, err := strconv.ParseInt(string(peer[1:len(peer)-2]), 10, 16)

	if err != nil {
		return &Resp{5, nil}, err
	}

	if size <= 0 {
		return &Resp{5, nil}, errors.New("read to bite is 0")
	}
	return &Resp{5, nil}, nil
}
