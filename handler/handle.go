package handler

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/ccfarm/simple_cache/buffer"
	"github.com/ccfarm/simple_cache/engine"
	"github.com/ccfarm/simple_cache/logger"
)

const (
	cmdSET   = "SET"
	cmdSETEX = "SETEX"
	cmdGET   = "GET"
	cmdDEL   = "DEL"

	delimCR = '\r'
	delimLF = '\n'

	respOK             = "+OK\r\n"
	respKeyNotExist    = "$-1\r\n"
	respErr            = "-ERR %s '%s'\r\n"
	respDelKey         = ":1\r\n"
	respDelKeyNotExist = ":0\r\n"

	unknownCommand = "unknown command"
	unknownErr     = "unknown err"
)

var (
	db *engine.FastKV
)

func init() {
	db = engine.NewDB()
}

// redis协议 https://www.redis.com.cn/topics/protocol.html

// example

// C: *2\r\n
// C: $3\r\n
// C: GET\r\n
// C: $3\r\n
// C: key\r\n

// S: $5\r\n
// S: value\r\n

func parseCMD(r *buffer.NoCopyBufferReader) ([][]byte, error) {
	tokenNumber, err := parseNumber(r)
	if err != nil {
		return nil, err
	}

	tokens := make([][]byte, 0, tokenNumber)
	for tokenNumber > 0 {
		tokenNumber -= 1

		tokenLen, err := parseNumber(r)
		if err != nil {
			return nil, err
		}

		token, err := r.Read(tokenLen)
		if err != nil {
			return nil, err
		}

		_, err = r.Read(2)
		if err != nil {
			return nil, err
		}

		tokens = append(tokens, token)
	}
	return tokens, nil
}

func parseNumber(r *buffer.NoCopyBufferReader) (int, error) {
	_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	n := 0
	var b byte = 0
	for {
		b, err = r.ReadByte()
		if err != nil {
			return 0, err
		}

		if b == delimCR {
			break
		}

		n = n*10 + int(b-'0')
		if err != nil {
			return 0, err
		}
	}

	_, err = r.ReadByte()
	if err != nil {
		return 0, err
	}

	return n, nil
}

func handleSet(key, value []byte, w *bufio.Writer) error {
	handleSetEX(key, value, 0, w)
	return nil
}

func handleSetEX(key, value []byte, expire int, w *bufio.Writer) error {
	db.Set(key, value, expire)
	if _, err := w.Write([]byte(respOK)); err != nil {
		return err
	}

	return nil
}

func handleGet(key []byte, w *bufio.Writer) error {
	value, err := db.Get(key)
	if err != nil {
		if err == engine.ErrNil {
			if _, err := w.Write([]byte(respKeyNotExist)); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if _, err = w.Write([]byte("$")); err != nil {
		return err
	}

	if _, err = w.Write([]byte(strconv.Itoa(len(value)))); err != nil {
		return err
	}

	if _, err = w.Write([]byte("\r\n")); err != nil {
		return err
	}

	if _, err = w.Write(value); err != nil {
		return err
	}

	if _, err = w.Write([]byte("\r\n")); err != nil {
		return err
	}

	return nil
}

func handleDel(key []byte, w *bufio.Writer) error {
	err := db.Delete(key)
	if err != nil {
		if err == engine.ErrNil {
			if _, err := w.Write([]byte(respDelKeyNotExist)); err != nil {

				return err
			}

			return nil
		} else {
			return err
		}
	}

	if _, err := w.Write([]byte(respDelKey)); err != nil {
		return err
	}

	return nil
}

func Handle(conn net.Conn) {
	reader := buffer.GetReader(conn)
	writer := bufio.NewWriter(conn)

	defer reader.Release()

	var err error
	defer func() {
		if err != nil {
			_, _ = writer.Write([]byte(fmt.Sprintf(respErr, unknownErr, err.Error())))
			_ = writer.Flush()
		}

		conn.Close() //处理完之后要关闭这个连接
	}()

	//针对当前的连接做数据的发送和接收
	for {
		var tokens [][]byte
		tokens, err = parseCMD(reader)

		if err != nil {
			if err != io.EOF {
				logger.Errorf("parse cmd err. %+v", err)
			}
			return
		}

		cmd := strings.ToUpper(string(tokens[0]))
		switch cmd {
		case cmdSET:
			err = handleSet(tokens[1], tokens[2], writer)
		case cmdSETEX:
			expire, _ := strconv.Atoi(string(tokens[2]))
			err = handleSetEX(tokens[1], tokens[3], expire, writer)
		case cmdGET:
			err = handleGet(tokens[1], writer)
		case cmdDEL:
			err = handleDel(tokens[1], writer)
		default:
			_, err = writer.Write([]byte(fmt.Sprintf(respErr, unknownCommand, string(cmd))))
		}

		if err != nil {
			return
		}

		err = writer.Flush()
		if err != nil {
			return
		}

		reader.CollectGarbage()
	}
}
