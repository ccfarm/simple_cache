package engine

import "errors"

type Engine interface {
	Set(key, value []byte, expire int) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) error
}

var (
	ErrNil = errors.New("Nil")
)
