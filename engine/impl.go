package engine

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/rryqszq4/go-murmurhash"
)

const (
	defaultBlockNumber          = 256
	defaultBlockCapacity        = 1024 * 1024 * 8 // 8MB
	seed                 uint64 = 0x12345678
)

type FastKV struct {
	partitions  []*block
	blockNumber uint64
}

type block struct {
	lock   *sync.RWMutex
	index  map[uint64]int
	data   []byte
	offset int //data 当前写入位置
	remain int //data 剩余空间
}

func getHash(key []byte) uint64 {
	v := murmurhash.MurmurHash64A(key, seed)
	return v
}

func NewDB() *FastKV {
	blocks := make([]*block, 0, defaultBlockNumber)
	for i := 0; i < defaultBlockNumber; i++ {
		blocks = append(blocks, newBlock())
	}

	return &FastKV{
		partitions:  blocks,
		blockNumber: defaultBlockNumber,
	}
}

func (f *FastKV) Set(key, value []byte, expire int) {
	hash := getHash(key)
	i := hash % f.blockNumber
	f.partitions[i].set(key, value, hash, expire)
}

func (f *FastKV) Get(key []byte) (value []byte, err error) {
	hash := getHash(key)
	i := hash % f.blockNumber
	return f.partitions[i].get(key, hash)
}

func (f *FastKV) Delete(key []byte) (err error) {
	hash := getHash(key)
	i := hash % f.blockNumber
	return f.partitions[i].delete(key, hash)
}

func newBlock() *block {
	return &block{
		lock:   &sync.RWMutex{},
		index:  make(map[uint64]int),
		data:   make([]byte, defaultBlockCapacity),
		offset: 0,
		remain: defaultBlockNumber,
	}
}

// 写入格式
// [key size - 4 byte][value size - 4 byte][expire at - 8 byte][key][value]
func (b *block) set(key, value []byte, hash uint64, expire int) {
	var expireAt int64 = 0
	if expire != 0 {
		expireAt = time.Now().Unix() + int64(expire)
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	lk := len(key)
	lv := len(value)
	l := lk + lv + 4 + 4 + 8
	for b.remain <= l {
		b.evict()
	}

	b.index[hash] = b.offset
	binary.LittleEndian.PutUint32(b.data[b.offset:], uint32(lk))
	binary.LittleEndian.PutUint32(b.data[b.offset+4:], uint32(lv))
	binary.LittleEndian.PutUint64(b.data[b.offset+4+4:], uint64(expireAt))
	copy(b.data[b.offset+4+4+8:], key)
	copy(b.data[b.offset+4+4+8+int(lk):], value)
	b.offset += l
	b.remain -= l
}

func (b *block) get(key []byte, hash uint64) (value []byte, err error) {
	b.lock.RLocker().Lock()
	defer b.lock.RLocker().Unlock()

	offset, ok := b.index[hash]
	// fmt.Println(string(key), offset, ok)
	if !ok {
		return nil, ErrNil
	}

	lk := binary.LittleEndian.Uint32(b.data[offset:])

	// 避免hash碰撞，需要实际检查下key是否相同
	if !bytes.Equal(key, b.data[offset+4+4+8:offset+4+4+8+int(lk)]) {
		return nil, ErrNil
	}

	expireAt := binary.LittleEndian.Uint64(b.data[offset+4+4:])
	if expireAt != 0 && expireAt < uint64(time.Now().Unix()) {
		return nil, ErrNil
	}

	lv := binary.LittleEndian.Uint32(b.data[offset+4:])
	return b.data[offset+4+4+8+int(lk) : offset+4+4+8+int(lk+lv)], nil
}

func (b *block) delete(key []byte, hash uint64) (err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	offset, ok := b.index[hash]
	if !ok {
		return ErrNil
	}

	lk := binary.LittleEndian.Uint32(b.data[offset:])

	// 避免hash碰撞，需要实际检查下key是否相同
	if !bytes.Equal(key, b.data[offset+4+4+8:offset+4+4+8+int(lk)]) {
		return ErrNil
	}

	delete(b.index, hash)

	expireAt := binary.LittleEndian.Uint64(b.data[offset+4+4:])
	if expireAt != 0 && expireAt < uint64(time.Now().Unix()) {
		return ErrNil
	}

	return nil
}

func (b *block) evict() {
	offset := b.offset + b.remain
	if offset+1 >= len(b.data) {
		// 放弃队列尾部空间，重置指针到头部
		binary.LittleEndian.PutUint32(b.data[b.offset:], 0)
		b.offset = 0
		b.remain = 0
		offset = 0
	}

	lk := binary.LittleEndian.Uint32(b.data[offset:])

	//  说明后面这段数据整体没被使用
	if lk == 0 {
		b.remain = len(b.data) - b.offset
		return
	}

	lv := binary.LittleEndian.Uint32(b.data[offset+4:])
	b.remain += int(lk + lv + 8 + 4 + 4)

	// 把一组kv的首位写成0，相当于删除了这组数据
	binary.LittleEndian.PutUint32(b.data[offset:], 0)

	// 删除数据的同时，需要检查索引并删除
	keyOffset := offset + 4 + 4 + 8
	hash := getHash(b.data[keyOffset : keyOffset+int(lk)])
	if b.index[hash] == offset {
		delete(b.index, hash)
	}

	return
}
