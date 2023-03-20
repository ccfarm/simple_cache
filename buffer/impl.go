package buffer

import (
	"net"
	"sync"
)

var (
	readerPool sync.Pool
	nodePool   sync.Pool
)

const (
	NodeCapacity = 4096
)

type NoCopyBufferReader struct {
	conn    net.Conn
	wNode   *LinkListNode
	rNode   *LinkListNode
	garbage *LinkListNode // 暂存0拷贝缓存，确认使用完成后再清理
	remain  int
}

type LinkListNode struct {
	readOffset  int
	writeOffset int
	data        []byte
	next        *LinkListNode
}

func init() {
	nodePool.New = newListNode
	readerPool.New = newReader
}

func GetReader(conn net.Conn) *NoCopyBufferReader {
	r := readerPool.Get().(*NoCopyBufferReader)
	r.conn = conn
	return r
}

func (b *NoCopyBufferReader) Release() {
	b.conn = nil
	b.remain = 0
	b.wNode = nil

	for b.rNode != nil {
		b.releaseRNode()
	}

	b.CollectGarbage()
}

func newReader() any {
	n := getListNode()

	return &NoCopyBufferReader{
		wNode:  n,
		rNode:  n,
		remain: 0,
	}
}

func (b *NoCopyBufferReader) ReadByte() (byte, error) {
	for b.remain < 1 {
		err := b.fill()
		if err != nil {
			return 0, err
		}
	}

	if b.rNode.writeOffset == b.rNode.readOffset {
		b.releaseRNode()
	}

	data := b.rNode.data[b.rNode.readOffset]
	b.rNode.readOffset += 1
	b.remain -= 1

	return data, nil
}

func (b *NoCopyBufferReader) Read(n int) ([]byte, error) {
	defer func() { b.remain -= n }()
	for b.remain < n {
		err := b.fill()
		return nil, err
	}

	if b.rNode.writeOffset == b.rNode.readOffset {
		b.releaseRNode()
	}

	if b.rNode.writeOffset-b.rNode.readOffset >= n {
		data := b.rNode.data[b.rNode.readOffset : b.rNode.readOffset+n]
		b.rNode.readOffset += n
		return data, nil
	}

	data := make([]byte, 0, n)
	remain := n

	for remain > 0 {
		if remain > b.rNode.writeOffset-b.rNode.readOffset {
			data = append(data, b.rNode.data[b.rNode.readOffset:b.rNode.writeOffset]...)
			remain -= b.rNode.writeOffset - b.rNode.readOffset
			b.rNode.readOffset = b.rNode.writeOffset
		} else {
			data = append(data, b.rNode.data[b.rNode.readOffset:b.rNode.readOffset+remain]...)
			b.rNode.readOffset += remain
			remain = 0
			return data, nil
		}

		if b.rNode.writeOffset == b.rNode.readOffset {
			b.releaseRNode()
		}
	}
	return data, nil
}

func (b *NoCopyBufferReader) releaseRNode() {
	t := b.rNode
	b.rNode = t.next

	t.next = b.garbage
	b.garbage = t
}

func newListNode() any {
	return &LinkListNode{
		readOffset:  0,
		writeOffset: 0,
		data:        make([]byte, NodeCapacity),
		next:        nil,
	}
}

func getListNode() *LinkListNode {
	return nodePool.Get().(*LinkListNode)
}

func releaseListNode(n *LinkListNode) {
	n.next = nil
	n.readOffset = 0
	n.writeOffset = 0
	nodePool.Put(n)
}

func (b *NoCopyBufferReader) CollectGarbage() {
	for b.garbage != nil {
		t := b.garbage
		b.garbage = t.next
		releaseListNode(t)
	}
}

func (b *NoCopyBufferReader) fill() error {
	if b.wNode.writeOffset == NodeCapacity {
		b.wNode.next = getListNode()
		b.wNode = b.wNode.next
	}

	n, err := b.conn.Read(b.wNode.data[b.wNode.writeOffset:])
	if err != nil {
		return err
	}

	b.remain += n
	b.wNode.writeOffset += n

	return nil
}
