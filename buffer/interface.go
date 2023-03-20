package buffer

type Buffer interface {
	Skip(n int) error
	Peek(n int) ([]byte, error)
	Read(n int) ([]byte, error)
	Write([]byte) error
	Flush() error
}
