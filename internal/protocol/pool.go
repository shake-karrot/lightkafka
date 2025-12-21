package protocol

import (
	"fmt"
	"sync"
)

//NOTE(Danu): Sync Pool이 아니라 Fixed pool을 사용하면 메모리 할당 로직을 직접 제어해야함. 나중에 Arena를 써볼 수 있을 것 같음

type PoolConfig struct {
	MaxPoolSize int
}

var DefaultPoolConfig = PoolConfig{
	MaxPoolSize: 1024 * 64,
}

var BytePool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

func GetBufferWithCapacity(capacity int) *[]byte {
	ptr := BytePool.Get().(*[]byte)

	// TODO(Danu): Byte pool을 종류별로 지정이 필요
	if cap(*ptr) < capacity {
		fmt.Println("Reallocating buffer with capacity", capacity)
		b := make([]byte, capacity)
		return &b
	}

	*ptr = (*ptr)[:capacity]
	return ptr
}

func PutBuffer(ptr *[]byte) {

	if len(*ptr) > DefaultPoolConfig.MaxPoolSize {
		fmt.Println("Discarding buffer with length", len(*ptr))
		return
	}

	BytePool.Put(ptr)
}
