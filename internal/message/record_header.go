package message

import "encoding/binary"

type Header struct {
	Key   []byte
	Value []byte
}

type HeaderIterator struct {
	data   []byte
	offset int
	count  int
}

func (hi *HeaderIterator) Next() (Header, bool) {
	if hi == nil || hi.count <= 0 || hi.offset >= len(hi.data) {
		return Header{}, false
	}

	// 1. Header Key Length (varint)
	keyLen, n := binary.Varint(hi.data[hi.offset:])
	if n <= 0 {
		return Header{}, false
	}
	hi.offset += n

	// 2. Header Key
	var key []byte
	if keyLen > 0 {
		key = hi.data[hi.offset : hi.offset+int(keyLen)]
		hi.offset += int(keyLen)
	}

	// 3. Header Value Length (varint)
	valLen, n := binary.Varint(hi.data[hi.offset:])
	if n <= 0 {
		return Header{}, false
	}
	hi.offset += n

	// 4. Header Value
	var val []byte
	if valLen > 0 {
		val = hi.data[hi.offset : hi.offset+int(valLen)]
		hi.offset += int(valLen)
	}

	hi.count--
	return Header{Key: key, Value: val}, true
}
