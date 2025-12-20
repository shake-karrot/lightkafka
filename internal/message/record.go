package message

import (
	"encoding/binary"
)

// Record represents a view into a single Kafka record.
// No data is copied; Key/Value point to the underlying mmap buffer.
type Record struct {
	Length         int64
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32

	// Computed Values
	Offset    int64
	Timestamp int64

	// Zero-Copy Slices
	Key   []byte
	Value []byte

	// Headers Info
	HeadersCount int
	headersRaw   []byte
}

// BatchIterator iterates over records without allocation.
type BatchIterator struct {
	data          []byte
	offset        int
	recordsLeft   int32
	baseOffset    int64
	baseTimestamp int64
}

func (b *RecordBatch) NewIterator() *BatchIterator {
	return &BatchIterator{
		data:          b.Payload,
		offset:        0,
		recordsLeft:   b.Header.RecordsCount,
		baseOffset:    b.Header.BaseOffset,
		baseTimestamp: b.Header.BaseTimestamp,
	}
}

func (it *BatchIterator) Next(out *Record) bool {
	if it.recordsLeft <= 0 || it.offset >= len(it.data) {
		return false
	}

	// 1. Length
	recLen, n := binary.Varint(it.data[it.offset:])
	if n <= 0 {
		return false
	}
	it.offset += n
	out.Length = recLen

	recordEnd := it.offset + int(recLen)
	if recordEnd > len(it.data) {
		return false
	}

	// 2. Attributes
	out.Attributes = int8(it.data[it.offset])
	it.offset++

	// 3. TimestampDelta
	tsDelta, n := binary.Varint(it.data[it.offset:])
	it.offset += n
	out.TimestampDelta = tsDelta
	out.Timestamp = it.baseTimestamp + tsDelta

	// 4. OffsetDelta
	offDelta, n := binary.Varint(it.data[it.offset:])
	it.offset += n
	out.OffsetDelta = int32(offDelta)
	out.Offset = it.baseOffset + int64(offDelta)

	// 5. Key
	keyLen, n := binary.Varint(it.data[it.offset:])
	it.offset += n
	if keyLen >= 0 {
		out.Key = it.data[it.offset : it.offset+int(keyLen)]
		it.offset += int(keyLen)
	} else {
		out.Key = nil
	}

	// 6. Value
	valLen, n := binary.Varint(it.data[it.offset:])
	it.offset += n
	if valLen >= 0 {
		out.Value = it.data[it.offset : it.offset+int(valLen)]
		it.offset += int(valLen)
	} else {
		out.Value = nil
	}

	// 7. Headers
	hCount, n := binary.Varint(it.data[it.offset:])
	it.offset += n
	out.HeadersCount = int(hCount)

	if it.offset < recordEnd {
		out.headersRaw = it.data[it.offset:recordEnd]
	} else {
		out.headersRaw = nil
	}

	it.offset = recordEnd
	it.recordsLeft--
	return true
}
