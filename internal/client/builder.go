package client

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// RecordBatchBuilder helps constructing a valid Kafka RecordBatch (v2).
type RecordBatchBuilder struct {
	records        []simpleRecord
	firstTimestamp int64
}

type simpleRecord struct {
	key   []byte
	value []byte
}

func NewRecordBatchBuilder() *RecordBatchBuilder {
	return &RecordBatchBuilder{
		records:        make([]simpleRecord, 0),
		firstTimestamp: time.Now().UnixMilli(),
	}
}

// Add appends a key-value record to the batch.
func (b *RecordBatchBuilder) Add(key, value []byte) {
	b.records = append(b.records, simpleRecord{key: key, value: value})
}

// Build encodes the batch into raw bytes ready to be sent to the broker.
func (b *RecordBatchBuilder) Build() []byte {
	// 1. Encode Records first to calculate size
	var recordsBuf []byte

	// 상대적 오프셋과 타임스탬프 델타 계산을 위한 기준점
	baseTimestamp := b.firstTimestamp

	for i, r := range b.records {
		recordsBuf = append(recordsBuf, encodeRecord(i, baseTimestamp, r.key, r.value)...)
	}

	// 2. Prepare Header (61 bytes)
	header := make([]byte, 61)

	totalSize := 61 + len(recordsBuf)
	batchLength := int32(totalSize - 12) // BaseOffset(8) + BatchLength(4) 제외

	// [Offset 0-7] BaseOffset (0 for now)
	binary.BigEndian.PutUint64(header[0:8], 0)

	// [Offset 8-11] BatchLength
	binary.BigEndian.PutUint32(header[8:12], uint32(batchLength))

	// [Offset 12-15] PartitionLeaderEpoch (0)
	binary.BigEndian.PutUint32(header[12:16], 0)

	// [Offset 16] Magic (Must be 2)
	header[16] = 2

	// [Offset 17-20] CRC (Will fill later)

	// [Offset 21-22] Attributes (0)
	binary.BigEndian.PutUint16(header[21:23], 0)

	// [Offset 23-26] LastOffsetDelta
	binary.BigEndian.PutUint32(header[23:27], uint32(len(b.records)-1))

	// [Offset 27-34] BaseTimestamp
	binary.BigEndian.PutUint64(header[27:35], uint64(baseTimestamp))

	// [Offset 35-42] MaxTimestamp (Same as base for now)
	binary.BigEndian.PutUint64(header[35:43], uint64(baseTimestamp))

	// [Offset 43-50] ProducerId (-1)
	binary.BigEndian.PutUint64(header[43:51], ^uint64(0)) // -1

	// [Offset 51-52] ProducerEpoch (-1)
	binary.BigEndian.PutUint16(header[51:53], ^uint16(0)) // -1

	// [Offset 53-56] BaseSequence (-1)
	binary.BigEndian.PutUint32(header[53:57], ^uint32(0)) // -1

	// [Offset 57-60] RecordsCount
	binary.BigEndian.PutUint32(header[57:61], uint32(len(b.records)))

	// 3. Combine Header + Records
	fullBatch := append(header, recordsBuf...)

	// 4. Calculate CRC (Covering from Attributes(21) to End)
	crc := crc32.Checksum(fullBatch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(fullBatch[17:21], crc)

	return fullBatch
}

// encodeRecord encodes a single record into Kafka v2 format.
// Format: [Length(varint)] [Attributes(1)] [TimestampDelta(varint)] [OffsetDelta(varint)] [KeyLen(varint)] [Key] [ValLen(varint)] [Value] [Headers(varint)]
func encodeRecord(deltaOffset int, baseTimestamp int64, key, value []byte) []byte {
	// Body Buffer
	var body []byte
	var buf [10]byte // varint buffer

	// Attributes (0)
	body = append(body, 0)

	// TimestampDelta (0 for simplicity)
	n := binary.PutVarint(buf[:], 0)
	body = append(body, buf[:n]...)

	// OffsetDelta
	n = binary.PutVarint(buf[:], int64(deltaOffset))
	body = append(body, buf[:n]...)

	// Key Length & Key
	if key == nil {
		n = binary.PutVarint(buf[:], -1)
		body = append(body, buf[:n]...)
	} else {
		n = binary.PutVarint(buf[:], int64(len(key)))
		body = append(body, buf[:n]...)
		body = append(body, key...)
	}

	// Value Length & Value
	if value == nil {
		n = binary.PutVarint(buf[:], -1)
		body = append(body, buf[:n]...)
	} else {
		n = binary.PutVarint(buf[:], int64(len(value)))
		body = append(body, buf[:n]...)
		body = append(body, value...)
	}

	// Headers Count (0)
	n = binary.PutVarint(buf[:], 0)
	body = append(body, buf[:n]...)

	// Total Record Length (varint) + Body
	recLen := int64(len(body))
	n = binary.PutVarint(buf[:], recLen)

	// Final Result: [Length] + [Body]
	finalRecord := make([]byte, n+len(body))
	copy(finalRecord, buf[:n])
	copy(finalRecord[n:], body)

	return finalRecord
}
