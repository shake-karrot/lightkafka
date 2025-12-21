package client

import (
	"encoding/binary"
	"fmt"
)

// ParsedRecord is a human-readable representation of a Kafka record.
type ParsedRecord struct {
	Offset int64
	Key    string
	Value  string
}

// DecodeBatch parses the raw bytes of a RecordBatch and returns individual records.
func DecodeBatch(data []byte) ([]ParsedRecord, error) {
	if len(data) < 61 {
		return nil, fmt.Errorf("data too short for batch header")
	}

	// 1. Parse Batch Header
	baseOffset := int64(binary.BigEndian.Uint64(data[0:8]))
	batchLength := int32(binary.BigEndian.Uint32(data[8:12]))
	recordsCount := int32(binary.BigEndian.Uint32(data[57:61]))

	// Validation
	if int(batchLength)+12 > len(data) {
		return nil, fmt.Errorf("batch length mismatch")
	}

	// 2. Parse Records
	// Records start at offset 61
	offset := 61
	var records []ParsedRecord

	for i := 0; i < int(recordsCount); i++ {
		if offset >= len(data) {
			break
		}

		// [Record Length] (varint)
		recLen, n := binary.Varint(data[offset:])
		offset += n

		// Record Start Position
		startPos := offset

		// [Attributes] (1 byte)
		// attributes := data[offset]
		offset += 1

		// [TimestampDelta] (varint)
		_, n = binary.Varint(data[offset:])
		offset += n

		// [OffsetDelta] (varint)
		offsetDelta, n := binary.Varint(data[offset:])
		offset += n

		// [Key Length] (varint)
		keyLen, n := binary.Varint(data[offset:])
		offset += n

		var key string
		if keyLen >= 0 {
			key = string(data[offset : offset+int(keyLen)])
			offset += int(keyLen)
		}

		// [Value Length] (varint)
		valLen, n := binary.Varint(data[offset:])
		offset += n

		var value string
		if valLen >= 0 {
			value = string(data[offset : offset+int(valLen)])
			offset += int(valLen)
		}

		// [Headers] (varint count) - Skip for now
		// headersCount, n := binary.Varint(data[offset:])
		// offset += n

		// 실제로는 여기서 헤더 루프를 돌아야 하지만,
		// 우리가 만든 Builder는 헤더를 0개 넣으므로 일단 무시하거나
		// 다음 레코드 위치를 정확히 찾기 위해 `recLen`을 이용해 점프하는 게 안전함.

		// 안전한 다음 레코드 위치 계산:
		// 현재 레코드의 끝 = startPos + recLen
		nextRecordPos := startPos + int(recLen)
		offset = nextRecordPos

		records = append(records, ParsedRecord{
			Offset: baseOffset + offsetDelta, // 절대 오프셋 계산
			Key:    key,
			Value:  value,
		})
	}

	return records, nil
}
