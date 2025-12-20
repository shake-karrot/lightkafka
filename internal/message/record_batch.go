package message

import (
	"errors"
	"fmt"
	"hash/crc32"

	"lightkafka/pkg"
)

var (
	ErrInsufficientData = errors.New("insufficient data to decode record batch")
	ErrInvalidMagic     = errors.New("invalid magic byte (expected 2)")
	ErrCRCMismatch      = errors.New("crc mismatch")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	BATCH_HEADER_SIZE         = 61
	BATCH_LENTH_METADATA_SIZE = BATCH_OFFEST_SIZE + BATCH_LENGTH_SIZE
	BATCH_OFFEST_SIZE         = 8
	BATCH_LENGTH_SIZE         = 4
)

// BatchHeader represents the fixed-size header of a Kafka RecordBatch (v2).
// 61 Bytes fixed header.
type BatchHeader struct {
	BaseOffset           int64 /* 8 bytes */
	BatchLength          int32 /* 4 bytes */
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32 /* 4 bytes */
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsCount         int32
}

// RecordBatch wraps the raw data and the parsed header.
type RecordBatch struct {
	Header  BatchHeader
	Payload []byte // Raw bytes of records (Zero-Copy slice)
}

// DecodeBatch parses the batch header strictly.
func DecodeBatch(data []byte) (*RecordBatch, error) {
	if len(data) < 61 {
		return nil, ErrInsufficientData
	}

	h := BatchHeader{}
	h.BaseOffset = int64(pkg.Encod.Uint64(data[0:8]))
	h.BatchLength = int32(pkg.Encod.Uint32(data[8:12]))

	// Validation: Check if we have the full batch data
	if int64(len(data)) < int64(h.BatchLength)+12 {
		return nil, ErrInsufficientData
	}

	h.PartitionLeaderEpoch = int32(pkg.Encod.Uint32(data[12:16]))
	h.Magic = int8(data[16])
	if h.Magic != 2 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidMagic, h.Magic)
	}

	h.CRC = pkg.Encod.Uint32(data[17:21])
	h.Attributes = int16(pkg.Encod.Uint16(data[21:23]))
	h.LastOffsetDelta = int32(pkg.Encod.Uint32(data[23:27]))
	h.BaseTimestamp = int64(pkg.Encod.Uint64(data[27:35]))
	h.MaxTimestamp = int64(pkg.Encod.Uint64(data[35:43]))
	h.ProducerId = int64(pkg.Encod.Uint64(data[43:51]))
	h.ProducerEpoch = int16(pkg.Encod.Uint16(data[51:53]))
	h.BaseSequence = int32(pkg.Encod.Uint32(data[53:57]))
	h.RecordsCount = int32(pkg.Encod.Uint32(data[57:61]))

	// Payload starts after the header (61 bytes)
	// If compressed, this is the compressed data.
	payloadEnd := 12 + int(h.BatchLength)

	calcCRC := crc32.Checksum(data[21:], crcTable)
	if calcCRC != h.CRC {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrCRCMismatch, h.CRC, calcCRC)
	}

	return &RecordBatch{
		Header:  h,
		Payload: data[61:payloadEnd], // Zero-Copy slicing
	}, nil
}

// Encode is a placeholder. For a broker, we usually just append raw bytes.
// If you are producing, you need a Builder.
func (b *RecordBatch) Size() int {
	return 12 + int(b.Header.BatchLength)
}
