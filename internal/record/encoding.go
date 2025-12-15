package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var ErrInsufficientBuffer = errors.New("buffer too small")
var ErrInvalidCRC = errors.New("invalid CRC")

func (r *Record) Size() uint32 {
	return HEADER_SIZE + uint32(len(r.Key)) + uint32(len(r.Value))
}

/**
 * Marshals the record to the destination buffer. (Destination buffer must allocate with mmap)
 * Returns the number of bytes written and an error if the buffer is too small.
 */
func (r *Record) MarshalTo(dest []byte) (int, error) {
	requiredSize := r.Size()
	if len(dest) < int(requiredSize) {
		return 0, ErrInsufficientBuffer
	}

	keyLength := uint32(len(r.Key))
	valueLength := uint32(len(r.Value))

	// Write Header
	// Total size
	binary.LittleEndian.PutUint32(dest[0:4], requiredSize)
	// Offset
	binary.LittleEndian.PutUint64(dest[4:12], r.Offset)
	// CRC
	binary.LittleEndian.PutUint32(dest[12:16], 0)
	// Timestamp
	binary.LittleEndian.PutUint64(dest[16:24], uint64(r.Timestamp))
	// Key size
	binary.LittleEndian.PutUint32(dest[24:28], keyLength)
	// Value size
	binary.LittleEndian.PutUint32(dest[28:32], valueLength)

	// Write Key
	copy(dest[32:32+keyLength], r.Key)

	// Write Value
	copy(dest[32+keyLength:32+keyLength+valueLength], r.Value)

	/* Checksum Offset to the end of the record */
	checksum := crc32.ChecksumIEEE(dest[16:requiredSize])
	binary.LittleEndian.PutUint32(dest[12:16], checksum)

	return int(requiredSize), nil
}

/**
 * Unmarshals the header from the source buffer and returns a Header struct.
 * Header does not include Pointer type, so it allocated on stack.
 * Pass by value means Escape Analysis will not allocate it on the heap.
 * zero allocation
 */
func UnmarshalHeader(source []byte) Header {
	return Header{
		TotalSize: binary.LittleEndian.Uint32(source[0:4]),
		Offset:    binary.LittleEndian.Uint64(source[4:12]),
		Crc:       binary.LittleEndian.Uint32(source[12:16]),
		Timestamp: int64(binary.LittleEndian.Uint64(source[16:24])),
		KeySize:   binary.LittleEndian.Uint32(source[24:28]),
		ValueSize: binary.LittleEndian.Uint32(source[28:32]),
	}
}

/**
 * Unmarshals the record into the record struct.
 * Zero-Copy body mapping. No actual data copy. (is feature of slice)
 */
func UnmarshalInto(src []byte, r *Record) error {
	if len(src) < HEADER_SIZE {
		return ErrInsufficientBuffer
	}

	h := UnmarshalHeader(src)

	calculatedCRC := crc32.ChecksumIEEE(src[16:h.TotalSize])
	if calculatedCRC != h.Crc {
		return ErrInvalidCRC
	}

	r.Offset = h.Offset
	r.Timestamp = h.Timestamp

	keyStart := HEADER_SIZE
	keyEnd := keyStart + int(h.KeySize)
	valEnd := keyEnd + int(h.ValueSize)

	if len(src) < valEnd {
		return ErrInsufficientBuffer
	}

	r.Key = src[keyStart:keyEnd]
	r.Value = src[keyEnd:valEnd]

	return nil
}
