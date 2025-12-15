package record

const TOTAL_SIZE_BYTES = 4
const OFFSET_BYTES = 8
const CRC_BYTES = 4
const TIMESTAMP_BYTES = 8
const KEY_SIZE_BYTES = 4
const VALUE_SIZE_BYTES = 4

const HEADER_SIZE = TOTAL_SIZE_BYTES + OFFSET_BYTES + CRC_BYTES + TIMESTAMP_BYTES + KEY_SIZE_BYTES + VALUE_SIZE_BYTES

type Header struct {
	TotalSize uint32
	Offset    uint64
	Crc       uint32
	Timestamp int64
	KeySize   uint32
	ValueSize uint32
}

type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}
