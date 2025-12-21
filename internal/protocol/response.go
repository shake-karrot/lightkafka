package protocol

import (
	"encoding/binary"
	"io"
)

// NOTE(Danu): Kafka Response Header v0 (CorrelationID only)
// NOTE(Danu): Structure: [Size(4)] + [CorrelationID(4)] + [Body...]
const (
	RESPONSE_HEADER_SIZE = CORRELATION_ID_SIZE
	CORRELATION_ID_SIZE  = 4

	FRAMING_SIZE = 4 //NOTE(Danu): Packet Size를 맨 앞에 고정크기로 두고 사용 (4 byte)
)

// NOTE(Danu): Memory Allocation을 줄이기 위해 Header+Framing은 스택 배열을 사용하고, Body는 복사 없이 io.Writer로 직접 씁니다.
// TODO(Danu): net.Buffers를 이용해서 더 효율적으로 처리할 수 있도록 수정해야 함
func SendResponse(w io.Writer, correlationID int32, body []byte) error {

	payloadSize := RESPONSE_HEADER_SIZE + len(body)

	// NOTE(Danu): make([]byte, 8) 대신 배열을 사용하여 Heap 할당 방지 (Escape Analysis에 유리)
	var headerBuf [FRAMING_SIZE + RESPONSE_HEADER_SIZE]byte

	var offset = 0

	// NOTE(Danu): Packet Size 쓰기
	binary.BigEndian.PutUint32(headerBuf[offset:offset+FRAMING_SIZE], uint32(payloadSize))
	offset += FRAMING_SIZE

	// NOTE(Danu): Correlation ID 쓰기
	binary.BigEndian.PutUint32(headerBuf[offset:offset+CORRELATION_ID_SIZE], uint32(correlationID))
	offset += CORRELATION_ID_SIZE

	// NOTE(Danu): Write IO를 이용해서 헤더만 먼저 복사
	if _, err := w.Write(headerBuf[:]); err != nil {
		return err
	}

	// NOTE(Danu): Write Body (Zero-Copy), Body가 있다면 io.Writer에 직접 씁니다.
	// NOTE(Danu): 해당 정보는 mmap을 이용해서 메모리에 매핑된 데이터를 씁니다.
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}

	return nil
}
