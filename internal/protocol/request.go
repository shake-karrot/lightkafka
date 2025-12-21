package protocol

import (
	"encoding/binary"
	"io"
)

const (
	MAX_REQUEST_SIZE = 100 * 1024 * 1024

	/* NOTE(Danu): Kafka RequestHeader v1애 명시된 고정 크기 */
	FIXED_REQUEST_HEADER_SIZE   = REQUEST_API_KEY_SIZE + REQUEST_API_VERSION_SIZE + REQUEST_CORRELATION_ID_SIZE
	REQUEST_API_KEY_SIZE        = 2
	REQUEST_API_VERSION_SIZE    = 2
	REQUEST_CORRELATION_ID_SIZE = 4
	REQUEST_CLIENT_ID_SIZE      = 2
)

const (
	ApiKeyProduce = 0
	ApiKeyFetch   = 1
)

// NOTE(Danu): Kafka Request Header (RequestHeader v1)
type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string // NOTE(Danu): 파싱해서 저장만 하고 현재 사용하지는 않음
}

type Request struct {
	Size      int32 // Note(Danu): 실제 Header에는 없지만, TCP 규약에 따라 앞의 4Byte는 무조건 패킷의 길이
	Header    RequestHeader
	Body      []byte
	rawBuffer *[]byte // NOTE(Danu): Sync Pool에 반납하기 위한 포인터
}

// NOTE(Danu): request 정보를 사용한 후 반납하기 위한 함수, 반드시 처리 후 호출해야 함
func (r *Request) Release() {
	if r.rawBuffer != nil {
		PutBuffer(r.rawBuffer)
		r.rawBuffer = nil
	}
}

func ReadRequest(r io.Reader) (*Request, error) {

	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))

	if size <= 0 || size > MAX_REQUEST_SIZE {
		return nil, ErrInvalidRequestSize
	}

	bufPtr := GetBufferWithCapacity(int(size))
	packet := *bufPtr

	if _, err := io.ReadFull(r, packet); err != nil {
		PutBuffer(bufPtr)
		return nil, err
	}

	if len(packet) < FIXED_REQUEST_HEADER_SIZE+REQUEST_CLIENT_ID_SIZE {
		PutBuffer(bufPtr)
		return nil, ErrPacketTooShort
	}

	offset := 0
	apiKey := int16(binary.BigEndian.Uint16(packet[offset:]))
	offset += REQUEST_API_KEY_SIZE
	apiVersion := int16(binary.BigEndian.Uint16(packet[offset:]))
	offset += REQUEST_API_VERSION_SIZE
	correlationID := int32(binary.BigEndian.Uint32(packet[offset:]))
	offset += REQUEST_CORRELATION_ID_SIZE
	clientIDLen := int16(binary.BigEndian.Uint16(packet[offset:]))
	offset += REQUEST_CLIENT_ID_SIZE

	var clientID string
	if clientIDLen != -1 {
		// NOTE(Danu): 남은 패킷 길이가 ClientID 길이보다 짧은지 검사
		if len(packet) < offset+int(clientIDLen) {
			PutBuffer(bufPtr)
			return nil, ErrPacketTooShort
		}

		clientID = string(packet[offset : offset+int(clientIDLen)])
		offset += int(clientIDLen)
	}

	header := RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID, // NOTE(Danu): 파싱된 ID 저장
	}

	return &Request{
		Size:      size,
		Header:    header,
		Body:      packet[offset:], //Body는 ClientID가 끝난 지점(offset)부터 시작
		rawBuffer: bufPtr,
	}, nil
}
