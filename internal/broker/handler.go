package broker

import (
	"encoding/binary"
	"fmt"

	"lightkafka/internal/protocol"
)

const (
	PRODUCE_RESPONSE_BODY_SIZE = 8 //NOTE(Danu): OFFSET의 크기는 8바이트

	FETCH_REQUEST_BODY_SIZE = 12 //NOTE(Danu): OFFSET(8) + MAX_BYTES(4)
)

func (b *Broker) handleRequest(req *protocol.Request) ([]byte, error) {
	switch req.Header.ApiKey {
	case protocol.ApiKeyProduce:
		return b.handleProduce(req)
	case protocol.ApiKeyFetch:
		return b.handleFetch(req)
	default:
		return nil, fmt.Errorf("unknown api key: %d", req.Header.ApiKey)
	}
}

func (b *Broker) handleProduce(req *protocol.Request) ([]byte, error) {

	//NOTE(Danu): Bytepool에 할당된 메모리가 바로  mmap으로 복사됨
	offset, err := b.Partition.Append(req.Body)
	if err != nil {
		return nil, err
	}

	// NOTE(Danu): OFFSET의 크기는 8바이트
	resp := make([]byte, PRODUCE_RESPONSE_BODY_SIZE)
	binary.BigEndian.PutUint64(resp, uint64(offset))

	return resp, nil
}

func (b *Broker) handleFetch(req *protocol.Request) ([]byte, error) {

	if len(req.Body) < FETCH_REQUEST_BODY_SIZE {
		return nil, fmt.Errorf("invalid fetch body size")
	}

	fetchOffset := int64(binary.BigEndian.Uint64(req.Body[0:8]))
	maxBytes := int32(binary.BigEndian.Uint32(req.Body[8:12]))

	// NOTE(Danu): mmap pointer를 반환하여 메모리에 매핑된 데이터를 읽음
	data, err := b.Partition.Read(fetchOffset, maxBytes)
	if err != nil {

		fmt.Printf("[Broker] Read error (offset %d): %v\n", fetchOffset, err)
		return []byte{}, nil
	}

	if data == nil {
		return []byte{}, nil
	}

	return data, nil
}
