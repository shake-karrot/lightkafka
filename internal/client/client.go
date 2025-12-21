package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"lightkafka/internal/message"
	"lightkafka/internal/protocol"
)

type Config struct {
	BrokerAddr string
	ClientID   string
}

type Client struct {
	Config Config
	conn   net.Conn
}

func NewClient(cfg Config) (*Client, error) {
	conn, err := net.DialTimeout("tcp", cfg.BrokerAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{Config: cfg, conn: conn}, nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// Produce sends a RecordBatch to the broker.
func (c *Client) Produce(batch *message.RecordBatch) (int64, error) {
	// 1. Prepare Request Body (RecordBatch Bytes)
	// (실제로는 여기서 RecordBatch를 인코딩해야 하지만,
	// 지금은 이미 []byte로 된 Payload가 있다고 가정하거나 단순화함)
	// 테스트를 위해 batch.Payload를 그대로 씁니다.
	reqBody := batch.Payload

	// 2. Send Request
	if err := c.sendRequest(protocol.ApiKeyProduce, reqBody); err != nil {
		return 0, err
	}

	// 3. Read Response (Offset 8 bytes)
	respBody, err := c.readResponse()
	if err != nil {
		return 0, err
	}

	if len(respBody) < 8 {
		return 0, fmt.Errorf("invalid response size: %d", len(respBody))
	}

	offset := int64(binary.BigEndian.Uint64(respBody))
	return offset, nil
}

// Fetch requests data from the broker.
func (c *Client) Fetch(offset int64, maxBytes int32) ([]byte, error) {
	// 1. Prepare Request Body: [Offset(8)] + [MaxBytes(4)]
	reqBody := make([]byte, 12)
	binary.BigEndian.PutUint64(reqBody[0:8], uint64(offset))
	binary.BigEndian.PutUint32(reqBody[8:12], uint32(maxBytes))

	// 2. Send Request
	if err := c.sendRequest(protocol.ApiKeyFetch, reqBody); err != nil {
		return nil, err
	}

	// 3. Read Response (Raw RecordBatch Data)
	return c.readResponse()
}

// sendRequest encodes and writes the request packet.
func (c *Client) sendRequest(apiKey int16, body []byte) error {
	// Header + Body
	// Request Header v1: ApiKey(2)+Ver(2)+CorrID(4)+ClientIDLen(2)+ClientIDStr

	// 편의상 ClientID 처리를 포함한 패킷 생성
	clientIDLen := len(c.Config.ClientID)
	headerSize := 2 + 2 + 4 + 2 + clientIDLen

	totalSize := headerSize + len(body)

	// Buffer Allocation (Client는 성능 덜 중요하므로 그냥 make)
	buf := make([]byte, 4+totalSize) // 4 is for Framing Size

	// 1. Framing Size (4 bytes)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalSize))

	// 2. Header
	offset := 4
	binary.BigEndian.PutUint16(buf[offset:], uint16(apiKey)) // ApiKey
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:], 0) // ApiVersion (v0)
	offset += 2
	binary.BigEndian.PutUint32(buf[offset:], 1) // CorrelationID (Fixed 1)
	offset += 4
	binary.BigEndian.PutUint16(buf[offset:], uint16(clientIDLen)) // ClientID Len
	offset += 2
	copy(buf[offset:], c.Config.ClientID) // ClientID Body
	offset += clientIDLen

	// 3. Body
	copy(buf[offset:], body)

	// 4. Write to Socket
	_, err := c.conn.Write(buf)
	return err
}

// readResponse reads the framed response packet.
func (c *Client) readResponse() ([]byte, error) {
	// 1. Read Size (4 bytes)
	var sizeBuf [4]byte
	if _, err := io.ReadFull(c.conn, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))

	// 2. Read Packet (Header + Body)
	data := make([]byte, size)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return nil, err
	}

	// 3. Parse Header (Response v0: CorrelationID 4 bytes)
	if len(data) < 4 {
		return nil, fmt.Errorf("response too short")
	}
	// correlationID := binary.BigEndian.Uint32(data[0:4])

	// 4. Return Body
	return data[4:], nil
}
