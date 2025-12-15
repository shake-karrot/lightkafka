package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 100만 개 메시지 전송 테스트
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("value-data-%d", i))

		// 1. 요청 패킷 생성
		// [KeyLen(4)][ValLen(4)][Key][Val]
		buf := make([]byte, 8+len(key)+len(val))

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(len(val)))
		copy(buf[8:], key)
		copy(buf[8+len(key):], val)

		// 2. 전송
		start := time.Now()
		if _, err := conn.Write(buf); err != nil {
			panic(err)
		}

		// 3. 응답 수신 (Offset 8byte)
		resp := make([]byte, 8)
		if _, err := conn.Read(resp); err != nil {
			panic(err)
		}
		offset := binary.LittleEndian.Uint64(resp)

		fmt.Printf("[Sent] Offset: %d (latency: %v)\n", offset, time.Since(start))
	}
}
