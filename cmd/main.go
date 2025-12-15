package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	internalLog "lightkafka/internal/log"
	"lightkafka/internal/record"
)

const (
	Dir     = "data/my-topic"
	SegSize = internalLog.DEFAULT_SEGMENT_MAX_BYTES
	Port    = ":8080"
)

func main() {

	cfg := internalLog.PartitionConfig{
		BaseDir:         Dir,
		SegmentMaxBytes: SegSize,
	}
	partition, err := internalLog.NewPartition(cfg)
	if err != nil {
		log.Fatalf("Failed to init partition: %v", err)
	}
	defer partition.Close()

	fmt.Printf("🔥 Kafka Engine Started! Storage: %s\n", Dir)

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
	}
	fmt.Printf("👂 Listening on %s...\n", Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}

		go handleConnection(conn, partition)
	}
}

func handleConnection(conn net.Conn, p *internalLog.Partition) {
	defer conn.Close()
	fmt.Println("New connection from:", conn.RemoteAddr())

	headerBuf := make([]byte, 8)

	for {

		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Read header error:", err)
			return
		}

		keyLen := binary.LittleEndian.Uint32(headerBuf[0:4])
		valLen := binary.LittleEndian.Uint32(headerBuf[4:8])

		bodyBuf := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(conn, bodyBuf); err != nil {
			log.Println("Read body error:", err)
			return
		}

		key := bodyBuf[:keyLen]
		val := bodyBuf[keyLen:]

		rec := record.Record{
			Key:   key,
			Value: val,
		}

		offset, err := p.Append(&rec)
		if err != nil {
			log.Println("Append error:", err)
			return
		}

		resp := make([]byte, 8)
		binary.LittleEndian.PutUint64(resp, offset)

		if _, err := conn.Write(resp); err != nil {
			log.Println("Write response error:", err)
			return
		}
	}
}
