package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"lightkafka/internal/broker"
	"lightkafka/internal/partition"
	"lightkafka/internal/resource"
	"lightkafka/internal/segment"
)

func main() {
	segConfig := segment.Config{
		SegmentMaxBytes:    10 * 1024 * 1024, // 10MB per segment
		IndexMaxBytes:      100 * 1024,       // 100KB index
		BaseDir:            "./data",         // Data directory
		IndexIntervalBytes: 4 * 1024,         // 4KB - index every 4KB of log data
	}

	partitionConfig := partition.PartitionConfig{
		SegmentConfig: segConfig,
	}

	listenAddr := ":9092" // Kafka Standard Port

	fmt.Println("[Init] Initializing Resource Cache...")
	resCache := resource.NewSegmentCache(50)
	defer resCache.Close()

	fmt.Println("[Init] Initializing Partition Storage...")
	p, err := partition.NewPartition(segConfig.BaseDir, "events", 0, partitionConfig, resCache)
	if err != nil {
		log.Fatalf("Failed to initialize partition: %v", err)
	}
	defer p.Close()

	brk := broker.NewBroker(broker.Config{ListenAddr: listenAddr}, p)

	go func() {
		if err := brk.Start(); err != nil {
			log.Fatalf("Broker failed to start: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n[Main] Shutting down broker...")
	brk.Stop()
	fmt.Println("[Main] Broker stopped. Bye!")
}
