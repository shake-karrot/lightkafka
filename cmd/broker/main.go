package main

import (
	"fmt"
	"lightkafka/internal/broker"
	"lightkafka/internal/partition"
	"lightkafka/internal/resource"
	"lightkafka/internal/retention"
	"lightkafka/internal/segment"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := broker.Config{
		ListenAddr: ":9092",
		BaseDir:    "./data",
		PartitionConfig: partition.PartitionConfig{
			SegmentConfig: segment.Config{
				SegmentMaxBytes: 10 * 1024 * 1024, // 10MB per segment
				IndexMaxBytes:   100 * 1024,       // 100KB index
			},
			RetentionMs:       7 * 24 * 60 * 60 * 1000, // 7 days
			RetentionBytes:    -1,                      // unlimited
			FileDelayDeleteMs: 60 * 1000,               // 60 seconds
		},
		CleanerConfig: retention.CleanerConfig{
			RetentionCheckIntervalMs: 5 * 60 * 1000, // 5 minutes
		},
	}

	fmt.Println("[Init] Initializing Resource Cache...")
	resCache := resource.NewSegmentCache(50)
	defer resCache.Close()

	fmt.Println("[Init] Initializing Partition Storage...")
	p, err := partition.NewPartition(cfg.BaseDir, "events", 0, cfg.PartitionConfig, resCache)
	if err != nil {
		log.Fatalf("Failed to initialize partition: %v", err)
	}
	defer p.Close()

	fmt.Println("[Init] Starting Retention Cleaner...")
	cleaner := retention.NewRetentionCleaner(cfg.CleanerConfig)
	cleaner.Register(p)
	cleaner.Start()
	defer cleaner.Stop()

	brk := broker.NewBroker(cfg, p)

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
