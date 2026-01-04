package retention

import (
	"lightkafka/internal/partition"
	"sync"
	"time"
)

type CleanerConfig struct {
	RetentionCheckIntervalMs int64
}

type RetentionCleaner struct {
	mu         sync.Mutex
	partitions []*partition.Partition
	config     CleanerConfig
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewRetentionCleaner(config CleanerConfig) *RetentionCleaner {
	return &RetentionCleaner{
		partitions: make([]*partition.Partition, 0),
		config:     config,
		stopCh:     make(chan struct{}),
	}
}

func (rc *RetentionCleaner) Register(p *partition.Partition) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.partitions = append(rc.partitions, p)
}

func (rc *RetentionCleaner) Start() {
	rc.wg.Add(1)
	go rc.run()
}

func (rc *RetentionCleaner) run() {
	defer rc.wg.Done()

	interval := time.Duration(rc.config.RetentionCheckIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rc.cleanupAll()
		case <-rc.stopCh:
			return
		}
	}
}

func (rc *RetentionCleaner) cleanupAll() {
	rc.mu.Lock()
	partitions := make([]*partition.Partition, len(rc.partitions))
	copy(partitions, rc.partitions)
	rc.mu.Unlock()

	for _, p := range partitions {
		p.DeleteOldSegments()
	}
}

func (rc *RetentionCleaner) Stop() {
	close(rc.stopCh)
	rc.wg.Wait()
}
