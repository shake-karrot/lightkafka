package partition

import (
	"sync"
	"time"
)

type RetentionCleaner struct {
	mu         sync.Mutex
	partitions []*Partition
	interval   time.Duration
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewRetentionCleaner(interval time.Duration) *RetentionCleaner {
	return &RetentionCleaner{
		partitions: make([]*Partition, 0),
		interval:   interval,
		stopCh:     make(chan struct{}),
	}
}

func (rc *RetentionCleaner) Register(p *Partition) {
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

	ticker := time.NewTicker(rc.interval)
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
	partitions := make([]*Partition, len(rc.partitions))
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
