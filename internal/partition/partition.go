package partition

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"lightkafka/internal/resource" // Import Resource
	"lightkafka/internal/segment"
)

// Partition manages a sequential list of segments.
// It uses a global LRU cache for read-only segments to save file descriptors.
type Partition struct {
	mu    sync.RWMutex
	Dir   string
	Topic string
	ID    int

	// Segments stores the BaseOffsets of all segments in this partition.
	// We only store offsets here (metadata), not the File handles.
	Segments []int64

	// activeSegment is the current segment being written to.
	// It is always kept open and NOT managed by the LRU cache.
	activeSegment *segment.Segment

	// cache is the shared global resource manager for read-only segments.
	cache *resource.SegmentCache

	Config PartitionConfig
}

// NewPartition creates or recovers a partition instance.
func NewPartition(
	baseDir string,
	topic string,
	id int,
	c PartitionConfig,
	resCache *resource.SegmentCache, // [DI] Global Cache Injection
) (*Partition, error) {

	// Directory: {baseDir}/{topic}-{id}
	partDir := filepath.Join(baseDir, fmt.Sprintf("%s-%d", topic, id))
	if err := os.MkdirAll(partDir, 0755); err != nil {
		return nil, err
	}

	p := &Partition{
		Dir:      partDir,
		Topic:    topic,
		ID:       id,
		Config:   c,
		Segments: make([]int64, 0),
		cache:    resCache,
	}

	// Scan Segments (Metadata only)
	// We don't open files here to ensure fast startup.
	if err := p.scanSegments(); err != nil {
		return nil, err
	}

	// 2. Initialize Active Segment
	// The active segment must be loaded directly to ensure write availability.
	if len(p.Segments) == 0 {
		// Case A: New Partition -> Create 0 offset segment
		seg, err := segment.NewSegment(p.Dir, 0, c.SegmentConfig)
		if err != nil {
			return nil, err
		}
		p.Segments = append(p.Segments, 0)
		p.activeSegment = seg
	} else {
		// Case B: Recovering -> Load the last segment as Active
		lastOffset := p.Segments[len(p.Segments)-1]
		seg, err := segment.NewSegment(p.Dir, lastOffset, c.SegmentConfig)
		if err != nil {
			return nil, err
		}
		p.activeSegment = seg
	}

	return p, nil
}

/* scanSegments */
func (p *Partition) scanSegments() error {
	entries, err := os.ReadDir(p.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Only look for .log files (.index is handled by segment)
		if strings.HasSuffix(name, ".log") {
			prefix := strings.TrimSuffix(name, ".log")
			baseOffset, err := strconv.ParseInt(prefix, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid log filename: %s", name)
			}
			p.Segments = append(p.Segments, baseOffset)
		}
	}

	// Sort by BaseOffset ASC
	sort.Slice(p.Segments, func(i, j int) bool {
		return p.Segments[i] < p.Segments[j]
	})

	return nil
}

// Append writes a batch to the active segment.
// It handles segment rolling if the current one is full.
func (p *Partition) Append(batchBytes []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	currentOffset := p.activeSegment.NextOffset

	// 배치 데이터의 맨 앞 8바이트(BaseOffset)를 실제 오프셋으로 덮어씀
	if len(batchBytes) >= 8 {
		binary.BigEndian.PutUint64(batchBytes[0:8], uint64(currentOffset))
	} else {
		return 0, fmt.Errorf("invalid batch data length: %d", len(batchBytes))
	}

	// 1. Try to append to the active segment
	offset, err := p.activeSegment.Append(batchBytes)

	// 2. Handle Segment Rolling
	if err == segment.ErrSegmentFull {
		// 롤링 할 때도 NextOffset은 보존됨
		nextOffset := p.activeSegment.NextOffset

		if err := p.activeSegment.Close(); err != nil {
			return 0, err
		}

		fmt.Printf("[Partition %d] Rolling segment: BaseOffset %d -> New %d\n", p.ID, p.activeSegment.BaseOffset, nextOffset)

		// 새 세그먼트 생성
		newSeg, err := segment.NewSegment(p.Dir, nextOffset, p.Config.SegmentConfig)
		if err != nil {
			return 0, err
		}

		p.activeSegment = newSeg

		return p.activeSegment.Append(batchBytes)
	}

	return offset, err
}

// Read routes the read request to the correct segment (Active or Cached).
func (p *Partition) Read(offset int64, maxBytes int32) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 1. Validate range
	if len(p.Segments) == 0 {
		return nil, segment.ErrOffsetOutOfRange
	}
	if offset < p.Segments[0] {
		return nil, segment.ErrOffsetOutOfRange
	}
	if offset >= p.activeSegment.NextOffset {
		return nil, nil // No new data available (EOF)
	}

	// 2. Fast Path: Read from Active Segment
	// If the offset is in the active segment, we read directly without cache overhead.
	if offset >= p.activeSegment.BaseOffset {
		return p.activeSegment.Read(offset, maxBytes)
	}

	// 3. Read-Only Path: Find the correct old segment
	// Binary search to find the segment that contains the offset.
	// We look for the largest BaseOffset that is <= offset.
	idx := sort.Search(len(p.Segments), func(i int) bool {
		return p.Segments[i] > offset
	}) - 1

	if idx < 0 {
		idx = 0
	}

	targetBaseOffset := p.Segments[idx]

	cacheKey := fmt.Sprintf("%s-%d-%d", p.Topic, p.ID, targetBaseOffset)

	loader := func() (*segment.Segment, error) {
		return segment.NewSegment(p.Dir, targetBaseOffset, p.Config.SegmentConfig)
	}

	seg, err := p.cache.GetOrLoad(cacheKey, loader)
	if err != nil {
		return nil, err
	}

	// 5. Read data
	return seg.Read(offset, maxBytes)
}

/* Close */
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeSegment != nil {
		if err := p.activeSegment.Close(); err != nil {
			return err
		}
	}
	return nil
}
