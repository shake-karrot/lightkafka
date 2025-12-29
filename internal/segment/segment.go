package segment

import (
	"fmt"
	"path/filepath"
	"sync"

	"lightkafka/internal/message"
	"lightkafka/pkg"
)

type Segment struct {
	mu               sync.RWMutex
	BaseOffset       int64
	NextOffset       int64
	LargestTimestamp int64 // max timestamp in this segment (ms)

	log    *Log
	index  *Index
	config Config
}

func NewSegment(dir string, baseOffset int64, c Config) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	l, err := NewLog(logPath, c.SegmentMaxBytes)
	if err != nil {
		return nil, err
	}

	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	idx, err := NewIndex(idxPath, c.IndexMaxBytes)
	if err != nil {
		l.Close()
		return nil, err
	}

	s := &Segment{
		BaseOffset: baseOffset,
		log:        l,
		index:      idx,
		config:     c,
	}

	if err := s.recover(); err != nil {
		s.Close()
		return nil, err
	}

	return s, nil
}

func (s *Segment) Append(batchBytes []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch, err := message.DecodeBatch(batchBytes)
	if err != nil {
		return 0, err
	}

	n, pos, err := s.log.Append(batchBytes)
	if err != nil {
		return 0, err
	}

	// Sparse Indexing: Write index for every batch (simplification)
	// Real Kafka writes every 4KB
	relOffset := int32(batch.Header.BaseOffset - s.BaseOffset)
	if n > 0 {
		_ = s.index.Write(relOffset, int32(pos))
	}

	if batch.Header.MaxTimestamp > s.LargestTimestamp {
		s.LargestTimestamp = batch.Header.MaxTimestamp
	}

	curr := s.NextOffset
	s.NextOffset += int64(batch.Header.RecordsCount)
	return curr, nil
}

// Read finds the exact batch and returns a chunk filled with batches.
func (s *Segment) Read(targetOffset int64, maxBytes int32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if targetOffset < s.BaseOffset || targetOffset >= s.NextOffset {
		return nil, ErrOffsetOutOfRange
	}

	// 1. Index Lookup
	rel := int32(targetOffset - s.BaseOffset)
	startPos, err := s.index.Lookup(rel)
	if err != nil {
		return nil, err
	}

	// 2. Linear Scan (Correct Position)
	currentPos := startPos
	found := false

	for currentPos < s.log.Size() {
		// Read 61 bytes header to check LastOffsetDelta
		headerBytes, err := s.log.ReadRaw(currentPos, 61)
		if err != nil {
			break
		}

		baseOffset := int64(pkg.Encod.Uint64(headerBytes[0:8]))
		batchLen := int32(pkg.Encod.Uint32(headerBytes[8:12]))
		lastOffsetDelta := int32(pkg.Encod.Uint32(headerBytes[23:27]))

		totalSize := 12 + int64(batchLen)
		lastOffset := baseOffset + int64(lastOffsetDelta)

		// Skip if this batch is completely before targetOffset
		if lastOffset < targetOffset {
			currentPos += totalSize
			continue
		}

		// Found the batch containing targetOffset (or the first one after it)
		found = true
		break
	}

	if !found {
		return nil, ErrOffsetOutOfRange
	}

	// 3. Fetch Data
	return s.log.ReadAt(currentPos, maxBytes)
}

// recover rebuilds state (NextOffset, Log Size) by scanning the log.
func (s *Segment) recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Get hints from index
	_, lastPos, _ := s.index.LastEntry()
	if int64(lastPos) > s.log.Size() {
		lastPos = 0
	}

	// 2. Scan log to verify data integrity
	currentPos := int64(lastPos)
	var lastNextOffset int64 = s.BaseOffset

	for currentPos < s.log.configSize() { // note: check physical size
		// Try reading header
		header, err := s.log.ReadRaw(currentPos, 12)
		if err != nil || len(header) < 12 {
			break
		}

		batchLen := int32(pkg.Encod.Uint32(header[8:12]))
		if batchLen == 0 {
			// Found zero-padding (pre-allocated space)
			break
		}

		totalSize := 12 + int64(batchLen)

		batchData, err := s.log.ReadRaw(currentPos, int(totalSize))
		if err != nil || len(batchData) < int(totalSize) {
			break
		}

		batch, err := message.DecodeBatch(batchData)
		if err != nil {
			break
		}

		lastNextOffset = batch.Header.BaseOffset + int64(batch.Header.RecordsCount)
		if batch.Header.MaxTimestamp > s.LargestTimestamp {
			s.LargestTimestamp = batch.Header.MaxTimestamp
		}
		currentPos += totalSize
	}

	// 3. Restore State
	s.NextOffset = lastNextOffset
	s.log.SetSize(currentPos)

	fmt.Printf("Recovered Segment %d: NextOffset=%d, ValidSize=%d\n", s.BaseOffset, s.NextOffset, currentPos)
	return nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.index.Close()
	_ = s.log.Close()
	return nil
}

func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.Size()
}

func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.index.Delete(); err != nil {
		return err
	}
	return s.log.Delete()
}
