package segment

import (
	"fmt"
	"path/filepath"
	"sync"

	"lightkafka/internal/message"
	"lightkafka/pkg"
)

type Segment struct {
	mu         sync.RWMutex
	BaseOffset int64
	NextOffset int64

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

	// Sparse Indexing: Index first message or at intervals
	// Always index the first message in the segment for quick access
	if n > 0 && s.config.IndexIntervalBytes > 0 {
		_, lastPos, _ := s.index.LastEntry()
		if s.log.Size() == int64(n) || int64(pos)-int64(lastPos) >= s.config.IndexIntervalBytes {
			relOffset := int32(batch.Header.BaseOffset - s.BaseOffset)
			_ = s.index.Write(relOffset, int32(pos))
		}
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

// recover rebuilds state (NextOffset, Log Size) by scanning the log and reconstructing the index.
func (s *Segment) recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.SetSize(s.log.configSize())

	// 1. Index file integrity check (Sanity Check)
	validIndex := true
	lastIdxOff, lastIdxPos, err := s.index.LastEntry()
	if err != nil || int64(lastIdxPos) > s.log.Size() {
		validIndex = false
	}

	// 2. Determine recovery starting position
	var currentPos int64 = 0
	if validIndex && lastIdxPos > 0 {
		currentPos = int64(lastIdxPos)
	} else {
		if err := s.index.Truncate(0); err != nil {
			return err
		}
	}

	// 3. Log scanning
	var lastNextOffset int64 = s.BaseOffset

	var lastIndexedPos int64 = -1
	// If we started from a valid index position, set it as last indexed
	if validIndex && lastIdxPos > 0 {
		lastIndexedPos = int64(lastIdxPos)
		lastNextOffset = s.BaseOffset + int64(lastIdxOff)
	}

	for currentPos < s.log.configSize() {
		// Read header (12 bytes: Offset + Length)
		headerBuf, err := s.log.ReadRaw(currentPos, 12)
		if err != nil || len(headerBuf) < 12 {
			break
		}

		// Check for zero padding (pre-allocated empty space)
		batchLen := int32(pkg.Encod.Uint32(headerBuf[8:12]))
		if batchLen == 0 {
			break
		}

		totalBatchSize := 12 + int64(batchLen)

		batchData, err := s.log.ReadRaw(currentPos, int(totalBatchSize))
		if err != nil {
			break
		}

		batch, err := message.DecodeBatch(batchData)
		if err != nil {
			// CRC mismatch or format error - this is the end of valid data
			break
		}

		// Sparse indexing: index first batch or when interval exceeded
		// Always index the first batch for quick segment access
		reachedIndexThreshold := false
		if s.config.IndexIntervalBytes > 0 {
			if lastIndexedPos == -1 {
				reachedIndexThreshold = true
			} else if currentPos-lastIndexedPos >= s.config.IndexIntervalBytes {
				// Interval exceeded
				reachedIndexThreshold = true
			}
		}

		if reachedIndexThreshold {
			relOffset := int32(batch.Header.BaseOffset - s.BaseOffset)
			if err := s.index.Write(relOffset, int32(currentPos)); err != nil {
				return err
			}
			lastIndexedPos = currentPos
		}

		lastNextOffset = batch.Header.BaseOffset + int64(batch.Header.RecordsCount)
		currentPos += totalBatchSize
	}

	// 4. Truncate log to valid size
	// Remove invalid data (partially written data, zero-filled regions)
	s.log.SetSize(currentPos)
	s.NextOffset = lastNextOffset

	indexEntries := int64(0)
	if s.index.size > 0 {
		indexEntries = s.index.size / 8
	}

	fmt.Printf("Recovered Segment %d: NextOffset=%d, ValidBytes=%d, IndexEntries=%d\n",
		s.BaseOffset, s.NextOffset, currentPos, indexEntries)

	return nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.index.Close()
	_ = s.log.Close()
	return nil
}
