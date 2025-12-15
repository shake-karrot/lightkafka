package log

import (
	"errors"
	"fmt"
	"lightkafka/internal/record"
	"lightkafka/internal/store"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrPartitionNotFound = errors.New("partition not found")
	ErrSegmentOpen       = errors.New("segment open failed")
)

const DEFAULT_SEGMENT_MAX_BYTES = 1024 * 1024 * 1024
const DEFAULT_BASE_DIR = "/tmp/lightkafka/logs"

type PartitionConfig struct {
	SegmentMaxBytes int64
	BaseDir         string
}

type Partition struct {
	mu     sync.Mutex
	config PartitionConfig

	activeSegment *store.Segment
	segments      []*store.Segment
}

func NewPartition(config PartitionConfig) (*Partition, error) {

	if err := os.MkdirAll(config.BaseDir, 0755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(config.BaseDir)
	if err != nil {
		return nil, err
	}

	var fileBaseOffsets []uint64
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".log" {
			nameWithoutExt := strings.TrimSuffix(entry.Name(), ".log")
			offset, err := strconv.ParseUint(nameWithoutExt, 10, 64)
			if err == nil {
				fileBaseOffsets = append(fileBaseOffsets, offset)
			}
		}
	}

	/* Sort the file base offsets in ascending order */
	sort.Slice(fileBaseOffsets, func(i, j int) bool {
		return fileBaseOffsets[i] < fileBaseOffsets[j]
	})

	p := &Partition{
		config:   config,
		segments: make([]*store.Segment, 0),
	}

	//TODO : segment lazy loading need, LRU Cahce need to maintain fixed length of segments
	for _, off := range fileBaseOffsets {
		path := filepath.Join(config.BaseDir, fmt.Sprintf("%020d.log", off))
		seg, err := store.NewSegment(path, off, config.SegmentMaxBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to open segment %d: %w", off, err)
		}
		p.segments = append(p.segments, seg)
	}

	if len(p.segments) == 0 {
		path := filepath.Join(config.BaseDir, fmt.Sprintf("%020d.log", 0))
		seg, err := store.NewSegment(path, 0, config.SegmentMaxBytes)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, seg)
		p.activeSegment = seg
	} else {
		p.activeSegment = p.segments[len(p.segments)-1]
	}

	return p, nil

}

func (p *Partition) Append(record *record.Record) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeSegment.CanAppend(int(record.Size())) {
		if err := p.roll(); err != nil {
			return 0, err
		}
	}

	return p.activeSegment.Append(record)
}

func (p *Partition) roll() error {
	fmt.Println("Rolling new segment...")

	if err := p.activeSegment.Sync(); err != nil {
		return err
	}

	nextOffset := p.activeSegment.NextOffset()
	path := filepath.Join(p.config.BaseDir, fmt.Sprintf("%020d.log", nextOffset))

	newSeg, err := store.NewSegment(path, nextOffset, p.config.SegmentMaxBytes)
	if err != nil {
		return err
	}

	p.segments = append(p.segments, newSeg)
	p.activeSegment = newSeg

	return nil
}

/* TODO Indexting 관리 기능 필요*/
func (p *Partition) Read(offset uint64) (*record.Record, error) {
	return nil, errors.New("not implemented")
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}
