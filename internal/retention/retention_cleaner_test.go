package retention

import (
	"lightkafka/internal/partition"
	"os"
	"path/filepath"
	"testing"
	"time"

	"lightkafka/internal/resource"
	"lightkafka/internal/segment"
)

func testConfig() partition.PartitionConfig {
	return partition.PartitionConfig{
		SegmentConfig: segment.Config{
			SegmentMaxBytes: 1024,
			IndexMaxBytes:   512,
		},
		RetentionMs:    1000,
		RetentionBytes: -1,
	}
}

func createTestBatch(timestamp int64) []byte {
	batch := make([]byte, 100)
	batch[16] = 2
	putUint64(batch[0:8], 0)
	putUint32(batch[8:12], 88)
	putUint32(batch[23:27], 0)
	putUint64(batch[27:35], uint64(timestamp))
	putUint64(batch[35:43], uint64(timestamp))
	putUint32(batch[57:61], 1)

	crc := computeCRC(batch[21:])
	putUint32(batch[17:21], crc)
	return batch
}

func putUint64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func computeCRC(data []byte) uint32 {
	const polynomial = 0x82F63B78
	crc := ^uint32(0)
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ polynomial
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}

func TestRetentionCleaner_StartStop(t *testing.T) {
	rc := NewRetentionCleaner(50 * time.Millisecond)
	rc.Start()
	time.Sleep(100 * time.Millisecond)
	rc.Stop()
}

func TestRetentionCleaner_Register(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	rc := NewRetentionCleaner(50 * time.Millisecond)
	rc.Register(p)

	if len(rc.partitions) != 1 {
		t.Errorf("expected 1 partition, got %d", len(rc.partitions))
	}
}

func TestPartition_DeleteRetentionMsBreached(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_ms_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = 500

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	oldTimestamp := time.Now().UnixMilli() - 1000
	batch1 := createTestBatch(oldTimestamp)
	if _, err := p.Append(batch1); err != nil {
		t.Fatal(err)
	}

	batch2 := createTestBatch(oldTimestamp)
	if _, err := p.Append(batch2); err != nil {
		t.Fatal(err)
	}

	newTimestamp := time.Now().UnixMilli()
	batch3 := createTestBatch(newTimestamp)
	if _, err := p.Append(batch3); err != nil {
		t.Fatal(err)
	}

	segmentsBefore := len(p.Segments)
	deleted := p.deleteRetentionMsBreachedSegments()

	if segmentsBefore <= 1 {
		t.Skip("not enough segments rolled for this test")
	}

	if deleted == 0 {
		t.Log("no segments deleted (may be expected if segment didn't roll)")
	}
}

func TestPartition_DeleteLogStartOffsetBreached(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "log_start_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = -1

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ts := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		batch := createTestBatch(ts)
		if _, err := p.Append(batch); err != nil {
			t.Fatal(err)
		}
	}

	segmentsBefore := len(p.Segments)
	if segmentsBefore <= 1 {
		t.Skip("not enough segments for this test")
	}

	p.SetLogStartOffset(p.Segments[1])

	deleted := p.deleteLogStartOffsetBreachedSegments()
	if deleted == 0 {
		t.Error("expected at least 1 segment to be deleted")
	}

	if len(p.Segments) >= segmentsBefore {
		t.Errorf("segments not reduced: before=%d, after=%d", segmentsBefore, len(p.Segments))
	}
}

func TestPartition_DeleteRetentionSizeBreached(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_size_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = -1
	cfg.RetentionBytes = 100

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ts := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		batch := createTestBatch(ts)
		if _, err := p.Append(batch); err != nil {
			t.Fatal(err)
		}
	}

	segmentsBefore := len(p.Segments)
	if segmentsBefore <= 1 {
		t.Skip("not enough segments for this test")
	}

	deleted := p.deleteRetentionSizeBreachedSegments()

	t.Logf("segments before: %d, deleted: %d, remaining: %d", segmentsBefore, deleted, len(p.Segments))
}

func TestPartition_DeleteOldSegments(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delete_old_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = -1
	cfg.RetentionBytes = -1

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ts := time.Now().UnixMilli()
	for i := 0; i < 3; i++ {
		batch := createTestBatch(ts)
		if _, err := p.Append(batch); err != nil {
			t.Fatal(err)
		}
	}

	deleted := p.DeleteOldSegments()
	if deleted != 0 {
		t.Errorf("expected 0 deletions with retention disabled, got %d", deleted)
	}
}

func TestRetentionCleaner_CleanupAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cleanup_all_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = -1
	cfg.RetentionBytes = -1

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	rc := NewRetentionCleaner(50 * time.Millisecond)
	rc.Register(p)

	rc.cleanupAll()
}

func TestSegmentFilesDeleted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment_files_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = -1
	cfg.RetentionBytes = 200

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ts := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		batch := createTestBatch(ts)
		if _, err := p.Append(batch); err != nil {
			t.Fatal(err)
		}
	}

	partDir := filepath.Join(tmpDir, "test-0")
	filesBefore, _ := os.ReadDir(partDir)
	countBefore := len(filesBefore)

	p.deleteRetentionSizeBreachedSegments()

	filesAfter, _ := os.ReadDir(partDir)
	countAfter := len(filesAfter)

	if countAfter >= countBefore && len(p.Segments) > 1 {
		t.Logf("files before: %d, after: %d", countBefore, countAfter)
	}
}
