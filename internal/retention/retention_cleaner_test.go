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
		RetentionMs:       1000,
		RetentionBytes:    -1,
		FileDelayDeleteMs: 0,
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
	rc := NewRetentionCleaner(CleanerConfig{RetentionCheckIntervalMs: 50})
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

	rc := NewRetentionCleaner(CleanerConfig{RetentionCheckIntervalMs: 50})
	rc.Register(p)

	if len(rc.partitions) != 1 {
		t.Errorf("expected 1 partition, got %d", len(rc.partitions))
	}
}

func TestRetentionCleaner_Integration_RetentionMs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_integration_ms_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = 100
	cfg.FileDelayDeleteMs = 0

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	oldTimestamp := time.Now().UnixMilli() - 500
	for i := 0; i < 3; i++ {
		batch := createTestBatch(oldTimestamp)
		if _, err := p.Append(batch); err != nil {
			t.Fatal(err)
		}
	}

	newTimestamp := time.Now().UnixMilli()
	batch := createTestBatch(newTimestamp)
	if _, err := p.Append(batch); err != nil {
		t.Fatal(err)
	}

	segmentsBefore := len(p.Segments)
	if segmentsBefore <= 1 {
		t.Skip("not enough segments rolled for this test")
	}

	rc := NewRetentionCleaner(CleanerConfig{RetentionCheckIntervalMs: 50})
	rc.Register(p)
	rc.Start()

	time.Sleep(150 * time.Millisecond)
	rc.Stop()

	time.Sleep(50 * time.Millisecond)

	segmentsAfter := len(p.Segments)
	if segmentsAfter >= segmentsBefore {
		t.Errorf("expected segments to be deleted: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}

	partDir := filepath.Join(tmpDir, "test-0")
	files, _ := os.ReadDir(partDir)
	t.Logf("segments before: %d, after: %d, files remaining: %d", segmentsBefore, segmentsAfter, len(files))
}

func TestRetentionCleaner_Integration_RetentionBytes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_integration_bytes_test")
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
	cfg.FileDelayDeleteMs = 0

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

	partDir := filepath.Join(tmpDir, "test-0")
	filesBefore, _ := os.ReadDir(partDir)
	countBefore := len(filesBefore)

	rc := NewRetentionCleaner(CleanerConfig{RetentionCheckIntervalMs: 50})
	rc.Register(p)
	rc.Start()

	time.Sleep(150 * time.Millisecond)
	rc.Stop()

	time.Sleep(50 * time.Millisecond)

	segmentsAfter := len(p.Segments)
	filesAfter, _ := os.ReadDir(partDir)
	countAfter := len(filesAfter)

	if segmentsAfter >= segmentsBefore {
		t.Errorf("expected segments to be deleted: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}

	if countAfter >= countBefore {
		t.Errorf("expected files to be deleted: before=%d, after=%d", countBefore, countAfter)
	}

	t.Logf("segments: %d->%d, files: %d->%d", segmentsBefore, segmentsAfter, countBefore, countAfter)
}

func TestRetentionCleaner_Integration_NoDeleteWhenDisabled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_integration_disabled_test")
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
	cfg.FileDelayDeleteMs = 0

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

	rc := NewRetentionCleaner(CleanerConfig{RetentionCheckIntervalMs: 50})
	rc.Register(p)
	rc.Start()

	time.Sleep(150 * time.Millisecond)
	rc.Stop()

	segmentsAfter := len(p.Segments)
	if segmentsAfter != segmentsBefore {
		t.Errorf("expected no segments to be deleted when retention disabled: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}
}

func TestRetentionCleaner_Integration_FilesActuallyDeleted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_files_deleted_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := resource.NewSegmentCache(10)
	defer cache.Close()

	cfg := testConfig()
	cfg.SegmentConfig.SegmentMaxBytes = 150
	cfg.RetentionMs = 50
	cfg.FileDelayDeleteMs = 0

	p, err := partition.NewPartition(tmpDir, "test", 0, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	oldTimestamp := time.Now().UnixMilli() - 500
	for i := 0; i < 4; i++ {
		batch := createTestBatch(oldTimestamp)
		if _, err := p.Append(batch); err != nil {
			t.Fatal(err)
		}
	}

	newTimestamp := time.Now().UnixMilli()
	batch := createTestBatch(newTimestamp)
	if _, err := p.Append(batch); err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(tmpDir, "test-0")
	filesBefore, _ := os.ReadDir(partDir)
	logFilesBefore := countLogFiles(filesBefore)

	if logFilesBefore <= 1 {
		t.Skip("not enough log files for this test")
	}

	rc := NewRetentionCleaner(CleanerConfig{RetentionCheckIntervalMs: 30})
	rc.Register(p)
	rc.Start()

	time.Sleep(200 * time.Millisecond)
	rc.Stop()

	time.Sleep(100 * time.Millisecond)

	filesAfter, _ := os.ReadDir(partDir)
	logFilesAfter := countLogFiles(filesAfter)

	if logFilesAfter >= logFilesBefore {
		t.Errorf("expected .log files to be deleted: before=%d, after=%d", logFilesBefore, logFilesAfter)
	}

	t.Logf("log files: %d -> %d (deleted %d)", logFilesBefore, logFilesAfter, logFilesBefore-logFilesAfter)
}

func countLogFiles(entries []os.DirEntry) int {
	count := 0
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".log" {
			count++
		}
	}
	return count
}
