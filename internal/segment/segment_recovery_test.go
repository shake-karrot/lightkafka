package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// createValidBatchBytes generates a valid batch binary matching the record_batch.go structure.
func createValidBatchBytes(baseOffset int64, recordsCount int32, payload []byte) []byte {
	buf := new(bytes.Buffer)

	// BatchLength = Total Header(61) - 12 + Payload Length = 49 + Payload
	batchLen := int32(49 + len(payload))

	// --- Batch Header (Part 1) ---
	binary.Write(buf, binary.BigEndian, baseOffset) // 1. BaseOffset (8)
	binary.Write(buf, binary.BigEndian, batchLen)   // 2. BatchLength (4)
	binary.Write(buf, binary.BigEndian, int32(0))   // 3. PartitionLeaderEpoch (4)
	binary.Write(buf, binary.BigEndian, int8(2))    // 4. Magic (1)

	// --- CRC Calculation Block ---
	crcBuf := new(bytes.Buffer)

	binary.Write(crcBuf, binary.BigEndian, int16(0))               // 6. Attributes (2)
	binary.Write(crcBuf, binary.BigEndian, int32(recordsCount-1))  // 7. LastOffsetDelta (4)
	binary.Write(crcBuf, binary.BigEndian, time.Now().UnixMilli()) // 8. BaseTimestamp (8)
	binary.Write(crcBuf, binary.BigEndian, time.Now().UnixMilli()) // 9. MaxTimestamp (8)
	binary.Write(crcBuf, binary.BigEndian, int64(-1))              // 10. ProducerId (8)
	binary.Write(crcBuf, binary.BigEndian, int16(-1))              // 11. ProducerEpoch (2)
	binary.Write(crcBuf, binary.BigEndian, int32(-1))              // 12. BaseSequence (4)
	binary.Write(crcBuf, binary.BigEndian, recordsCount)           // 13. RecordsCount (4)
	crcBuf.Write(payload)                                          // Payload

	// --- Final Assembly ---
	// 5. CRC (4)
	crc := crc32.Checksum(crcBuf.Bytes(), crc32.MakeTable(crc32.Castagnoli))
	binary.Write(buf, binary.BigEndian, crc)

	// Append the rest of the body
	buf.Write(crcBuf.Bytes())

	return buf.Bytes()
}

func TestSegment_Recovery_RebuildIndex(t *testing.T) {
	// 1. Setup
	dir := t.TempDir()
	cfg := Config{
		SegmentMaxBytes:    1024 * 1024,
		IndexMaxBytes:      1024 * 1024,
		IndexIntervalBytes: 10, // Force frequent indexing
	}
	baseOffset := int64(0)

	seg, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 2. Write data (3 batches)
	// Batch 1: Offset 0~9 (10 records)
	seg.Append(createValidBatchBytes(0, 10, []byte("payload-1")))

	// Batch 2: Offset 10~19 (10 records)
	seg.Append(createValidBatchBytes(10, 10, []byte("payload-2")))

	// Batch 3: Offset 20~24 (5 records)
	seg.Append(createValidBatchBytes(20, 5, []byte("payload-3")))

	expectedNextOffset := int64(25)
	if seg.NextOffset != expectedNextOffset {
		t.Errorf("Expected NextOffset %d, got %d", expectedNextOffset, seg.NextOffset)
	}
	seg.Close()

	// 3. Sabotage: Truncate index file (Simulate loss)
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	if err := os.Truncate(idxPath, 0); err != nil {
		t.Fatalf("Failed to truncate index: %v", err)
	}

	// 4. Recovery: Re-open segment
	recoveredSeg, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("Failed to recover segment: %v", err)
	}
	defer recoveredSeg.Close()

	// 5. Verify
	// Check NextOffset recovery
	if recoveredSeg.NextOffset != expectedNextOffset {
		t.Errorf("Recovered NextOffset mismatch. Want %d, Got %d", expectedNextOffset, recoveredSeg.NextOffset)
	}

	// Check Index regeneration (Lookup test)
	// Should find relative offset 10 (Start of Batch 2)
	offset, err := recoveredSeg.index.Lookup(10)
	if err != nil || offset == 0 {
		t.Errorf("Index lookup failed after recovery. Offset: %d, Err: %v", offset, err)
	}
}

func TestSegment_Recovery_TruncateCorruptLog(t *testing.T) {
	// 1. Setup
	dir := t.TempDir()
	cfg := Config{
		SegmentMaxBytes:    1024 * 1024,
		IndexMaxBytes:      1024 * 1024,
		IndexIntervalBytes: 100,
	}
	baseOffset := int64(100)

	seg, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 2. Write valid data
	seg.Append(createValidBatchBytes(100, 5, []byte("valid-data")))

	validSize := seg.log.Size() // Snapshot valid size
	seg.Close()

	// 3. Sabotage: Append garbage data to log
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("Failed to open log for corruption: %v", err)
	}

	// Write corrupt/partial data (invalid header/length/CRC)
	garbage := []byte{0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF}
	if _, err := f.Write(garbage); err != nil {
		t.Fatalf("Failed to write garbage: %v", err)
	}
	f.Close()

	// 4. Recovery
	recoveredSeg, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("Failed to recover segment: %v", err)
	}
	defer recoveredSeg.Close()

	// 5. Verify
	// Log size should revert to pre-corruption state
	if recoveredSeg.log.Size() != validSize {
		t.Errorf("Log size mismatch. Expected %d (truncated), Got %d", validSize, recoveredSeg.log.Size())
	}

	// NextOffset should be correct (100 + 5 = 105)
	if recoveredSeg.NextOffset != 105 {
		t.Errorf("NextOffset mismatch. Expected 105, Got %d", recoveredSeg.NextOffset)
	}
}
