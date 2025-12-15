package store

import (
	"io"
	"lightkafka/internal/record"
	"path/filepath"
	"testing"
)

func setupTestSegment(t *testing.T, baseOffset uint64, maxSize int64) (*Segment, string) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test-segment.dat")

	seg, err := NewSegment(filename, baseOffset, maxSize)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	return seg, filename
}

func TestNewSegment(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			baseOffset uint64
			maxSize    int64
		}
		want struct {
			baseOffset uint64
			nextOffset uint64
			sizeBytes  int64
		}
		wantErr bool
	}{
		{
			name: "normal segment",
			args: struct {
				baseOffset uint64
				maxSize    int64
			}{
				baseOffset: 0,
				maxSize:    1024 * 1024, // 1MB
			},
			want: struct {
				baseOffset uint64
				nextOffset uint64
				sizeBytes  int64
			}{
				baseOffset: 0,
				nextOffset: 0,
				sizeBytes:  0,
			},
			wantErr: false,
		},
		{
			name: "large segment",
			args: struct {
				baseOffset uint64
				maxSize    int64
			}{
				baseOffset: 1000,
				maxSize:    10 * 1024 * 1024, // 10MB
			},
			want: struct {
				baseOffset uint64
				nextOffset uint64
				sizeBytes  int64
			}{
				baseOffset: 1000,
				nextOffset: 1000,
				sizeBytes:  0,
			},
			wantErr: false,
		},
		{
			name: "small segment",
			args: struct {
				baseOffset uint64
				maxSize    int64
			}{
				baseOffset: 0,
				maxSize:    1024, // 1KB
			},
			want: struct {
				baseOffset uint64
				nextOffset uint64
				sizeBytes  int64
			}{
				baseOffset: 0,
				nextOffset: 0,
				sizeBytes:  0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seg, _ := setupTestSegment(t, tt.args.baseOffset, tt.args.maxSize)
			defer seg.Close()

			if seg.BaseOffset() != tt.want.baseOffset {
				t.Errorf("BaseOffset() = %v, want %v", seg.BaseOffset(), tt.want.baseOffset)
			}
			if seg.NextOffset() != tt.want.nextOffset {
				t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), tt.want.nextOffset)
			}
			if seg.SizeBytes() != tt.want.sizeBytes {
				t.Errorf("SizeBytes() = %v, want %v", seg.SizeBytes(), tt.want.sizeBytes)
			}
		})
	}
}

func TestSegment_Append(t *testing.T) {
	seg, _ := setupTestSegment(t, 0, 1024*1024)
	defer seg.Close()

	tests := []struct {
		name string
		args struct {
			record *record.Record
		}
		want struct {
			offset     uint64
			nextOffset uint64
		}
		wantErr bool
	}{
		{
			name: "append first record",
			args: struct {
				record *record.Record
			}{
				record: &record.Record{
					Timestamp: 1234567890,
					Key:       []byte("key1"),
					Value:     []byte("value1"),
				},
			},
			want: struct {
				offset     uint64
				nextOffset uint64
			}{
				offset:     0,
				nextOffset: 1,
			},
			wantErr: false,
		},
		{
			name: "append second record",
			args: struct {
				record *record.Record
			}{
				record: &record.Record{
					Timestamp: 1234567891,
					Key:       []byte("key2"),
					Value:     []byte("value2"),
				},
			},
			want: struct {
				offset     uint64
				nextOffset uint64
			}{
				offset:     1,
				nextOffset: 2,
			},
			wantErr: false,
		},
		{
			name: "append empty key and value",
			args: struct {
				record *record.Record
			}{
				record: &record.Record{
					Timestamp: 1234567892,
					Key:       []byte{},
					Value:     []byte{},
				},
			},
			want: struct {
				offset     uint64
				nextOffset uint64
			}{
				offset:     2,
				nextOffset: 3,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, err := seg.Append(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && offset != tt.want.offset {
				t.Errorf("Append() offset = %v, want %v", offset, tt.want.offset)
			}
			if !tt.wantErr && seg.NextOffset() != tt.want.nextOffset {
				t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), tt.want.nextOffset)
			}
		})
	}
}

func TestSegment_Append_SegmentFull(t *testing.T) {
	seg, _ := setupTestSegment(t, 0, record.HEADER_SIZE+10)
	defer seg.Close()

	rec1 := &record.Record{
		Timestamp: 1234567890,
		Key:       []byte("k"),
		Value:     []byte("v"),
	}
	_, err := seg.Append(rec1)
	if err != nil {
		t.Fatalf("Failed to append first record: %v", err)
	}

	rec2 := &record.Record{
		Timestamp: 1234567891,
		Key:       []byte("key2"),
		Value:     []byte("value2"),
	}
	_, err = seg.Append(rec2)
	if err != ErrSegmentFull {
		t.Errorf("Append() error = %v, want %v", err, ErrSegmentFull)
	}
}

func TestSegment_ReadWithPosition(t *testing.T) {
	seg, _ := setupTestSegment(t, 100, 1024*1024)
	defer seg.Close()

	records := []*record.Record{
		{
			Timestamp: 1234567890,
			Key:       []byte("key1"),
			Value:     []byte("value1"),
		},
		{
			Timestamp: 1234567891,
			Key:       []byte("key2"),
			Value:     []byte("value2"),
		},
		{
			Timestamp: 1234567892,
			Key:       []byte("key3"),
			Value:     []byte("value3"),
		},
	}

	positions := make([]int64, len(records))
	for i, rec := range records {
		offset, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
		if i == 0 {
			positions[i] = 0
		} else {
			positions[i] = positions[i-1] + int64(records[i-1].Size())
		}
		if offset != uint64(100+i) {
			t.Errorf("Append() offset = %v, want %v", offset, 100+i)
		}
	}

	// Read records back
	var position int64 = 0
	for i, wantRec := range records {
		gotRec, nextPos, err := seg.ReadWithPosition(position)
		if err != nil {
			t.Fatalf("ReadWithPosition() error = %v", err)
		}

		if gotRec.Offset != uint64(100+i) {
			t.Errorf("ReadWithPosition() offset = %v, want %v", gotRec.Offset, 100+i)
		}
		if gotRec.Timestamp != wantRec.Timestamp {
			t.Errorf("ReadWithPosition() timestamp = %v, want %v", gotRec.Timestamp, wantRec.Timestamp)
		}
		if string(gotRec.Key) != string(wantRec.Key) {
			t.Errorf("ReadWithPosition() key = %v, want %v", gotRec.Key, wantRec.Key)
		}
		if string(gotRec.Value) != string(wantRec.Value) {
			t.Errorf("ReadWithPosition() value = %v, want %v", gotRec.Value, wantRec.Value)
		}

		position = nextPos
	}

	_, _, err := seg.ReadWithPosition(position)
	if err != io.EOF {
		t.Errorf("ReadWithPosition() error = %v, want %v", err, io.EOF)
	}
}

func TestSegment_ReadWithPosition_OutOfRange(t *testing.T) {
	maxSize := int64(record.HEADER_SIZE + 10)
	seg, _ := setupTestSegment(t, 0, maxSize)
	defer seg.Close()

	rec := &record.Record{
		Timestamp: 1234567890,
		Key:       []byte("k"),
		Value:     []byte("v"),
	}
	_, err := seg.Append(rec)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	position := maxSize - record.HEADER_SIZE + 1
	_, _, err = seg.ReadWithPosition(position)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("ReadWithPosition() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestSegment_CanAppend(t *testing.T) {
	seg, _ := setupTestSegment(t, 0, 1024)
	defer seg.Close()

	tests := []struct {
		name string
		args struct {
			size int
		}
		want bool
	}{
		{
			name: "can append small record",
			args: struct {
				size int
			}{
				size: 100,
			},
			want: false,
		},
		{
			name: "cannot append large record",
			args: struct {
				size int
			}{
				size: 2000,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := seg.CanAppend(tt.args.size)
			if got != tt.want {
				t.Errorf("CanAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSegment_Recover(t *testing.T) {
	seg, filename := setupTestSegment(t, 100, 1024*1024)

	records := []*record.Record{
		{
			Timestamp: 1234567890,
			Key:       []byte("key1"),
			Value:     []byte("value1"),
		},
		{
			Timestamp: 1234567891,
			Key:       []byte("key2"),
			Value:     []byte("value2"),
		},
	}

	for _, rec := range records {
		_, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Failed to append record: %v", err)
		}
	}

	expectedNextOffset := seg.NextOffset()
	expectedWritePos := seg.SizeBytes()

	seg.Close()

	recoveredSeg, err := NewSegment(filename, 100, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen segment: %v", err)
	}
	defer recoveredSeg.Close()

	if recoveredSeg.NextOffset() != expectedNextOffset {
		t.Errorf("Recovered NextOffset() = %v, want %v", recoveredSeg.NextOffset(), expectedNextOffset)
	}
	if recoveredSeg.SizeBytes() != expectedWritePos {
		t.Errorf("Recovered SizeBytes() = %v, want %v", recoveredSeg.SizeBytes(), expectedWritePos)
	}

	var position int64 = 0
	for i, wantRec := range records {
		gotRec, nextPos, err := recoveredSeg.ReadWithPosition(position)
		if err != nil {
			t.Fatalf("ReadWithPosition() error = %v", err)
		}

		if gotRec.Offset != uint64(100+i) {
			t.Errorf("ReadWithPosition() offset = %v, want %v", gotRec.Offset, 100+i)
		}
		if string(gotRec.Key) != string(wantRec.Key) {
			t.Errorf("ReadWithPosition() key = %v, want %v", gotRec.Key, wantRec.Key)
		}
		if string(gotRec.Value) != string(wantRec.Value) {
			t.Errorf("ReadWithPosition() value = %v, want %v", gotRec.Value, wantRec.Value)
		}

		position = nextPos
	}
}

func TestSegment_Recover_EmptySegment(t *testing.T) {
	seg, filename := setupTestSegment(t, 200, 1024*1024)
	baseOffset := seg.BaseOffset()
	seg.Close()

	recoveredSeg, err := NewSegment(filename, baseOffset, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen segment: %v", err)
	}
	defer recoveredSeg.Close()

	if recoveredSeg.NextOffset() != baseOffset {
		t.Errorf("Recovered NextOffset() = %v, want %v", recoveredSeg.NextOffset(), baseOffset)
	}
	if recoveredSeg.SizeBytes() != 0 {
		t.Errorf("Recovered SizeBytes() = %v, want 0", recoveredSeg.SizeBytes())
	}
}

func TestSegment_Sync(t *testing.T) {
	seg, _ := setupTestSegment(t, 0, 1024*1024)
	defer seg.Close()

	rec := &record.Record{
		Timestamp: 1234567890,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	_, err := seg.Append(rec)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestSegment_Close(t *testing.T) {
	seg, _ := setupTestSegment(t, 0, 1024*1024)

	rec := &record.Record{
		Timestamp: 1234567890,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	_, err := seg.Append(rec)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	_ = seg.Close()
}

func TestSegment_ReadWithPosition_CorruptedRecord(t *testing.T) {
	seg, filename := setupTestSegment(t, 0, 1024*1024)

	rec := &record.Record{
		Timestamp: 1234567890,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	_, err := seg.Append(rec)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	seg.Close()

	recoveredSeg, err := NewSegment(filename, 0, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen segment: %v", err)
	}
	defer recoveredSeg.Close()

	_, _, err = recoveredSeg.ReadWithPosition(0)
	if err != nil {
		t.Errorf("ReadWithPosition() error = %v, want nil", err)
	}
}

func TestSegment_MultipleAppends(t *testing.T) {
	seg, _ := setupTestSegment(t, 0, 1024*1024)
	defer seg.Close()

	numRecords := 100
	for i := 0; i < numRecords; i++ {
		rec := &record.Record{
			Timestamp: int64(i),
			Key:       []byte("key"),
			Value:     []byte("value"),
		}

		offset, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}

		if offset != uint64(i) {
			t.Errorf("Append() offset = %v, want %v", offset, i)
		}
	}

	if seg.NextOffset() != uint64(numRecords) {
		t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), numRecords)
	}

	var position int64 = 0
	for i := 0; i < numRecords; i++ {
		rec, nextPos, err := seg.ReadWithPosition(position)
		if err != nil {
			t.Fatalf("ReadWithPosition() error at record %d: %v", i, err)
		}

		if rec.Offset != uint64(i) {
			t.Errorf("ReadWithPosition() offset = %v, want %v", rec.Offset, i)
		}
		if rec.Timestamp != int64(i) {
			t.Errorf("ReadWithPosition() timestamp = %v, want %v", rec.Timestamp, i)
		}

		position = nextPos
	}
}
