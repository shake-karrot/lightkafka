package record

import (
	"hash/crc32"
	"testing"
)

func TestRecord_Size(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			record Record
		}
		want uint32
	}{
		{
			name: "empty key and value",
			args: struct {
				record Record
			}{
				record: Record{
					Key:   []byte{},
					Value: []byte{},
				},
			},
			want: HEADER_SIZE,
		},
		{
			name: "key only",
			args: struct {
				record Record
			}{
				record: Record{
					Key:   []byte("test-key"),
					Value: []byte{},
				},
			},
			want: HEADER_SIZE + 8,
		},
		{
			name: "value only",
			args: struct {
				record Record
			}{
				record: Record{
					Key:   []byte{},
					Value: []byte("test-value"),
				},
			},
			want: HEADER_SIZE + 10,
		},
		{
			name: "both key and value",
			args: struct {
				record Record
			}{
				record: Record{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			want: HEADER_SIZE + 3 + 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.args.record.Size(); got != tt.want {
				t.Errorf("Record.Size() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecord_MarshalTo(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			record     Record
			bufferSize int
		}
		want struct {
			bytesWritten int
			err          error
			validateCRC  bool
		}
		wantErr bool
	}{
		{
			name: "successful marshal",
			args: struct {
				record     Record
				bufferSize int
			}{
				record: Record{
					Offset:    100,
					Timestamp: 1234567890,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
				},
				bufferSize: 1000,
			},
			want: struct {
				bytesWritten int
				err          error
				validateCRC  bool
			}{
				validateCRC: true,
			},
			wantErr: false,
		},
		{
			name: "buffer too small",
			args: struct {
				record     Record
				bufferSize int
			}{
				record: Record{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
				bufferSize: HEADER_SIZE - 1,
			},
			want: struct {
				bytesWritten int
				err          error
				validateCRC  bool
			}{
				err: ErrInsufficientBuffer,
			},
			wantErr: true,
		},
		{
			name: "empty key and value",
			args: struct {
				record     Record
				bufferSize int
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte{},
				},
				bufferSize: 100,
			},
			want: struct {
				bytesWritten int
				err          error
				validateCRC  bool
			}{
				validateCRC: true,
			},
			wantErr: false,
		},
		{
			name: "large key and value",
			args: struct {
				record     Record
				bufferSize int
			}{
				record: Record{
					Offset:    999999,
					Timestamp: 9999999999,
					Key:       make([]byte, 1000),
					Value:     make([]byte, 5000),
				},
				bufferSize: 10000,
			},
			want: struct {
				bytesWritten int
				err          error
				validateCRC  bool
			}{
				validateCRC: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.args.bufferSize)
			got, err := tt.args.record.MarshalTo(buf)

			if (err != nil) != tt.wantErr {
				t.Errorf("Record.MarshalTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				if err != tt.want.err {
					t.Errorf("Record.MarshalTo() error = %v, want %v", err, tt.want.err)
				}
				return
			}

			if got != int(tt.args.record.Size()) {
				t.Errorf("Record.MarshalTo() = %v, want %v", got, tt.args.record.Size())
			}

			if tt.want.validateCRC {
				header := UnmarshalHeader(buf[:HEADER_SIZE])
				if header.Offset != tt.args.record.Offset {
					t.Errorf("Unmarshaled offset = %v, want %v", header.Offset, tt.args.record.Offset)
				}
				if header.Timestamp != tt.args.record.Timestamp {
					t.Errorf("Unmarshaled timestamp = %v, want %v", header.Timestamp, tt.args.record.Timestamp)
				}
				if header.KeySize != uint32(len(tt.args.record.Key)) {
					t.Errorf("Unmarshaled keySize = %v, want %v", header.KeySize, len(tt.args.record.Key))
				}
				if header.ValueSize != uint32(len(tt.args.record.Value)) {
					t.Errorf("Unmarshaled valueSize = %v, want %v", header.ValueSize, len(tt.args.record.Value))
				}

				calculatedCRC := crc32.ChecksumIEEE(buf[16:header.TotalSize])
				if header.Crc != calculatedCRC {
					t.Errorf("CRC mismatch: stored = %v, calculated = %v", header.Crc, calculatedCRC)
				}
			}
		})
	}
}

func TestUnmarshalHeader(t *testing.T) {
	record := Record{
		Offset:    12345,
		Timestamp: 9876543210,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	buf := make([]byte, record.Size())
	_, err := record.MarshalTo(buf)
	if err != nil {
		t.Fatalf("Failed to marshal record: %v", err)
	}

	header := UnmarshalHeader(buf[:HEADER_SIZE])

	if header.TotalSize != record.Size() {
		t.Errorf("UnmarshalHeader() TotalSize = %v, want %v", header.TotalSize, record.Size())
	}
	if header.Offset != record.Offset {
		t.Errorf("UnmarshalHeader() Offset = %v, want %v", header.Offset, record.Offset)
	}
	if header.Timestamp != record.Timestamp {
		t.Errorf("UnmarshalHeader() Timestamp = %v, want %v", header.Timestamp, record.Timestamp)
	}
	if header.KeySize != uint32(len(record.Key)) {
		t.Errorf("UnmarshalHeader() KeySize = %v, want %v", header.KeySize, len(record.Key))
	}
	if header.ValueSize != uint32(len(record.Value)) {
		t.Errorf("UnmarshalHeader() ValueSize = %v, want %v", header.ValueSize, len(record.Value))
	}
}

func TestUnmarshalInto(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			record Record
		}
		want struct {
			record Record
		}
		wantErr bool
	}{
		{
			name: "successful unmarshal",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    100,
					Timestamp: 1234567890,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    100,
					Timestamp: 1234567890,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
				},
			},
			wantErr: false,
		},
		{
			name: "empty key and value",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte{},
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte{},
				},
			},
			wantErr: false,
		},
		{
			name: "large values",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    999999,
					Timestamp: 9999999999,
					Key:       make([]byte, 100),
					Value:     make([]byte, 200),
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    999999,
					Timestamp: 9999999999,
					Key:       make([]byte, 100),
					Value:     make([]byte, 200),
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.args.record.Size())
			_, err := tt.args.record.MarshalTo(buf)
			if err != nil {
				t.Fatalf("Failed to marshal record: %v", err)
			}

			var result Record
			err = UnmarshalInto(buf, &result)

			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalInto() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if result.Offset != tt.want.record.Offset {
					t.Errorf("UnmarshalInto() Offset = %v, want %v", result.Offset, tt.want.record.Offset)
				}
				if result.Timestamp != tt.want.record.Timestamp {
					t.Errorf("UnmarshalInto() Timestamp = %v, want %v", result.Timestamp, tt.want.record.Timestamp)
				}
				if string(result.Key) != string(tt.want.record.Key) {
					t.Errorf("UnmarshalInto() Key = %v, want %v", result.Key, tt.want.record.Key)
				}
				if string(result.Value) != string(tt.want.record.Value) {
					t.Errorf("UnmarshalInto() Value = %v, want %v", result.Value, tt.want.record.Value)
				}
			}
		})
	}
}

func TestUnmarshalInto_InvalidCRC(t *testing.T) {
	record := Record{
		Offset:    100,
		Timestamp: 1234567890,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	buf := make([]byte, record.Size())
	_, err := record.MarshalTo(buf)
	if err != nil {
		t.Fatalf("Failed to marshal record: %v", err)
	}

	buf[12] ^= 0xFF

	var result Record
	err = UnmarshalInto(buf, &result)
	if err != ErrInvalidCRC {
		t.Errorf("UnmarshalInto() error = %v, want %v", err, ErrInvalidCRC)
	}
}

func TestUnmarshalInto_InsufficientBuffer(t *testing.T) {
	record := Record{
		Offset:    100,
		Timestamp: 1234567890,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	buf := make([]byte, record.Size())
	_, err := record.MarshalTo(buf)
	if err != nil {
		t.Fatalf("Failed to marshal record: %v", err)
	}

	smallBuf := buf[:HEADER_SIZE-1]
	var result Record
	err = UnmarshalInto(smallBuf, &result)
	if err != ErrInsufficientBuffer {
		t.Errorf("UnmarshalInto() error = %v, want %v", err, ErrInsufficientBuffer)
	}

	header := UnmarshalHeader(buf[:HEADER_SIZE])
	smallBuf2 := buf[:HEADER_SIZE+int(header.KeySize)-1]
	var result2 Record
	err = UnmarshalInto(smallBuf2, &result2)
	if err != ErrInsufficientBuffer {
		t.Errorf("UnmarshalInto() error = %v, want %v", err, ErrInsufficientBuffer)
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			record Record
		}
		want struct {
			record Record
		}
	}{
		{
			name: "normal record",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    12345,
					Timestamp: 9876543210,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    12345,
					Timestamp: 9876543210,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
				},
			},
		},
		{
			name: "empty key",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte("value-only"),
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte("value-only"),
				},
			},
		},
		{
			name: "empty value",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    999,
					Timestamp: 1111111111,
					Key:       []byte("key-only"),
					Value:     []byte{},
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    999,
					Timestamp: 1111111111,
					Key:       []byte("key-only"),
					Value:     []byte{},
				},
			},
		},
		{
			name: "both empty",
			args: struct {
				record Record
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte{},
				},
			},
			want: struct {
				record Record
			}{
				record: Record{
					Offset:    0,
					Timestamp: 0,
					Key:       []byte{},
					Value:     []byte{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.args.record.Size())
			_, err := tt.args.record.MarshalTo(buf)
			if err != nil {
				t.Fatalf("MarshalTo() error = %v", err)
			}

			var result Record
			err = UnmarshalInto(buf, &result)
			if err != nil {
				t.Fatalf("UnmarshalInto() error = %v", err)
			}

			if result.Offset != tt.want.record.Offset {
				t.Errorf("Offset = %v, want %v", result.Offset, tt.want.record.Offset)
			}
			if result.Timestamp != tt.want.record.Timestamp {
				t.Errorf("Timestamp = %v, want %v", result.Timestamp, tt.want.record.Timestamp)
			}
			if string(result.Key) != string(tt.want.record.Key) {
				t.Errorf("Key = %v, want %v", result.Key, tt.want.record.Key)
			}
			if string(result.Value) != string(tt.want.record.Value) {
				t.Errorf("Value = %v, want %v", result.Value, tt.want.record.Value)
			}
		})
	}
}
