package segment

import (
	"os"
	"sync"
	"syscall"

	"lightkafka/pkg"

	"golang.org/x/sys/unix"
)

type Log struct {
	mu   sync.RWMutex
	file *os.File
	data []byte // mmap region
	size int64  // logical size (valid data limit)
}

func NewLog(path string, maxBytes int64) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Pre-allocation
	if fi.Size() < maxBytes {
		if err := f.Truncate(maxBytes); err != nil {
			f.Close()
			return nil, err
		}
	}

	data, err := syscall.Mmap(
		int(f.Fd()), 0, int(maxBytes),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Log{file: f, data: data, size: 0}, nil
}

// Size returns the logical size of the log.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

// SetSize manually updates the logical size (used during recovery).
func (l *Log) SetSize(size int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.size = size
}

func (l *Log) Append(b []byte) (int, int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := len(b)
	if l.size+int64(n) > int64(len(l.data)) {
		return 0, 0, ErrSegmentFull
	}

	copy(l.data[l.size:], b)
	pos := l.size
	l.size += int64(n)

	return n, pos, nil
}

// ReadAt accumulates batches starting from pos up to maxBytes.
func (l *Log) ReadAt(pos int64, maxBytes int32) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if pos >= l.size {
		return nil, ErrOffsetOutOfRange
	}

	currentPos := pos
	totalBytes := int64(0)

	for currentPos < l.size {
		// Check minimal header size (12 bytes for Offset + Length)
		if l.size-currentPos < 12 {
			break
		}

		// Parse Batch Length
		lenBytes := l.data[currentPos+8 : currentPos+12]
		batchLen := int32(pkg.Encod.Uint32(lenBytes))
		currentBatchSize := 12 + int64(batchLen)

		// Boundary Check
		if currentPos+currentBatchSize > l.size {
			break
		}

		// MaxBytes Check
		if totalBytes+currentBatchSize > int64(maxBytes) {
			// If it's the first batch, include it to ensure progress.
			if totalBytes == 0 {
				totalBytes = currentBatchSize
				break
			}
			// Otherwise, stop here.
			break
		}

		totalBytes += currentBatchSize
		currentPos += currentBatchSize
	}

	if totalBytes == 0 {
		return nil, nil
	}

	return l.data[pos : pos+totalBytes], nil
}

// ReadRaw reads exactly `size` bytes. Used for header scanning.
func (l *Log) ReadRaw(pos int64, size int) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if pos+int64(size) > l.size {
		return nil, nil // Not enough data
	}
	return l.data[pos : pos+int64(size)], nil
}

func (l *Log) configSize() int64 {
	return int64(len(l.data))
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_ = unix.Msync(l.data, unix.MS_SYNC)
	_ = syscall.Munmap(l.data)
	_ = l.file.Truncate(l.size) // Trim to actual data size
	return l.file.Close()
}

func (l *Log) Delete() error {
	path := l.file.Name()
	_ = syscall.Munmap(l.data)
	_ = l.file.Close()
	return os.Remove(path)
}
