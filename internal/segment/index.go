package segment

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"syscall"
)

const entryWidth = 8 // Offset(4) + Position(4)

type Index struct {
	mu   sync.RWMutex
	file *os.File
	data []byte // mmap
	size int64  // used bytes
}

func NewIndex(path string, maxBytes int64) (*Index, error) {
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
		return nil, err
	}

	return &Index{file: f, data: data, size: 0}, nil
}

// Write appends (RelativeOffset, PhysicalPosition).
func (i *Index) Write(off int32, pos int32) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size+entryWidth > int64(len(i.data)) {
		return io.EOF
	}

	binary.BigEndian.PutUint32(i.data[i.size:], uint32(off))
	binary.BigEndian.PutUint32(i.data[i.size+4:], uint32(pos))
	i.size += entryWidth
	return nil
}

// Lookup performs binary search to find position <= relOff.
func (i *Index) Lookup(relOff int32) (int64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.size == 0 {
		return 0, nil
	}

	var outPos int32 = -1
	entries := int(i.size / entryWidth)
	low, high := 0, entries-1

	for low <= high {
		mid := (low + high) / 2
		offsetPos := mid * entryWidth

		midOff := int32(binary.BigEndian.Uint32(i.data[offsetPos:]))
		midPos := int32(binary.BigEndian.Uint32(i.data[offsetPos+4:]))

		if midOff <= relOff {
			outPos = midPos
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if outPos == -1 {
		return 0, nil
	}
	return int64(outPos), nil
}

func (i *Index) Close() error {
	syscall.Munmap(i.data)
	i.file.Truncate(i.size) // Trim to actual size
	return i.file.Close()
}

/* Last Entry */
func (i *Index) LastEntry() (off int32, pos int32, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.size == 0 {
		return 0, 0, nil
	}

	lastOffset := i.size - entryWidth
	off = int32(binary.BigEndian.Uint32(i.data[lastOffset : lastOffset+4]))
	pos = int32(binary.BigEndian.Uint32(i.data[lastOffset+4 : lastOffset+8]))
	return off, pos, nil
}

/* Truncate */
func (i *Index) Truncate(size int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if size > int64(len(i.data)) {
		return io.ErrShortBuffer
	}

	i.size = size
	return nil
}
