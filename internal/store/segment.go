package store

import (
	"errors"
	"fmt"
	"io"
	"lightkafka/internal/record"
	"lightkafka/pkg"
	"os"
	"syscall"
)

var (
	ErrSegmentFull     = errors.New("segment is full")
	ErrOutOfRange      = errors.New("out of range")
	ErrCorruptedRecord = errors.New("corrupted record")
)

/**
 * A segment manage one file on disk.
 */
type Segment struct {
	file       *os.File
	data       []byte // mmap'd data
	size       int64  // size of the file
	writePos   int64  // position to write next
	nextOffset uint64 // offset of the next record
	baseOffset uint64 // base offset of the segment
}

func NewSegment(filename string, baseOffset uint64, maxSize int64) (*Segment, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	filestat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if filestat.Size() < maxSize {
		if err := syscall.Ftruncate(int(file.Fd()), maxSize); err != nil {
			file.Close()
			return nil, err
		}
	}

	mapeed, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(maxSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		return nil, err
	}

	s := &Segment{
		file:       file,
		data:       mapeed,
		size:       maxSize,
		writePos:   0,
		nextOffset: baseOffset,
		baseOffset: baseOffset,
	}

	if err := s.recover(); err != nil {
		s.Close()
		return nil, err
	}

	return s, nil

}

/* Write a record to the segment */
func (s *Segment) Append(record *record.Record) (uint64, error) {
	needSize := record.Size()

	if s.writePos+int64(needSize) > s.size {
		return 0, ErrSegmentFull
	}

	currentOffset := s.nextOffset
	record.Offset = currentOffset

	destSlice := s.data[s.writePos : s.writePos+int64(needSize)]

	if _, err := record.MarshalTo(destSlice); err != nil {
		return 0, err
	}

	s.writePos += int64(needSize)
	s.nextOffset++

	return currentOffset, nil
}

/* Read a record from the segment */
func (s *Segment) ReadWithPosition(position int64) (*record.Record, int64, error) {

	/* If the position is greater than the write position, return EOF */
	if position >= s.writePos {
		return nil, 0, io.EOF
	}

	/* If the position is greater than the size of the file, return unexpected EOF */
	if position+record.HEADER_SIZE > s.size {
		return nil, 0, io.ErrUnexpectedEOF
	}

	header := record.UnmarshalHeader(s.data[position : position+record.HEADER_SIZE])

	recordEnd := position + int64(header.TotalSize)

	if recordEnd > s.writePos {
		return nil, 0, ErrCorruptedRecord
	}

	var rec record.Record
	if err := record.UnmarshalInto(s.data[position:recordEnd], &rec); err != nil {
		return nil, 0, err
	}

	return &rec, recordEnd, nil
}

func (s *Segment) CanAppend(size int) bool {
	return s.writePos+int64(size) > s.size
}

func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

func (s *Segment) NextOffset() uint64 {
	return s.nextOffset
}

/* Returns the number of bytes written to the segment */
func (s *Segment) SizeBytes() int64 {
	return s.writePos
}

func (s *Segment) Close() error {
	if err := s.Sync(); err != nil {
		fmt.Printf("Failed to sync segment: %v\n", err)
	}
	if err := syscall.Munmap(s.data); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *Segment) Sync() error {
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(pkg.ToUintptr(s.data)),
		uintptr(s.size),
		syscall.MS_SYNC,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func (s *Segment) recover() error {
	var position int64 = 0

	lastOffset := s.baseOffset
	foundAny := false

	for {
		if position+record.HEADER_SIZE > s.size {
			break
		}

		header := record.UnmarshalHeader(s.data[position : position+record.HEADER_SIZE])

		/* Cause deafult data is 0, it means the record is empty */
		if header.TotalSize == 0 {
			break
		}

		if position+int64(header.TotalSize) > s.size {
			fmt.Printf("Truncating corrupted segment at %d\n", position)
			break
		}

		lastOffset = header.Offset
		foundAny = true

		position += int64(header.TotalSize)
	}

	s.writePos = position

	if foundAny {
		s.nextOffset = lastOffset + 1
	} else {
		s.nextOffset = s.baseOffset
	}

	return nil
}
