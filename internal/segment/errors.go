package segment

import "errors"

var (
	ErrSegmentFull      = errors.New("segment is full")
	ErrIndexFull        = errors.New("index is full")
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrInvalidConfig    = errors.New("invalid configuration")
	ErrInsufficientData = errors.New("insufficient data to decode record batch")
)
