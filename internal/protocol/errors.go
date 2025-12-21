package protocol

import "errors"

var (
	ErrInvalidRequestSize = errors.New("invalid request size")
	ErrPacketTooShort     = errors.New("packet too short")
)
