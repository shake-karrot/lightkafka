package pkg

import "encoding/binary"

const (
	// Size lengths (in bytes)
	LenOffset   = 8
	LenSize     = 4
	LenCRC      = 4
	LenMagic    = 1
	LenAttr     = 2
	LenEpoch    = 4
	LenSequence = 4
	LenCount    = 4
)

// Encoding alias (Kafka uses BigEndian)
var Encod = binary.BigEndian
