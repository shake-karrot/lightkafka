package segment

type Config struct {
	SegmentMaxBytes int64
	IndexMaxBytes   int64
	IndexIntervalBytes int64  // e.g., 4KB
}
