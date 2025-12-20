package segment

type Config struct {
	SegmentMaxBytes int64  // e.g., 1GB
	IndexMaxBytes   int64  // e.g., 10MB
	BaseDir         string // e.g., "./data"
}
