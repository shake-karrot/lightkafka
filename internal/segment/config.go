package segment

type Config struct {
	SegmentMaxBytes int64
	IndexMaxBytes   int64
}

func DefaultConfig() Config {
	return Config{
		SegmentMaxBytes: 1 << 30,  // 1GB
		IndexMaxBytes:   10 << 20, // 10MB
	}
}
