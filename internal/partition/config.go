package partition

import "lightkafka/internal/segment"

type PartitionConfig struct {
	SegmentConfig segment.Config

	RetentionMs              int64
	RetentionBytes           int64
	RetentionCheckIntervalMs int64
}

func DefaultConfig() PartitionConfig {
	return PartitionConfig{
		SegmentConfig: segment.DefaultConfig(),

		RetentionMs:              7 * 24 * 60 * 60 * 1000, // 7 days
		RetentionBytes:           -1,                      // unlimited
		RetentionCheckIntervalMs: 5 * 60 * 1000,           // 5 minutes
	}
}
