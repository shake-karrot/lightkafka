package partition

import "lightkafka/internal/segment"

type PartitionConfig struct {
	SegmentConfig segment.Config

	RetentionMs              int64
	RetentionBytes           int64
	RetentionCheckIntervalMs int64
}
