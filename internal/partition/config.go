package partition

import "lightkafka/internal/segment"

type PartitionConfig struct {
	SegmentConfig segment.Config
}
