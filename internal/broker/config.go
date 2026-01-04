package broker

import "lightkafka/internal/partition"

// TODO: TopicConfig 추가 시 BrokerConfig → TopicConfig → PartitionConfig 계층 병합 추가
type Config struct {
	ListenAddr      string
	BaseDir         string
	PartitionConfig partition.PartitionConfig
}
