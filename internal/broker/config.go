package broker

import "lightkafka/internal/partition"

// TODO: TopicConfig 추가 시 BrokerConfig → TopicConfig → PartitionConfig 계층 병합 추가
type Config struct {
	ListenAddr      string
	BaseDir         string
	PartitionConfig partition.PartitionConfig
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:      ":9092",
		BaseDir:         "./data",
		PartitionConfig: partition.DefaultConfig(),
	}
}
