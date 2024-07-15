package containers

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	kafkaContainerVersion          = "confluentinc/confluent-local:7.5.5"
	kafkaDefaultPort      nat.Port = "9092/tcp"
)

type KafkaBrokerContainer struct {
	Broker           *kafka.KafkaContainer
	BootstrapServers string
}

func StartKafka(ctx context.Context) KafkaBrokerContainer {
	kafkaContainer, err := kafka.Run(ctx, kafkaContainerVersion)
	if err != nil {
		panic("failed to start kafka container")
	}
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		if shutdownErr := kafkaContainer.Terminate(ctx); shutdownErr != nil {
			panic("failed to shut down kafka contianer")
		}
		panic("failed to get BootstrapServers for kafka container")
	}
	bootstrapServers := strings.Join(brokers, ",")
	fmt.Printf("#### using kafka bootstrap servers=%s ####\n", bootstrapServers)
	return KafkaBrokerContainer{Broker: kafkaContainer, BootstrapServers: bootstrapServers}
}
