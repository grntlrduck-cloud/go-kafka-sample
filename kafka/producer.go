package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

func (p *KafkaProducer) SendEventSync(topic string, key string, event string) error {
	// source https://docs.confluent.io/kafka-clients/go/current/overview.html
	deliveryChan := make(chan kafka.Event, 1000)
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(event),
		Key:            []byte(key),
	},
		deliveryChan,
	)
	result := <-deliveryChan
	message := result.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		fmt.Printf("delivery failed: %v\n", message.TopicPartition.Error)
	} else {
		fmt.Printf("delivered message to topic %s [%d] at offset %v\n",
			*message.TopicPartition.Topic, message.TopicPartition.Partition, message.TopicPartition.Offset)
	}
	close(deliveryChan)
	return err
}

func (p *KafkaProducer) Close() {
	p.producer.Flush(100)
	p.producer.Close()
  println("#### closed producer ####")
}

func NewProducer(bootStrapServers string) *KafkaProducer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootStrapServers,
		"client.id":         "my-client-id",
		"acks":              "all",
		"socket.timeout.ms": 1000,
		"log_level":         1,
	})
	if err != nil {
		panic("failed to create producer")
	}
	return &KafkaProducer{producer: p}
}
