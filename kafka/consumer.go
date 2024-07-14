package kafka

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const minCommitCount = 5

type KafkaConsumerProps struct {
	BootstrapServers string
	ConsumerGroup    string
	Topics           []string
}

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topics   []string
}

func (c *KafkaConsumer) Consume(ctx context.Context) {
	err := c.consumer.SubscribeTopics(c.topics, nil)
	if err != nil {
		panic("failed to subscribe to topics")
	}
	var msgCount uint32 = 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.poll(msgCount)
		}
	}
}

func (c *KafkaConsumer) Close() {
	err := c.consumer.Close()
	if err != nil {
		panic("failed to close consumer")
	}
	println("closed consumer")
}

func (c *KafkaConsumer) poll(msgCount uint32) {
	// source: https://docs.confluent.io/kafka-clients/go/current/overview.html
	ev := c.consumer.Poll(100)
	switch e := ev.(type) {
	case *kafka.Message:
		msgCount += 1
		if msgCount%minCommitCount == 0 {
			go func() {
				offsets, err := c.consumer.Commit()
				if err != nil {
					fmt.Printf("failed to commit offset %s with error %s", offsets, err)
				}
			}()
		}
		fmt.Printf("consumed message on %s: key=%s message=%s\n",
			e.TopicPartition, string(e.Key), string(e.Value))
	case kafka.PartitionEOF:
		fmt.Printf("reached %v\n", e)
	case kafka.Error:
		fmt.Fprintf(os.Stderr, "error: %v\n", e)
		return
	}
}

func NewConsumer(props KafkaConsumerProps) *KafkaConsumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               props.BootstrapServers,
		"group.id":                        props.ConsumerGroup,
		"go.application.rebalance.enable": true,
		"socket.timeout.ms":               100000,
		"log_level":                       1,
	})
	if err != nil {
		panic("failed to create consumer")
	}
	return &KafkaConsumer{consumer: consumer, topics: props.Topics}
}
