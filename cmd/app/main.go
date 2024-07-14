package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/grntlduck-cloud/go-kafka-sample/containers"
	"github.com/grntlduck-cloud/go-kafka-sample/kafka"
)

const (
	demoTopicName = "com.grntlrduck.cloud.greetings"
	consumerGroup = "consumer-group-1"
)

func main() {
	ctx := context.Background()
	kafkaContainer := containers.StartKafka(ctx)
	defer func() {
		if shutdownErr := kafkaContainer.Broker.Terminate(ctx); shutdownErr != nil {
			panic("failed to shut down kafka contianer")
		}
	}()

	consumer := kafka.NewConsumer(kafka.KafkaConsumerProps{
		BootstrapServers: kafkaContainer.BootstrapServers,
		Topics:           []string{demoTopicName},
		ConsumerGroup:    consumerGroup,
	})
	defer consumer.Close()
	// we need to create a context that can be canled to prevent
	// the poll go-routine from running when the consumer is already closed
	// keep in mind the last defered func will be executed first -> FILO order
	ctxCancel, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Consume(ctxCancel)

	producer := kafka.NewProducer(kafkaContainer.BootstrapServers)
	defer producer.Close()
  // could use go-routines or a wait group here instead of blant for
	for range 1000 {
		kafkaSendErr := producer.SendEventSync(demoTopicName, uuid.New().String(), "hello there")
		if kafkaSendErr != nil {
			fmt.Printf("failed to send kafka message %v", kafkaSendErr)
		}
	}

	// give the consumer some processing time
	time.Sleep(5 * time.Second)

	println("############################")
	println("Shutting down. C'Ya!")
	println("############################")
}
