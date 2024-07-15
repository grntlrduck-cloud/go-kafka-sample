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
	println("##### starting kafka container #####")
	ctx := context.Background()
	kafkaContainer := containers.StartKafka(ctx)
	defer func() {
		if shutdownErr := kafkaContainer.Broker.Terminate(ctx); shutdownErr != nil {
			panic("failed to shut down kafka contianer")
		}
	}()
  println("#### started container sucessfully ####")

	producer := kafka.NewProducer(kafkaContainer.BootstrapServers)
	defer producer.Close()
  println("#### created producer ####")
  
  // create the topic by sending a message
	kafkaSendErr := producer.SendEventSync(demoTopicName, uuid.New().String(), "hello world")
	if kafkaSendErr != nil {
		fmt.Printf("failed to send kafka message %v", kafkaSendErr)
	}
  time.Sleep(2 * time.Second)

	consumer := kafka.NewConsumer(kafka.KafkaConsumerProps{
		BootstrapServers: kafkaContainer.BootstrapServers,
		Topics:           []string{demoTopicName},
		ConsumerGroup:    consumerGroup,
	})
	defer consumer.Close()
	// we need to create a context that can be cancled to prevent
	// the poll go-routine from running when the consumer is already closed
	// keep in mind the last defered func will be executed first -> FILO order
	ctxCancel, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Consume(ctxCancel)
  println("#### started consumer ####")
  
  println("#### sending messages ... ####")
	// could use go-routines or a wait group here instead of blunt for
	for range 1000 {
		kafkaSendErr := producer.SendEventSync(demoTopicName, uuid.New().String(), "hello there")
		if kafkaSendErr != nil {
			fmt.Printf("failed to send kafka message %v", kafkaSendErr)
		}
	}
  println("#### done publishing ####")

	// give the consumer some processing time
	time.Sleep(2 * time.Second)

	println("#### Shutting down. C'Ya! ####")
}
