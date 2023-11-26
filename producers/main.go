package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-uuid"
)

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:29092"}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}

	return producer, nil
}

func main() {
	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("unable to initialize pubsub model")
	}
	defer producer.Close()

	producer.BeginTxn()

	for {
		v, _ := uuid.GenerateUUID()
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: "products",
			Key:   sarama.StringEncoder("producer-01"),
			Value: sarama.StringEncoder("apples" + v),
		})

		if err != nil {
			producer.AbortTxn()
			return
		}

		producer.CommitTxn()
		fmt.Printf("Message sent to the broker. Partition : %v, offset : %v \n", partition, offset)
		time.Sleep(5 * time.Second)
	}
}
