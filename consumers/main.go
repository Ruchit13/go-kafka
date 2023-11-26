package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := sarama.NewConsumer([]string{"localhost:29092"}, config)
	if err != nil {
		log.Printf("error in initializing consumer grp. error : %v", err)
		return
	}
	defer consumer.Close()

	p, err := consumer.ConsumePartition("products", 0, 15)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-sigs:
				fmt.Println("graceful termination")
				return
			case msg := <-p.Messages():
				fmt.Printf("Offset : %v, Message %v from Partition : %v\n", msg.Offset, string(msg.Value), msg.Partition)
			}
		}
	}()

	<-sigs
}
