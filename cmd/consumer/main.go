package main

import (
	"context"
	"fmt"
	"log"
	

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "order-consumer-group",
	})

	defer reader.Close()

	fmt.Println("Consumer started...")

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"Received: key=%s value=%s partition=%d offset=%d\n",
			string(message.Key),
			string(message.Value),
			message.Partition,
			message.Offset,
		)
	}
	
}