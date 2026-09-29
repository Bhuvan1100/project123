package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "orders",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  4,
		BatchSize:    2,
		BatchTimeout: 5 * time.Second,
		Compression:  kafka.Gzip,
		
	}

	defer func() {
		fmt.Println("Closing writer; waiting for pending writes...")
		err := writer.Close();
		if  err != nil {
			log.Println("Close error:", err)
		}

		fmt.Println("Writer closed.")
	}()

	fmt.Println("Producer started...")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	start := time.Now()

	err := writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte("order-111"),
			Value: []byte(`{"order_id":109,"amount":999.5}`),
		},
		
	)

	fmt.Println("WriteMessages took:", time.Since(start))

	if err != nil {
		log.Println("Write returned an error:", err)
		return
	}

	fmt.Println("Message written successfully.")
}