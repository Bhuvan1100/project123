package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	partition := 0

	conn, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		"localhost:9092",
		"orders",
		partition,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	broker := conn.Broker()

	fmt.Println("Topic: orders")
	fmt.Println("Partition:", partition)
	fmt.Println("Leader ID:", broker.ID)
	fmt.Println("Leader Host:", broker.Host)
	fmt.Println("Leader Port:", broker.Port)
}