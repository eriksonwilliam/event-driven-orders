//go:build kafka

package main

import (
	"os"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/kafka"
)

const brokerName = "kafka"

// newBroker devolve o adapter real sobre Apache Kafka (build tag "kafka").
func newBroker() (application.Broker, func(), error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	b := kafka.NewBroker(brokers)
	return b, b.Close, nil
}
