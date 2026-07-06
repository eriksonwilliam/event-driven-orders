//go:build !kafka

package main

import (
	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/memory"
)

const brokerName = "memory"

// newBroker devolve o broker in-process (padrão, sem dependências externas).
func newBroker() (application.Broker, func(), error) {
	return memory.NewBroker(), func() {}, nil
}
