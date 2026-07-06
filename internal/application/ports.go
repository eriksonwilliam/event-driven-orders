package application

import (
	"context"

	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
)

// Handler reage a um evento recebido de um tópico.
type Handler = func(ctx context.Context, e domain.Event) error

// Broker é a porta de mensageria (publish/subscribe) entre os serviços.
type Broker interface {
	Publish(ctx context.Context, topic string, e domain.Event) error
	Subscribe(topic string, h Handler)
}

// OrderStore é a porta do read model consultado pela API.
type OrderStore interface {
	Put(o domain.Order)
	Get(id string) (domain.Order, bool)
}
