package memory

import (
	"context"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
)

// Broker é um broker in-process que despacha de forma síncrona (inline): ao
// publicar, chama os handlers do tópico na ordem de inscrição. Determinístico —
// ideal para testes e desenvolvimento. O adapter Kafka é assíncrono de verdade.
type Broker struct {
	handlers map[string][]application.Handler
}

// NewBroker cria um broker in-process vazio.
func NewBroker() *Broker {
	return &Broker{handlers: map[string][]application.Handler{}}
}

// Subscribe registra um handler para um tópico.
func (b *Broker) Subscribe(topic string, h application.Handler) {
	b.handlers[topic] = append(b.handlers[topic], h)
}

// Publish entrega o evento a cada handler do tópico, parando no primeiro erro.
func (b *Broker) Publish(ctx context.Context, topic string, e domain.Event) error {
	for _, h := range b.handlers[topic] {
		if err := h(ctx, e); err != nil {
			return err
		}
	}
	return nil
}
