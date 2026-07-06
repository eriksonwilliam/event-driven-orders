//go:build kafka

package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// Broker é o adapter real de mensageria sobre Apache Kafka. Cada Subscribe
// inicia um consumidor num goroutine; Publish produz o evento serializado em
// JSON no tópico correspondente.
type Broker struct {
	brokers []string
	writer  *kafkago.Writer

	mu      sync.Mutex
	readers []*kafkago.Reader
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewBroker cria o adapter apontando para os brokers informados (CSV).
func NewBroker(brokers string) *Broker {
	list := strings.Split(brokers, ",")
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{
		brokers: list,
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(list...),
			Balancer:               &kafkago.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Publish serializa e produz o evento no tópico.
func (b *Broker) Publish(ctx context.Context, topic string, e domain.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return b.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(e.OrderID),
		Value: payload,
	})
}

// Subscribe inicia um consumidor para o tópico e entrega cada evento ao handler.
func (b *Broker) Subscribe(topic string, h application.Handler) {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: b.brokers,
		Topic:   topic,
		GroupID: "orders-" + topic,
	})

	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()

	go func() {
		for {
			m, err := r.ReadMessage(b.ctx)
			if err != nil {
				return
			}
			var e domain.Event
			if err := json.Unmarshal(m.Value, &e); err != nil {
				continue
			}
			_ = h(b.ctx, e)
		}
	}()
}

// Close encerra o produtor e todos os consumidores.
func (b *Broker) Close() {
	b.cancel()
	_ = b.writer.Close()
	b.mu.Lock()
	for _, r := range b.readers {
		_ = r.Close()
	}
	b.mu.Unlock()
}
