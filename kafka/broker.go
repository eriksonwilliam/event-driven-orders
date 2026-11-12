//go:build kafka

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

type messageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

// Broker uses one consumer per topic and dispatches each event to all local handlers.
// Register all handlers before publishing events. A failed consumer requires restart.
type Broker struct {
	writer *kafkago.Writer

	mu        sync.Mutex
	readers   map[string]messageReader
	handlers  map[string][]application.Handler
	newReader func(string) messageReader
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewBroker(brokers string) *Broker {
	list := strings.Split(brokers, ",")
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(list...),
			Balancer:               &kafkago.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
		readers:  make(map[string]messageReader),
		handlers: make(map[string][]application.Handler),
		newReader: func(topic string) messageReader {
			return kafkago.NewReader(kafkago.ReaderConfig{
				Brokers: list,
				Topic:   topic,
				GroupID: "orders-" + topic,
			})
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

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

func (b *Broker) Subscribe(topic string, h application.Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[topic] = append(b.handlers[topic], h)
	if _, exists := b.readers[topic]; exists {
		return
	}
	r := b.newReader(topic)
	b.readers[topic] = r
	go func() {
		err := consume(b.ctx, r, func() []application.Handler {
			b.mu.Lock()
			defer b.mu.Unlock()
			return append([]application.Handler(nil), b.handlers[topic]...)
		})
		if err != nil && b.ctx.Err() == nil {
			log.Printf("Kafka consumer stopped for topic %s: %v", topic, err)
		}
	}()
}

// consume never fetches a later message after a failure, preventing a later commit
// from acknowledging a failed earlier offset in the same partition.
func consume(ctx context.Context, r messageReader, handlers func() []application.Handler) error {
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var e domain.Event
		if err := json.Unmarshal(m.Value, &e); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		for _, h := range handlers() {
			if err := h(ctx, e); err != nil {
				return fmt.Errorf("handle event: %w", err)
			}
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("commit event: %w", err)
		}
	}
}

func (b *Broker) Close() {
	b.cancel()
	_ = b.writer.Close()
	b.mu.Lock()
	readers := make([]messageReader, 0, len(b.readers))
	for _, r := range b.readers {
		readers = append(readers, r)
	}
	b.mu.Unlock()
	for _, r := range readers {
		_ = r.Close()
	}
}
