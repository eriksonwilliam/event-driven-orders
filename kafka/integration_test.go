//go:build kafka

package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// A real broker verifies failure/restart replay and local fan-out. Rebalance and
// multi-partition recovery remain separate experiments.
func TestIntegrationFailureIsReplayedThenCommitted(t *testing.T) {
	address := os.Getenv("KAFKA_INTEGRATION_BROKER")
	if address == "" {
		t.Skip("set KAFKA_INTEGRATION_BROKER to run against real Kafka")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	topic := fmt.Sprintf("orders-integration-%d", time.Now().UnixNano())
	conn, err := kafkago.DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	admin, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	defer admin.DeleteTopics(topic)
	writer := &kafkago.Writer{Addr: kafkago.TCP(address), Topic: topic, RequiredAcks: kafkago.RequireAll}
	defer writer.Close()
	if err := writer.WriteMessages(ctx, kafkago.Message{Value: []byte(`{"orderId":"replay-me"}`)}); err != nil {
		t.Fatal(err)
	}
	newReader := func() *kafkago.Reader {
		return kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: []string{address}, Topic: topic, GroupID: topic, StartOffset: kafkago.FirstOffset,
			MinBytes: 1, MaxWait: 100 * time.Millisecond, CommitInterval: 0,
		})
	}
	failure := errors.New("transient handler failure")
	first := newReader()
	err = consume(ctx, first, func() []application.Handler {
		return []application.Handler{
			func(context.Context, domain.Event) error { return failure },
		}
	})
	first.Close()
	if !errors.Is(err, failure) {
		t.Fatalf("expected handler error, got %v", err)
	}
	second := newReader()
	var calls []string
	wrapped := &commitSignalReader{messageReader: second, committed: make(chan struct{}, 1)}
	replayCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- consume(replayCtx, wrapped, func() []application.Handler {
			return []application.Handler{
				func(_ context.Context, e domain.Event) error {
					if e.OrderID != "replay-me" {
						return fmt.Errorf("unexpected order %s", e.OrderID)
					}
					calls = append(calls, "projection")
					return nil
				},
				func(context.Context, domain.Event) error { calls = append(calls, "saga"); return nil },
			}
		})
	}()
	select {
	case <-wrapped.committed:
	case <-ctx.Done():
		stop()
		second.Close()
		<-done
		t.Fatal(ctx.Err())
	}
	stop()
	second.Close()
	<-done
	if len(calls) != 2 || calls[0] != "projection" || calls[1] != "saga" {
		t.Fatalf("calls=%v", calls)
	}
	// Same group resumes after the committed event, rather than replaying it.
	third := newReader()
	defer third.Close()
	checkCtx, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	if m, err := third.FetchMessage(checkCtx); err == nil {
		t.Fatalf("committed event replayed: %s", m.Value)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected read error: %v", err)
	}
}

type commitSignalReader struct {
	messageReader
	committed chan struct{}
}

func (r *commitSignalReader) CommitMessages(ctx context.Context, messages ...kafkago.Message) error {
	if err := r.messageReader.CommitMessages(ctx, messages...); err != nil {
		return err
	}
	select {
	case r.committed <- struct{}{}:
	default:
	}
	return nil
}
