//go:build kafka

package kafka

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

type fakeReader struct {
	messages  []kafkago.Message
	fetched   int
	commits   []int64
	commitErr error
}

func (r *fakeReader) FetchMessage(context.Context) (kafkago.Message, error) {
	if r.fetched == len(r.messages) {
		return kafkago.Message{}, io.EOF
	}
	m := r.messages[r.fetched]
	r.fetched++
	return m, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	for _, m := range messages {
		r.commits = append(r.commits, m.Offset)
	}
	return r.commitErr
}

func (r *fakeReader) Close() error { return nil }

func eventMessage(offset int64) kafkago.Message {
	return kafkago.Message{Offset: offset, Value: []byte(`{"orderId":"order-1"}`)}
}

func TestDispatchesToAllHandlersBeforeCommit(t *testing.T) {
	r := &fakeReader{messages: []kafkago.Message{eventMessage(1)}}
	var calls []string
	handlers := []application.Handler{
		func(context.Context, domain.Event) error { calls = append(calls, "projection"); return nil },
		func(context.Context, domain.Event) error {
			if len(r.commits) != 0 {
				t.Fatal("message committed before all handlers completed")
			}
			calls = append(calls, "saga")
			return nil
		},
	}
	err := consume(context.Background(), r, func() []application.Handler { return handlers })
	if !errors.Is(err, io.EOF) || !reflect.DeepEqual(calls, []string{"projection", "saga"}) || !reflect.DeepEqual(r.commits, []int64{1}) {
		t.Fatalf("err=%v calls=%v commits=%v", err, calls, r.commits)
	}
}

func TestHandlerFailureStopsBeforeCommitOrNextFetch(t *testing.T) {
	r := &fakeReader{messages: []kafkago.Message{eventMessage(1), eventMessage(2)}}
	failure := errors.New("database unavailable")
	laterCalled := false
	handlers := []application.Handler{
		func(context.Context, domain.Event) error { return failure },
		func(context.Context, domain.Event) error { laterCalled = true; return nil },
	}
	err := consume(context.Background(), r, func() []application.Handler { return handlers })
	if !errors.Is(err, failure) || len(r.commits) != 0 || r.fetched != 1 || laterCalled {
		t.Fatalf("err=%v commits=%v fetched=%d later=%v", err, r.commits, r.fetched, laterCalled)
	}
}

func TestMalformedEventIsNotCommitted(t *testing.T) {
	r := &fakeReader{messages: []kafkago.Message{{Value: []byte("invalid JSON")}, eventMessage(2)}}
	called := false
	err := consume(context.Background(), r, func() []application.Handler {
		return []application.Handler{func(context.Context, domain.Event) error { called = true; return nil }}
	})
	if err == nil || len(r.commits) != 0 || r.fetched != 1 || called {
		t.Fatalf("err=%v commits=%v fetched=%d called=%v", err, r.commits, r.fetched, called)
	}
}

func TestCommitFailureStopsBeforeNextFetch(t *testing.T) {
	failure := errors.New("commit unavailable")
	r := &fakeReader{messages: []kafkago.Message{eventMessage(1), eventMessage(2)}, commitErr: failure}
	err := consume(context.Background(), r, func() []application.Handler { return nil })
	if !errors.Is(err, failure) || r.fetched != 1 {
		t.Fatalf("err=%v fetched=%d", err, r.fetched)
	}
}

func TestSubscribersShareOneReaderPerTopic(t *testing.T) {
	created := 0
	b := &Broker{
		ctx:       context.Background(),
		readers:   make(map[string]messageReader),
		handlers:  make(map[string][]application.Handler),
		newReader: func(string) messageReader { created++; return &fakeReader{} },
	}
	h := func(context.Context, domain.Event) error { return nil }
	b.Subscribe("orders", h)
	b.Subscribe("orders", h)
	b.mu.Lock()
	defer b.mu.Unlock()
	if created != 1 || len(b.handlers["orders"]) != 2 {
		t.Fatalf("readers=%d handlers=%d", created, len(b.handlers["orders"]))
	}
}
