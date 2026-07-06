package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/memory"
)

func TestBrokerDeliversToSubscribers(t *testing.T) {
	b := memory.NewBroker()
	got := 0
	b.Subscribe("t", func(context.Context, domain.Event) error { got++; return nil })
	b.Subscribe("t", func(context.Context, domain.Event) error { got++; return nil })

	if err := b.Publish(context.Background(), "t", domain.Event{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got != 2 {
		t.Fatalf("handlers chamados = %d, quero 2", got)
	}
}

func TestBrokerStopsOnError(t *testing.T) {
	b := memory.NewBroker()
	boom := errors.New("boom")
	second := false
	b.Subscribe("t", func(context.Context, domain.Event) error { return boom })
	b.Subscribe("t", func(context.Context, domain.Event) error { second = true; return nil })

	if err := b.Publish(context.Background(), "t", domain.Event{}); !errors.Is(err, boom) {
		t.Fatalf("erro = %v, quero boom", err)
	}
	if second {
		t.Fatal("segundo handler não deveria rodar após erro")
	}
}

func TestBrokerNoSubscribers(t *testing.T) {
	if err := memory.NewBroker().Publish(context.Background(), "vazio", domain.Event{}); err != nil {
		t.Fatalf("tópico sem inscritos deveria ser no-op, veio: %v", err)
	}
}

func TestOrderStorePutGet(t *testing.T) {
	s := memory.NewOrderStore()
	if _, ok := s.Get("x"); ok {
		t.Fatal("pedido inexistente não deveria ser encontrado")
	}
	s.Put(domain.NewOrder("x", "SKU-1", 1, 100))
	if o, ok := s.Get("x"); !ok || o.ID != "x" {
		t.Fatalf("Get após Put falhou: %+v ok=%v", o, ok)
	}
}
