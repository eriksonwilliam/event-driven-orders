package application_test

import (
	"context"
	"testing"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/memory"
)

func setup(limit int, stock map[string]int) (*application.Saga, *memory.OrderStore) {
	broker := memory.NewBroker()
	orders := memory.NewOrderStore()
	saga := application.NewSaga(broker, orders, limit, stock)
	return saga, orders
}

func TestHappyPathShips(t *testing.T) {
	saga, orders := setup(100000, map[string]int{"SKU-1": 5})
	if err := saga.PlaceOrder(context.Background(), "ord-1", "SKU-1", 2, 4990); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	o, ok := orders.Get("ord-1")
	if !ok || o.Status != domain.StatusShipped {
		t.Fatalf("status = %q (ok=%v), quero SHIPPED", o.Status, ok)
	}
}

func TestPaymentDeclinedAboveLimit(t *testing.T) {
	saga, orders := setup(1000, map[string]int{"SKU-1": 5})
	_ = saga.PlaceOrder(context.Background(), "ord-2", "SKU-1", 1, 5000)
	o, _ := orders.Get("ord-2")
	if o.Status != domain.StatusDeclined {
		t.Fatalf("status = %q, quero PAYMENT_DECLINED", o.Status)
	}
}

func TestOutOfStock(t *testing.T) {
	saga, orders := setup(100000, map[string]int{"SKU-1": 1})
	_ = saga.PlaceOrder(context.Background(), "ord-3", "SKU-1", 3, 4990)
	o, _ := orders.Get("ord-3")
	if o.Status != domain.StatusOutOfStock {
		t.Fatalf("status = %q, quero OUT_OF_STOCK", o.Status)
	}
}
