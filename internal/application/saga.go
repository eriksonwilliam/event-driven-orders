package application

import (
	"context"
	"sync"

	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
)

// Saga orquestra o fluxo do pedido por coreografia: cada passo reage a um
// evento e publica o próximo (pagamento → estoque → envio). É registrada nos
// tópicos do broker; a projeção do read model roda antes de cada passo.
type Saga struct {
	broker       Broker
	orders       OrderStore
	paymentLimit int

	mu    sync.Mutex
	stock map[string]int
}

// NewSaga registra a projeção e os handlers de pagamento, estoque e envio.
// A projeção é inscrita primeiro para que o read model reflita cada evento
// antes de o próximo passo avançar (despacho síncrono do broker in-process).
func NewSaga(b Broker, orders OrderStore, paymentLimitCents int, stock map[string]int) *Saga {
	s := &Saga{broker: b, orders: orders, paymentLimit: paymentLimitCents, stock: stock}

	for _, t := range []domain.EventType{
		domain.PaymentApproved, domain.PaymentDeclined,
		domain.StockReserved, domain.StockUnavailable, domain.OrderShipped,
	} {
		b.Subscribe(string(t), s.project)
	}

	b.Subscribe(string(domain.OrderPlaced), s.onOrderPlaced)
	b.Subscribe(string(domain.PaymentApproved), s.onPaymentApproved)
	b.Subscribe(string(domain.StockReserved), s.onStockReserved)
	return s
}

// PlaceOrder inicia o fluxo publicando o evento OrderPlaced.
func (s *Saga) PlaceOrder(ctx context.Context, id, sku string, qty, amount int) error {
	return s.broker.Publish(ctx, string(domain.OrderPlaced), domain.Event{
		Type: domain.OrderPlaced, OrderID: id, SKU: sku, Qty: qty, Amount: amount,
	})
}

func (s *Saga) onOrderPlaced(ctx context.Context, e domain.Event) error {
	s.orders.Put(domain.NewOrder(e.OrderID, e.SKU, e.Qty, e.Amount))

	next := domain.PaymentApproved
	if !domain.Authorize(e.Amount, s.paymentLimit) {
		next = domain.PaymentDeclined
	}
	return s.emit(ctx, next, e)
}

func (s *Saga) onPaymentApproved(ctx context.Context, e domain.Event) error {
	s.mu.Lock()
	ok := s.stock[e.SKU] >= e.Qty
	if ok {
		s.stock[e.SKU] -= e.Qty
	}
	s.mu.Unlock()

	next := domain.StockReserved
	if !ok {
		next = domain.StockUnavailable
	}
	return s.emit(ctx, next, e)
}

func (s *Saga) onStockReserved(ctx context.Context, e domain.Event) error {
	return s.emit(ctx, domain.OrderShipped, e)
}

// project atualiza o read model do pedido a partir de um evento.
func (s *Saga) project(_ context.Context, e domain.Event) error {
	if o, ok := s.orders.Get(e.OrderID); ok {
		o.Apply(e.Type)
		s.orders.Put(o)
	}
	return nil
}

func (s *Saga) emit(ctx context.Context, t domain.EventType, src domain.Event) error {
	ev := src
	ev.Type = t
	return s.broker.Publish(ctx, string(t), ev)
}
