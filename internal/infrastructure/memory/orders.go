package memory

import (
	"sync"

	"github.com/eriksonwilliam/event-driven-orders/internal/domain"
)

// OrderStore é o read model dos pedidos, em memória.
type OrderStore struct {
	mu     sync.RWMutex
	orders map[string]domain.Order
}

// NewOrderStore cria um read model vazio.
func NewOrderStore() *OrderStore {
	return &OrderStore{orders: map[string]domain.Order{}}
}

// Put insere ou atualiza um pedido.
func (s *OrderStore) Put(o domain.Order) {
	s.mu.Lock()
	s.orders[o.ID] = o
	s.mu.Unlock()
}

// Get devolve um pedido pelo id, indicando se existe.
func (s *OrderStore) Get(id string) (domain.Order, bool) {
	s.mu.RLock()
	o, ok := s.orders[id]
	s.mu.RUnlock()
	return o, ok
}
