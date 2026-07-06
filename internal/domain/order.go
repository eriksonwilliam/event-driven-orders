package domain

// Status representa o estado atual do pedido no read model.
type Status string

const (
	StatusPlaced     Status = "PLACED"
	StatusApproved   Status = "PAYMENT_APPROVED"
	StatusDeclined   Status = "PAYMENT_DECLINED"
	StatusReserved   Status = "STOCK_RESERVED"
	StatusOutOfStock Status = "OUT_OF_STOCK"
	StatusShipped    Status = "SHIPPED"
)

// Order é a projeção do pedido, com o histórico dos eventos aplicados.
type Order struct {
	ID      string      `json:"id"`
	SKU     string      `json:"sku"`
	Qty     int         `json:"qty"`
	Amount  int         `json:"amount"`
	Status  Status      `json:"status"`
	History []EventType `json:"history"`
}

// NewOrder cria um pedido recém-colocado (estado inicial).
func NewOrder(id, sku string, qty, amount int) Order {
	return Order{
		ID:      id,
		SKU:     sku,
		Qty:     qty,
		Amount:  amount,
		Status:  StatusPlaced,
		History: []EventType{OrderPlaced},
	}
}

// Apply evolui o estado do pedido a partir de um evento subsequente.
func (o *Order) Apply(t EventType) {
	o.History = append(o.History, t)
	switch t {
	case PaymentApproved:
		o.Status = StatusApproved
	case PaymentDeclined:
		o.Status = StatusDeclined
	case StockReserved:
		o.Status = StatusReserved
	case StockUnavailable:
		o.Status = StatusOutOfStock
	case OrderShipped:
		o.Status = StatusShipped
	}
}
