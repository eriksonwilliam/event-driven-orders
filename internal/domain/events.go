package domain

// EventType identifica o tipo de um evento no fluxo do pedido.
type EventType string

const (
	OrderPlaced      EventType = "OrderPlaced"
	PaymentApproved  EventType = "PaymentApproved"
	PaymentDeclined  EventType = "PaymentDeclined"
	StockReserved    EventType = "StockReserved"
	StockUnavailable EventType = "StockUnavailable"
	OrderShipped     EventType = "OrderShipped"
)

// Event é a mensagem que trafega pelo broker entre os serviços.
type Event struct {
	Type    EventType `json:"type"`
	OrderID string    `json:"orderId"`
	SKU     string    `json:"sku"`
	Qty     int       `json:"qty"`
	Amount  int       `json:"amount"` // em centavos
}
