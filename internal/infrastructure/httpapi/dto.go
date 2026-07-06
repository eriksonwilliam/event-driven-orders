package httpapi

import "github.com/eriksonwilliam/event-driven-orders/internal/domain"

type placeOrderRequest struct {
	SKU    string `json:"sku"`
	Qty    int    `json:"qty"`
	Amount int    `json:"amount"`
}

type orderResponse struct {
	ID      string   `json:"id"`
	SKU     string   `json:"sku"`
	Qty     int      `json:"qty"`
	Amount  int      `json:"amount"`
	Status  string   `json:"status"`
	History []string `json:"history"`
}

func toResponse(o domain.Order) orderResponse {
	history := make([]string, len(o.History))
	for i, t := range o.History {
		history[i] = string(t)
	}
	return orderResponse{
		ID:      o.ID,
		SKU:     o.SKU,
		Qty:     o.Qty,
		Amount:  o.Amount,
		Status:  string(o.Status),
		History: history,
	}
}
