package httpapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
)

// Placer inicia o fluxo de um pedido (implementado pela saga).
type Placer interface {
	PlaceOrder(ctx context.Context, id, sku string, qty, amount int) error
}

// Handler expõe a API HTTP do serviço de pedidos.
type Handler struct {
	saga   Placer
	orders application.OrderStore
	nextID func() string
}

// NewHandler monta o handler com a saga, o read model e o gerador de ids.
func NewHandler(saga Placer, orders application.OrderStore, nextID func() string) *Handler {
	return &Handler{saga: saga, orders: orders, nextID: nextID}
}

func (h *Handler) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req placeOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "corpo inválido")
		return
	}
	if req.SKU == "" || req.Qty <= 0 || req.Amount <= 0 {
		writeErr(w, http.StatusBadRequest, "sku, qty (>0) e amount (>0) são obrigatórios")
		return
	}
	id := h.nextID()
	if err := h.saga.PlaceOrder(r.Context(), id, req.SKU, req.Qty, req.Amount); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	o, _ := h.orders.Get(id)
	writeJSON(w, http.StatusCreated, toResponse(o))
}

func (h *Handler) getOrder(w http.ResponseWriter, r *http.Request) {
	o, ok := h.orders.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "pedido não encontrado")
		return
	}
	writeJSON(w, http.StatusOK, toResponse(o))
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
