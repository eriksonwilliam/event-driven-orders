package httpapi

import "net/http"

// NewRouter monta as rotas HTTP do serviço.
func NewRouter(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", h.placeOrder)
	mux.HandleFunc("GET /orders/{id}", h.getOrder)
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /openapi.json", h.openAPI)
	mux.HandleFunc("GET /docs", h.swaggerUI)
	return mux
}
