package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/httpapi"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/memory"
)

// wiring real: saga + broker in-process + read model.
func realServer() http.Handler {
	broker := memory.NewBroker()
	orders := memory.NewOrderStore()
	saga := application.NewSaga(broker, orders, 100000, map[string]int{"SKU-1": 10})
	return httpapi.NewRouter(httpapi.NewHandler(saga, orders, fixedID("ord-1")))
}

func fixedID(id string) func() string { return func() string { return id } }

// errPlacer força o caminho de erro 500 no placeOrder.
type errPlacer struct{}

func (errPlacer) PlaceOrder(context.Context, string, string, int, int) error {
	return errors.New("broker fora do ar")
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPlaceOrderShips(t *testing.T) {
	h := realServer()
	rec := do(t, h, http.MethodPost, "/orders", `{"sku":"SKU-1","qty":2,"amount":4990}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("code = %d, quero 201 (body=%s)", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "SHIPPED") {
		t.Fatalf("esperava status SHIPPED, veio %s", rec.Body)
	}
}

func TestPlaceOrderInvalidJSON(t *testing.T) {
	rec := do(t, realServer(), http.MethodPost, "/orders", `{`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, quero 400", rec.Code)
	}
}

func TestPlaceOrderMissingFields(t *testing.T) {
	rec := do(t, realServer(), http.MethodPost, "/orders", `{"sku":"","qty":0,"amount":0}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, quero 400", rec.Code)
	}
}

func TestPlaceOrderSagaError(t *testing.T) {
	orders := memory.NewOrderStore()
	h := httpapi.NewRouter(httpapi.NewHandler(errPlacer{}, orders, fixedID("x")))
	rec := do(t, h, http.MethodPost, "/orders", `{"sku":"SKU-1","qty":1,"amount":100}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, quero 500", rec.Code)
	}
}

func TestGetOrderFoundAndNotFound(t *testing.T) {
	h := realServer()
	do(t, h, http.MethodPost, "/orders", `{"sku":"SKU-1","qty":1,"amount":100}`)

	if rec := do(t, h, http.MethodGet, "/orders/ord-1", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET existente = %d, quero 200", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/orders/naoexiste", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET inexistente = %d, quero 404", rec.Code)
	}
}

func TestHealthAndDocs(t *testing.T) {
	h := realServer()
	if rec := do(t, h, http.MethodGet, "/health", ""); rec.Code != http.StatusOK {
		t.Fatalf("/health = %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/openapi.json", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "openapi") {
		t.Fatalf("/openapi.json inválido: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/docs", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "swagger-ui") {
		t.Fatalf("/docs inválido: %d", rec.Code)
	}
}
