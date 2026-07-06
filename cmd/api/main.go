package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/eriksonwilliam/event-driven-orders/internal/application"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/httpapi"
	"github.com/eriksonwilliam/event-driven-orders/internal/infrastructure/memory"
)

func main() {
	broker, closeBroker, err := newBroker()
	if err != nil {
		log.Fatalf("broker: %v", err)
	}
	defer closeBroker()

	orders := memory.NewOrderStore()
	limit := envInt("PAYMENT_LIMIT", 100000) // R$ 1.000,00 em centavos
	stock := map[string]int{"SKU-1": 10, "SKU-2": 3, "SKU-3": 0}
	saga := application.NewSaga(broker, orders, limit, stock)

	var seq int64
	nextID := func() string { return "ord-" + strconv.FormatInt(atomic.AddInt64(&seq, 1), 10) }

	addr := ":" + env("PORT", "8080")
	log.Printf("event-driven-orders ouvindo em %s (broker=%s)", addr, brokerName)
	if err := http.ListenAndServe(addr, httpapi.NewRouter(httpapi.NewHandler(saga, orders, nextID))); err != nil {
		log.Fatal(err)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
