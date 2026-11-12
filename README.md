# event-driven-orders

A Go reference project for an order saga, in-process events, and an optional
Kafka adapter. The Kafka adapter uses one consumer per topic and dispatches each
event to all handlers registered in that process, in registration order. This
prevents local saga and projection handlers from competing in the same group.
Register the complete handler set before publishers start sending messages.

### Kafka acknowledgment and failure policy

`FetchMessage` reads without automatically acknowledging the event. The adapter
calls `CommitMessages` only after every local handler succeeds. A decoding,
handler, fetch, or commit error stops that topic's consumer and logs the failure;
it does not fetch a later message that could commit past a failed earlier offset.
The process must be restarted after the underlying failure is resolved. There is
no automatic retry, dead-letter queue, or consumer-health endpoint in this version.
The HTTP health endpoint alone does not prove that consumers are still running.

Delivery remains at least once: a failure after a side effect, or an ambiguous
commit result, can lead to replay. Handlers still need durable idempotency.
The in-memory saga/store is a reference implementation; it does not provide a
transactional outbox, crash-safe business state, or exactly-once business effects.
Malformed messages remain uncommitted and need an explicit operational resolution.
Fake-reader regression tests verify handler fan-out, acknowledgment order, and
stop-on-failure behavior without claiming real-broker integration coverage.

```bash
go test ./internal/...
go test -race -tags kafka ./kafka/...
go build -tags kafka ./...
```

### Documentação em português


Microserviços **orientados a eventos** (event-driven) em **Go**: um fluxo de
pedido resolvido por **coreografia de saga** — pagamento → reserva de estoque →
envio — em que cada passo **reage a um evento e publica o próximo**. O broker de
mensageria fica atrás de uma **porta**: um adapter **in-process** (síncrono,
determinístico, usado nos testes) e um adapter **Apache Kafka** de verdade.
**Cobertura de testes 100%.**

```
POST /orders ──▶ OrderPlaced
                    │
             (pagamento) ──▶ PaymentApproved ──┐  ou  PaymentDeclined ✗
                                                │
                                     (estoque) ─┴▶ StockReserved ──┐  ou  StockUnavailable ✗
                                                                   │
                                                        (envio) ───┴▶ OrderShipped ✓
```

## Destaques

- **Coreografia de saga** — sem orquestrador central: cada serviço (pagamento,
  estoque, envio) escuta um evento e emite o próximo. Baixo acoplamento.
- **Broker atrás de uma porta** — a interface `Broker` (publish/subscribe) isola
  o domínio da mensageria. Troca o adapter sem tocar na lógica.
- **In-process e Kafka** — o adapter in-process despacha **inline** (síncrono e
  determinístico — ótimo para testes/dev); o adapter **Kafka** (KRaft) é
  assíncrono de verdade, selecionado por *build tag*.
- **Read model projetado dos eventos** — o estado do pedido (`PLACED` →
  `SHIPPED` / `PAYMENT_DECLINED` / `OUT_OF_STOCK`) é uma **projeção** do fluxo,
  com o histórico de eventos aplicado.
- **Arquitetura hexagonal** — domínio (regras de pagamento/estoque/pedido) →
  aplicação (saga + portas) → infraestrutura (broker, read model, HTTP).
- **Cobertura 100%** — verificada na CI. O adapter Kafka exige broker externo,
  então fica fora da métrica (a CI valida que ele **compila**).
- **OpenAPI/Swagger** — documentação interativa em `/docs`.

## Stack

- **Linguagem:** Go 1.24
- **Mensageria:** porta `Broker` + adapters in-process e Apache Kafka (`kafka-go`)
- **HTTP:** `net/http` (roteamento com padrões do Go 1.22)
- **Testes/cobertura:** `go test -cover` (gate de 100%)

## Arquitetura

```
cmd/api/                # bootstrap; seleção do broker por build tag
internal/
├── domain/             # eventos, pedido (máquina de estados), regra de pagamento
├── application/        # Saga (coreografia) + portas (Broker, OrderStore)
└── infrastructure/
    ├── memory/         # broker in-process (síncrono) + read model
    └── httpapi/        # rotas, DTOs, Swagger
kafka/                  # adapter real (Apache Kafka) — build tag "kafka"
```

## Como rodar

```bash
# broker in-process (padrão, sem dependências):
go run ./cmd/api            # http://localhost:8080  (Swagger em /docs)

# coloca um pedido e acompanha a saga rodar até o fim:
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-1","qty":2,"amount":4990}'
# -> { "status": "SHIPPED", "history": ["OrderPlaced","PaymentApproved","StockReserved","OrderShipped"] }

curl localhost:8080/orders/ord-1
```

Estoque inicial: `SKU-1` (10), `SKU-2` (3), `SKU-3` (0). Limite de pagamento:
`PAYMENT_LIMIT` (padrão R$ 1.000,00 em centavos). Peça `SKU-3` para ver
`OUT_OF_STOCK`; passe do limite para ver `PAYMENT_DECLINED`.

### Docker

```bash
docker compose up --build            # modo in-process

# com Kafka real (KRaft), sobe broker + a API compilada com o adapter:
docker compose --profile real up --build   # API em :8081, Kafka em :9092
```

## Endpoints

| Método | Rota            | Descrição                                   |
| ------ | --------------- | ------------------------------------------- |
| POST   | `/orders`       | Coloca um pedido e dispara a saga           |
| GET    | `/orders/{id}`  | Estado atual do pedido + histórico          |
| GET    | `/health`       | Health check                                |
| GET    | `/docs`         | Swagger UI                                  |

## Testes e cobertura

```bash
go test ./internal/... -covermode=atomic -coverprofile=coverage.out
go tool cover -func=coverage.out        # total: 100.0%

# validar que o adapter Kafka compila (não roda sem broker):
go build -tags kafka ./...
```

## Por que "coreografia" e não "orquestração"?

Na **orquestração**, um serviço central manda em todos os passos. Na
**coreografia** (esta), cada serviço só conhece os eventos que consome e produz —
adicionar um passo (ex.: antifraude, nota fiscal) é **inscrever um novo handler**,
sem mexer nos outros. O custo é a visibilidade: o "fluxo" não está num lugar só,
está distribuído. O read model projetado aqui devolve essa visibilidade.

## Licença

MIT.

## Real Kafka regression (2026-11-12)

The `Kafka integration` workflow starts Apache Kafka 3.9.0 and runs:

```bash
KAFKA_INTEGRATION_BROKER=localhost:9092 go test -race -tags kafka ./kafka/... -run TestIntegration -count=1 -timeout=120s
```

Use a disposable local broker: the test creates and removes its own uniquely named
topic. It verifies that handler failure leaves the message available after reader
restart, that both local handlers run, and that the same group resumes after a
successful explicit commit. Without the environment variable this integration test
skips; the fake-reader tests still run. The compose app listener advertises `kafka`,
so use the CI broker's host-accessible listener or configure your local listener
for host execution. Do not infer multi-partition/rebalance guarantees from this
single-partition case. No retry, DLQ, durable inbox or outbox is introduced here.

### Durable processing roadmap

1. Introduce stable event IDs, a PostgreSQL schema and repository transactions.
2. Persist order/saga state and test recovery after process restart.
3. Persist inbox deduplication and domain effects in the same transaction; test races.
4. Persist outbox with business state, then implement relay leases/retries and recovery.
5. Test crash after publish/before marking sent and duplicate delivery against Kafka.

Each step needs executable failure tests before the portfolio describes it as shipped.
