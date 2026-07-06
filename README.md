# event-driven-orders

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
