# Build. Use --build-arg TAGS=kafka para compilar o adapter real de mensageria.
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TAGS=""
RUN CGO_ENABLED=0 go build -tags "$TAGS" -o /out/app ./cmd/api

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
