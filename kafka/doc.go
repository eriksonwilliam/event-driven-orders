// Package kafka contém o adapter real de mensageria sobre Apache Kafka. Ele é
// compilado apenas com a build tag "kafka" (por exigir um broker externo e o
// cliente kafka-go), ficando fora da cobertura de testes. A CI valida que ele
// compila; o docker-compose (perfil "real") o sobe contra um Kafka em KRaft.
package kafka
