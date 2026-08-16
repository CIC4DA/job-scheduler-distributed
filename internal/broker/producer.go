package broker

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
)


type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer{
	return &Producer{
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),
			Topic: topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// Publish JSON-encodes v and writes it to Kafka under the given key.
// key matters for ordering: Kafka guarantees messages with the same key
// land on the same partition, so they're read back in the order they were written.
func (p *Producer) Publish(ctx context.Context, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(key),
		Value: body,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}