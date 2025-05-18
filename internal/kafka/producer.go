package kafka

import (
	"context"
	
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

type Producer struct {
	writer *kafka.Writer
}

// NewProducer initializes a Kafka writer using config from Viper
func NewProducer() *Producer {
	brokers := viper.GetStringSlice("kafka.brokers")
	topic   := viper.GetString("kafka.topic")

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		// Tune timeouts as needed
		BatchTimeout: 100 * time.Millisecond,
	}

	return &Producer{writer: w}
}

// Publish sends a single message payload to Kafka
func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
	return p.writer.WriteMessages(ctx, msg)
}

// Close the writer on shutdown
func (p *Producer) Close() error {
	return p.writer.Close()
}
