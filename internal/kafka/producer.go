package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"user-management-system/internal/config"
	"user-management-system/internal/models"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	SendUserEvent(ctx context.Context, event *models.UserEvent) error
	SendUserEventBatch(ctx context.Context, events []*models.UserEvent) error
	Close() error
}

type kafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(cfg *config.KafkaConfig) (Producer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Snappy,
		MaxAttempts:  3,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &kafkaProducer{
		writer: writer,
		topic:  cfg.Topic,
	}, nil
}

func (p *kafkaProducer) SendUserEvent(ctx context.Context, event *models.UserEvent) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.UserID),
		Value: data,
		Time:  event.Timestamp,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "timestamp", Value: []byte(event.Timestamp.Format(time.RFC3339))},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	return nil
}

func (p *kafkaProducer) SendUserEventBatch(ctx context.Context, events []*models.UserEvent) error {
	if len(events) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(events))

	for _, event := range events {
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now()
		}

		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		msg := kafka.Message{
			Key:   []byte(event.UserID),
			Value: data,
			Time:  event.Timestamp,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(event.EventType)},
				{Key: "timestamp", Value: []byte(event.Timestamp.Format(time.RFC3339))},
			},
		}
		messages = append(messages, msg)
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("failed to write batch messages to Kafka: %w", err)
	}

	return nil
}

func (p *kafkaProducer) Close() error {
	return p.writer.Close()
}
