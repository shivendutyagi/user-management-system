package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"user-management-system/internal/config"
	"user-management-system/internal/models"

	"github.com/segmentio/kafka-go"
)

type EventHandler func(ctx context.Context, event *models.UserEvent) error

type Consumer interface {
	Start(ctx context.Context, handler EventHandler) error
	Close() error
}

type kafkaConsumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg *config.KafkaConfig) (Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
		ErrorLogger:    kafka.LoggerFunc(log.Printf),
	})

	return &kafkaConsumer{
		reader: reader,
	}, nil
}

func (c *kafkaConsumer) Start(ctx context.Context, handler EventHandler) error {
	log.Println("Starting Kafka consumer...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down Kafka consumer...")
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				log.Printf("Error fetching message: %v", err)
				continue
			}

			if err := c.processMessage(ctx, msg, handler); err != nil {
				log.Printf("Error processing message: %v", err)
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			}
		}
	}
}

func (c *kafkaConsumer) processMessage(ctx context.Context, msg kafka.Message, handler EventHandler) error {
	var event models.UserEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	log.Printf("Processing event: %s for user: %s", event.EventType, event.UserID)

	if err := handler(ctx, &event); err != nil {
		return fmt.Errorf("handler error: %w", err)
	}

	return nil
}

func (c *kafkaConsumer) Close() error {
	return c.reader.Close()
}
