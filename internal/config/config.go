package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Server  ServerConfig
	MongoDB MongoDBConfig
	Redis   RedisConfig
	Kafka   KafkaConfig
	Logger  LoggerConfig
	Metrics MetricsConfig
}

type ServerConfig struct {
	Port            int
	GracefulTimeout time.Duration
}

type MongoDBConfig struct {
	URI            string
	Database       string
	MaxPoolSize    uint64
	MinPoolSize    uint64
	ConnectTimeout time.Duration
}

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	TTL          time.Duration
}

type KafkaConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	BatchSize     int
}

type LoggerConfig struct {
	Level      string
	OutputPath string
}

type MetricsConfig struct {
	Port int
	Path string
}

func LoadConfig() (*Config, error) {
	return &Config{
		Server: ServerConfig{
			Port:            getEnvAsInt("SERVER_PORT", 50051),
			GracefulTimeout: getEnvAsDuration("GRACEFUL_TIMEOUT", 30*time.Second),
		},
		MongoDB: MongoDBConfig{
			URI:            getEnv("MONGO_URI", "mongodb://localhost:27017"),
			Database:       getEnv("MONGO_DATABASE", "userdb"),
			MaxPoolSize:    uint64(getEnvAsInt("MONGO_MAX_POOL_SIZE", 100)),
			MinPoolSize:    uint64(getEnvAsInt("MONGO_MIN_POOL_SIZE", 10)),
			ConnectTimeout: getEnvAsDuration("MONGO_CONNECT_TIMEOUT", 10*time.Second),
		},
		Redis: RedisConfig{
			Addr:         getEnv("REDIS_ADDR", "localhost:6379"),
			Password:     getEnv("REDIS_PASSWORD", ""),
			DB:           getEnvAsInt("REDIS_DB", 0),
			PoolSize:     getEnvAsInt("REDIS_POOL_SIZE", 100),
			MinIdleConns: getEnvAsInt("REDIS_MIN_IDLE_CONNS", 10),
			MaxRetries:   getEnvAsInt("REDIS_MAX_RETRIES", 3),
			TTL:          getEnvAsDuration("REDIS_TTL", 1*time.Hour),
		},
		Kafka: KafkaConfig{
			Brokers:       []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
			Topic:         getEnv("KAFKA_TOPIC", "user-events"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "analytics-service"),
			BatchSize:     getEnvAsInt("KAFKA_BATCH_SIZE", 100),
		},
		Logger: LoggerConfig{
			Level:      getEnv("LOG_LEVEL", "info"),
			OutputPath: getEnv("LOG_OUTPUT", "stdout"),
		},
		Metrics: MetricsConfig{
			Port: getEnvAsInt("METRICS_PORT", 9090),
			Path: getEnv("METRICS_PATH", "/metrics"),
		},
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := getEnv(key, "")
	if value, err := time.ParseDuration(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}
	if c.MongoDB.URI == "" {
		return fmt.Errorf("MongoDB URI is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("Kafka brokers are required")
	}
	return nil
}
