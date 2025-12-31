package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"user-management-system/internal/config"
	"user-management-system/internal/models"

	"github.com/redis/go-redis/v9"
)

type CacheRepository interface {
	Get(ctx context.Context, key string) (*models.User, error)
	Set(ctx context.Context, key string, user *models.User, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	DeletePattern(ctx context.Context, pattern string) error
	Exists(ctx context.Context, key string) (bool, error)
	IncrementCounter(ctx context.Context, key string) (int64, error)
	GetCounter(ctx context.Context, key string) (int64, error)
}

type redisCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisCache(cfg *config.RedisConfig) (CacheRepository, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &redisCache{
		client: client,
		ttl:    cfg.TTL,
	}, nil
}

func (c *redisCache) Get(ctx context.Context, key string) (*models.User, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get from cache: %w", err)
	}

	var user models.User
	if err := json.Unmarshal([]byte(val), &user); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user: %w", err)
	}

	return &user, nil
}

func (c *redisCache) Set(ctx context.Context, key string, user *models.User, ttl time.Duration) error {
	if ttl == 0 {
		ttl = c.ttl
	}

	data, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user: %w", err)
	}

	if err := c.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	return nil
}

func (c *redisCache) Delete(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete from cache: %w", err)
	}
	return nil
}

func (c *redisCache) DeletePattern(ctx context.Context, pattern string) error {
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		if err := c.client.Del(ctx, iter.Val()).Err(); err != nil {
			return fmt.Errorf("failed to delete key %s: %w", iter.Val(), err)
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}
	return nil
}

func (c *redisCache) Exists(ctx context.Context, key string) (bool, error) {
	count, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}
	return count > 0, nil
}

func (c *redisCache) IncrementCounter(ctx context.Context, key string) (int64, error) {
	val, err := c.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to increment counter: %w", err)
	}

	c.client.Expire(ctx, key, 24*time.Hour)

	return val, nil
}

func (c *redisCache) GetCounter(ctx context.Context, key string) (int64, error) {
	val, err := c.client.Get(ctx, key).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get counter: %w", err)
	}
	return val, nil
}

func (c *redisCache) Close() error {
	return c.client.Close()
}

func UserCacheKey(id string) string {
	return fmt.Sprintf("user:%s", id)
}

func UserListCacheKey(page, pageSize int) string {
	return fmt.Sprintf("users:list:%d:%d", page, pageSize)
}

func UserSearchCacheKey(query string) string {
	return fmt.Sprintf("users:search:%s", query)
}

func UserStatsCacheKey() string {
	return "users:stats"
}
