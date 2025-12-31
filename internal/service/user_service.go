package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"user-management-system/internal/kafka"
	"user-management-system/internal/models"
	"user-management-system/internal/repository"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UserService interface {
	CreateUser(ctx context.Context, user *models.User) error
	GetUserByID(ctx context.Context, id string) (*models.User, error)
	GetUsersByIDs(ctx context.Context, ids []string) ([]*models.User, error)
	UpdateUser(ctx context.Context, user *models.User) error
	DeleteUser(ctx context.Context, id string, soft bool) error
	ListUsers(ctx context.Context, page, pageSize int, sortBy string, ascending bool) ([]*models.User, int64, error)
	SearchUsers(ctx context.Context, filter *models.SearchFilter) ([]*models.User, int64, error)
	GetUserStats(ctx context.Context, startDate, endDate time.Time) (*models.UserStats, error)
}

type userService struct {
	repo     repository.UserRepository
	cache    repository.CacheRepository
	producer kafka.Producer
}

func NewUserService(
	repo repository.UserRepository,
	cache repository.CacheRepository,
	producer kafka.Producer,
) UserService {
	return &userService{
		repo:     repo,
		cache:    cache,
		producer: producer,
	}
}

func (s *userService) CreateUser(ctx context.Context, user *models.User) error {
	if err := s.repo.Create(ctx, user); err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	cacheKey := repository.UserCacheKey(user.ID.Hex())
	if err := s.cache.Set(ctx, cacheKey, user, 0); err != nil {
		log.Printf("Failed to cache user: %v", err)
	}

	if err := s.cache.DeletePattern(ctx, "users:list:*"); err != nil {
		log.Printf("Failed to invalidate list cache: %v", err)
	}

	event := &models.UserEvent{
		EventType: models.EventUserCreated,
		UserID:    user.ID.Hex(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"name":    user.Name,
			"email":   user.Email,
			"city":    user.City,
			"married": user.Married,
		},
	}

	if err := s.producer.SendUserEvent(ctx, event); err != nil {
		log.Printf("Failed to send user created event: %v", err)
	}

	return nil
}

func (s *userService) GetUserByID(ctx context.Context, id string) (*models.User, error) {
	cacheKey := repository.UserCacheKey(id)
	user, err := s.cache.Get(ctx, cacheKey)
	if err != nil {
		log.Printf("Cache error: %v", err)
	}

	if user != nil {
		log.Printf("Cache hit for user: %s", id)

		go func() {
			event := &models.UserEvent{
				EventType: models.EventUserViewed,
				UserID:    id,
				Timestamp: time.Now(),
				Data:      map[string]interface{}{"source": "cache"},
			}
			s.producer.SendUserEvent(context.Background(), event)
		}()

		return user, nil
	}

	log.Printf("Cache miss for user: %s", id)
	user, err = s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(ctx, cacheKey, user, 0); err != nil {
		log.Printf("Failed to cache user: %v", err)
	}

	event := &models.UserEvent{
		EventType: models.EventUserViewed,
		UserID:    id,
		Timestamp: time.Now(),
		Data:      map[string]interface{}{"source": "database"},
	}

	if err := s.producer.SendUserEvent(ctx, event); err != nil {
		log.Printf("Failed to send user viewed event: %v", err)
	}

	return user, nil
}

func (s *userService) GetUsersByIDs(ctx context.Context, ids []string) ([]*models.User, error) {
	users := make([]*models.User, 0, len(ids))
	missingIDs := make([]string, 0)

	for _, id := range ids {
		cacheKey := repository.UserCacheKey(id)
		user, err := s.cache.Get(ctx, cacheKey)
		if err != nil || user == nil {
			missingIDs = append(missingIDs, id)
			continue
		}
		users = append(users, user)
	}

	if len(missingIDs) > 0 {
		dbUsers, err := s.repo.GetByIDs(ctx, missingIDs)
		if err != nil {
			return nil, err
		}

		for _, user := range dbUsers {
			cacheKey := repository.UserCacheKey(user.ID.Hex())
			if err := s.cache.Set(ctx, cacheKey, user, 0); err != nil {
				log.Printf("Failed to cache user: %v", err)
			}
			users = append(users, user)
		}
	}

	return users, nil
}

func (s *userService) UpdateUser(ctx context.Context, user *models.User) error {
	if err := s.repo.Update(ctx, user); err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	cacheKey := repository.UserCacheKey(user.ID.Hex())
	if err := s.cache.Set(ctx, cacheKey, user, 0); err != nil {
		log.Printf("Failed to update cache: %v", err)
	}

	s.cache.DeletePattern(ctx, "users:list:*")
	s.cache.DeletePattern(ctx, "users:search:*")

	event := &models.UserEvent{
		EventType: models.EventUserUpdated,
		UserID:    user.ID.Hex(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"name":    user.Name,
			"email":   user.Email,
			"city":    user.City,
			"married": user.Married,
		},
	}

	if err := s.producer.SendUserEvent(ctx, event); err != nil {
		log.Printf("Failed to send user updated event: %v", err)
	}

	return nil
}

func (s *userService) DeleteUser(ctx context.Context, id string, soft bool) error {
	if err := s.repo.Delete(ctx, id, soft); err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	cacheKey := repository.UserCacheKey(id)
	if err := s.cache.Delete(ctx, cacheKey); err != nil {
		log.Printf("Failed to delete from cache: %v", err)
	}

	s.cache.DeletePattern(ctx, "users:list:*")
	s.cache.DeletePattern(ctx, "users:search:*")

	event := &models.UserEvent{
		EventType: models.EventUserDeleted,
		UserID:    id,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"soft_delete": soft,
		},
	}

	if err := s.producer.SendUserEvent(ctx, event); err != nil {
		log.Printf("Failed to send user deleted event: %v", err)
	}

	return nil
}

func (s *userService) ListUsers(ctx context.Context, page, pageSize int, sortBy string, ascending bool) ([]*models.User, int64, error) {
	users, total, err := s.repo.List(ctx, page, pageSize, sortBy, ascending)
	if err != nil {
		return nil, 0, err
	}

	return users, total, nil
}

func (s *userService) SearchUsers(ctx context.Context, filter *models.SearchFilter) ([]*models.User, int64, error) {
	users, total, err := s.repo.Search(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	return users, total, nil
}

func (s *userService) GetUserStats(ctx context.Context, startDate, endDate time.Time) (*models.UserStats, error) {
	cacheKey := repository.UserStatsCacheKey()

	exists, err := s.cache.Exists(ctx, cacheKey)
	if err == nil && exists {
		log.Println("Stats cache exists")
	}

	stats, err := s.repo.GetStats(ctx, startDate, endDate)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

func objectIDToString(id primitive.ObjectID) string {
	return id.Hex()
}
