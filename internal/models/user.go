package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type User struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name      string             `bson:"name" json:"name"`
	Email     string             `bson:"email" json:"email"`
	City      string             `bson:"city" json:"city"`
	Phone     string             `bson:"phone" json:"phone"`
	Married   bool               `bson:"married" json:"married"`
	Status    UserStatus         `bson:"status" json:"status"`
	Metadata  map[string]string  `bson:"metadata,omitempty" json:"metadata,omitempty"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at" json:"updated_at"`
}

type UserStatus string

const (
	StatusActive    UserStatus = "active"
	StatusInactive  UserStatus = "inactive"
	StatusSuspended UserStatus = "suspended"
	StatusDeleted   UserStatus = "deleted"
)

type SearchFilter struct {
	Query   string
	City    *string
	Married *bool
	Status  *UserStatus
	Page    int
	Size    int
}

type UserEvent struct {
	EventType string                 `json:"event_type"`
	UserID    string                 `json:"user_id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

const (
	EventUserCreated = "user.created"
	EventUserUpdated = "user.updated"
	EventUserDeleted = "user.deleted"
	EventUserViewed  = "user.viewed"
)

type UserStats struct {
	TotalUsers     int64            `json:"total_users"`
	ActiveUsers    int64            `json:"active_users"`
	NewUsersToday  int64            `json:"new_users_today"`
	UsersByCity    map[string]int64 `json:"users_by_city"`
	AvgUsersPerDay float64          `json:"avg_users_per_day"`
}
