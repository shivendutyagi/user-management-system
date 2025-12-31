package repository

import (
	"context"
	"fmt"
	"time"

	"user-management-system/internal/config"
	"user-management-system/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type UserRepository interface {
	Create(ctx context.Context, user *models.User) error
	GetByID(ctx context.Context, id string) (*models.User, error)
	GetByIDs(ctx context.Context, ids []string) ([]*models.User, error)
	Update(ctx context.Context, user *models.User) error
	Delete(ctx context.Context, id string, soft bool) error
	List(ctx context.Context, page, pageSize int, sortBy string, ascending bool) ([]*models.User, int64, error)
	Search(ctx context.Context, filter *models.SearchFilter) ([]*models.User, int64, error)
	GetStats(ctx context.Context, startDate, endDate time.Time) (*models.UserStats, error)
}

type mongoRepository struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func NewMongoRepository(cfg *config.MongoDBConfig) (UserRepository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetMaxConnIdleTime(5 * time.Minute).
		SetRetryWrites(true).
		SetRetryReads(true)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	collection := client.Database(cfg.Database).Collection("users")

	if err := createIndexes(ctx, collection); err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	return &mongoRepository{
		client:     client,
		collection: collection,
	}, nil
}

func createIndexes(ctx context.Context, collection *mongo.Collection) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "email", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "city", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "created_at", Value: -1}},
		},
		{
			Keys: bson.D{
				{Key: "name", Value: "text"},
				{Key: "email", Value: "text"},
				{Key: "city", Value: "text"},
			},
		},
	}

	_, err := collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *mongoRepository) Create(ctx context.Context, user *models.User) error {
	user.ID = primitive.NewObjectID()
	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	user.Status = models.StatusActive

	_, err := r.collection.InsertOne(ctx, user)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("user with email %s already exists", user.Email)
		}
		return fmt.Errorf("failed to create user: %w", err)
	}
	return nil
}

func (r *mongoRepository) GetByID(ctx context.Context, id string) (*models.User, error) {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID: %w", err)
	}

	var user models.User
	err = r.collection.FindOne(ctx, bson.M{"_id": objectID}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("user not found")
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	return &user, nil
}

func (r *mongoRepository) GetByIDs(ctx context.Context, ids []string) ([]*models.User, error) {
	objectIDs := make([]primitive.ObjectID, 0, len(ids))
	for _, id := range ids {
		objectID, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			continue
		}
		objectIDs = append(objectIDs, objectID)
	}

	cursor, err := r.collection.Find(ctx, bson.M{"_id": bson.M{"$in": objectIDs}})
	if err != nil {
		return nil, fmt.Errorf("failed to get users: %w", err)
	}
	defer cursor.Close(ctx)

	var users []*models.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("failed to decode users: %w", err)
	}
	return users, nil
}

func (r *mongoRepository) Update(ctx context.Context, user *models.User) error {
	user.UpdatedAt = time.Now()

	update := bson.M{"$set": user}
	result, err := r.collection.UpdateOne(
		ctx,
		bson.M{"_id": user.ID},
		update,
	)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("user not found")
	}
	return nil
}

func (r *mongoRepository) Delete(ctx context.Context, id string, soft bool) error {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid user ID: %w", err)
	}

	if soft {
		update := bson.M{
			"$set": bson.M{
				"status":     models.StatusDeleted,
				"updated_at": time.Now(),
			},
		}
		result, err := r.collection.UpdateOne(ctx, bson.M{"_id": objectID}, update)
		if err != nil {
			return fmt.Errorf("failed to soft delete user: %w", err)
		}
		if result.MatchedCount == 0 {
			return fmt.Errorf("user not found")
		}
		return nil
	}

	result, err := r.collection.DeleteOne(ctx, bson.M{"_id": objectID})
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf("user not found")
	}
	return nil
}

func (r *mongoRepository) List(ctx context.Context, page, pageSize int, sortBy string, ascending bool) ([]*models.User, int64, error) {
	skip := int64((page - 1) * pageSize)
	limit := int64(pageSize)

	sortOrder := -1
	if ascending {
		sortOrder = 1
	}
	if sortBy == "" {
		sortBy = "created_at"
	}

	findOpts := options.Find().
		SetSkip(skip).
		SetLimit(limit).
		SetSort(bson.D{{Key: sortBy, Value: sortOrder}})

	filter := bson.M{"status": bson.M{"$ne": models.StatusDeleted}}

	cursor, err := r.collection.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list users: %w", err)
	}
	defer cursor.Close(ctx)

	var users []*models.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, 0, fmt.Errorf("failed to decode users: %w", err)
	}

	total, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count users: %w", err)
	}

	return users, total, nil
}

func (r *mongoRepository) Search(ctx context.Context, filter *models.SearchFilter) ([]*models.User, int64, error) {
	query := bson.M{"status": bson.M{"$ne": models.StatusDeleted}}

	if filter.Query != "" {
		query["$text"] = bson.M{"$search": filter.Query}
	}

	if filter.City != nil {
		query["city"] = primitive.Regex{Pattern: *filter.City, Options: "i"}
	}

	if filter.Married != nil {
		query["married"] = *filter.Married
	}

	if filter.Status != nil {
		query["status"] = *filter.Status
	}

	skip := int64((filter.Page - 1) * filter.Size)
	limit := int64(filter.Size)

	findOpts := options.Find().
		SetSkip(skip).
		SetLimit(limit).
		SetSort(bson.D{{Key: "created_at", Value: -1}})

	cursor, err := r.collection.Find(ctx, query, findOpts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to search users: %w", err)
	}
	defer cursor.Close(ctx)

	var users []*models.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, 0, fmt.Errorf("failed to decode users: %w", err)
	}

	total, err := r.collection.CountDocuments(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count users: %w", err)
	}

	return users, total, nil
}

func (r *mongoRepository) GetStats(ctx context.Context, startDate, endDate time.Time) (*models.UserStats, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"status": models.StatusActive,
		}}},
		{{Key: "$facet", Value: bson.M{
			"total": mongo.Pipeline{
				{{Key: "$count", Value: "count"}},
			},
			"new_today": mongo.Pipeline{
				{{Key: "$match", Value: bson.M{
					"created_at": bson.M{"$gte": time.Now().Truncate(24 * time.Hour)},
				}}},
				{{Key: "$count", Value: "count"}},
			},
			"by_city": mongo.Pipeline{
				{{Key: "$group", Value: bson.M{
					"_id":   "$city",
					"count": bson.M{"$sum": 1},
				}}},
			},
		}}},
	}

	cursor, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode stats: %w", err)
	}

	stats := &models.UserStats{
		UsersByCity: make(map[string]int64),
	}

	if len(results) > 0 {
		result := results[0]

		if total, ok := result["total"].([]interface{}); ok && len(total) > 0 {
			if t, ok := total[0].(bson.M); ok {
				if count, ok := t["count"].(int32); ok {
					stats.TotalUsers = int64(count)
				}
			}
		}

		if newToday, ok := result["new_today"].([]interface{}); ok && len(newToday) > 0 {
			if t, ok := newToday[0].(bson.M); ok {
				if count, ok := t["count"].(int32); ok {
					stats.NewUsersToday = int64(count)
				}
			}
		}

		if byCity, ok := result["by_city"].([]interface{}); ok {
			for _, item := range byCity {
				if cityData, ok := item.(bson.M); ok {
					city := cityData["_id"].(string)
					count := cityData["count"].(int32)
					stats.UsersByCity[city] = int64(count)
				}
			}
		}
	}

	stats.ActiveUsers = stats.TotalUsers
	if !startDate.IsZero() && !endDate.IsZero() {
		days := endDate.Sub(startDate).Hours() / 24
		if days > 0 {
			stats.AvgUsersPerDay = float64(stats.TotalUsers) / days
		}
	}

	return stats, nil
}

func (r *mongoRepository) Close(ctx context.Context) error {
	return r.client.Disconnect(ctx)
}
