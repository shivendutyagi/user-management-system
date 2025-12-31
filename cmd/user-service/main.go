package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	pb "user-management-system/api/proto"
	"user-management-system/internal/config"
	"user-management-system/internal/kafka"
	"user-management-system/internal/middleware"
	"user-management-system/internal/models"
	"user-management-system/internal/repository"
	"user-management-system/internal/service"
	"user-management-system/pkg/logger"
	"user-management-system/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type server struct {
	pb.UnimplementedUserServiceServer
	userService service.UserService
}

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	// Initialize logger
	if err := logger.InitLogger(cfg.Logger.Level); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting user service", zap.String("version", "1.0.0"))

	// Initialize MongoDB repository
	mongoRepo, err := repository.NewMongoRepository(&cfg.MongoDB)
	if err != nil {
		logger.Fatal("Failed to initialize MongoDB", zap.Error(err))
	}
	logger.Info("MongoDB connected successfully")

	// Initialize Redis cache
	redisCache, err := repository.NewRedisCache(&cfg.Redis)
	if err != nil {
		logger.Fatal("Failed to initialize Redis", zap.Error(err))
	}
	logger.Info("Redis connected successfully")

	// Initialize Kafka producer
	kafkaProducer, err := kafka.NewProducer(&cfg.Kafka)
	if err != nil {
		logger.Fatal("Failed to initialize Kafka producer", zap.Error(err))
	}
	defer kafkaProducer.Close()
	logger.Info("Kafka producer initialized successfully")

	// Initialize user service
	userSvc := service.NewUserService(mongoRepo, redisCache, kafkaProducer)

	// Start metrics server
	go startMetricsServer(cfg.Metrics.Port, cfg.Metrics.Path)

	// Create gRPC server with middleware
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			middleware.RecoveryInterceptor(),
			middleware.LoggingInterceptor(),
			middleware.MetricsInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			middleware.StreamRecoveryInterceptor(),
			middleware.StreamLoggingInterceptor(),
		),
		grpc.MaxRecvMsgSize(10*1024*1024), // 10MB
		grpc.MaxSendMsgSize(10*1024*1024), // 10MB
	)

	// Register services
	pb.RegisterUserServiceServer(grpcServer, &server{userService: userSvc})

	// Register health check
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Register reflection for grpcurl
	reflection.Register(grpcServer)

	// Start server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.Port))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	logger.Info("Server starting", zap.Int("port", cfg.Server.Port))

	// Graceful shutdown
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve", zap.Error(err))
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")
	grpcServer.GracefulStop()
	logger.Info("Server stopped")
}

func startMetricsServer(port int, path string) {
	http.Handle(path, promhttp.Handler())
	addr := fmt.Sprintf(":%d", port)
	logger.Info("Metrics server starting", zap.String("address", addr), zap.String("path", path))
	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("Metrics server error", zap.Error(err))
	}
}

// gRPC Service Implementation

func (s *server) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.UserResponse, error) {
	user := &models.User{
		Name:     req.Name,
		Email:    req.Email,
		City:     req.City,
		Phone:    req.Phone,
		Married:  req.Married,
		Metadata: req.Metadata,
	}

	if err := s.userService.CreateUser(ctx, user); err != nil {
		logger.Error("Failed to create user", zap.Error(err))
		return &pb.UserResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	metrics.UsersCreatedTotal.Inc()

	return &pb.UserResponse{
		Success: true,
		Message: "User created successfully",
		User:    convertUserToProto(user),
	}, nil
}

func (s *server) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.UserResponse, error) {
	user, err := s.userService.GetUserByID(ctx, req.Id)
	if err != nil {
		return &pb.UserResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.UserResponse{
		Success: true,
		User:    convertUserToProto(user),
	}, nil
}

func (s *server) UpdateUser(ctx context.Context, req *pb.UpdateUserRequest) (*pb.UserResponse, error) {
	// First get existing user
	user, err := s.userService.GetUserByID(ctx, req.Id)
	if err != nil {
		return &pb.UserResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Update fields if provided
	if req.Name != nil {
		user.Name = *req.Name
	}
	if req.Email != nil {
		user.Email = *req.Email
	}
	if req.City != nil {
		user.City = *req.City
	}
	if req.Phone != nil {
		user.Phone = *req.Phone
	}
	if req.Married != nil {
		user.Married = *req.Married
	}
	if req.Metadata != nil {
		user.Metadata = req.Metadata
	}

	if err := s.userService.UpdateUser(ctx, user); err != nil {
		return &pb.UserResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.UserResponse{
		Success: true,
		Message: "User updated successfully",
		User:    convertUserToProto(user),
	}, nil
}

func (s *server) DeleteUser(ctx context.Context, req *pb.DeleteUserRequest) (*pb.APIResponse, error) {
	if err := s.userService.DeleteUser(ctx, req.Id, req.SoftDelete); err != nil {
		return &pb.APIResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.APIResponse{
		Success: true,
		Message: "User deleted successfully",
	}, nil
}

func (s *server) ListUsers(ctx context.Context, req *pb.ListUsersRequest) (*pb.ListUsersResponse, error) {
	page := int(req.Page)
	if page < 1 {
		page = 1
	}
	pageSize := int(req.PageSize)
	if pageSize < 1 {
		pageSize = 10
	}

	users, total, err := s.userService.ListUsers(ctx, page, pageSize, req.SortBy, req.Ascending)
	if err != nil {
		return &pb.ListUsersResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	pbUsers := make([]*pb.User, len(users))
	for i, user := range users {
		pbUsers[i] = convertUserToProto(user)
	}

	return &pb.ListUsersResponse{
		Success:    true,
		Users:      pbUsers,
		TotalCount: int32(total),
		Page:       req.Page,
		PageSize:   req.PageSize,
	}, nil
}

func (s *server) SearchUsers(ctx context.Context, req *pb.SearchRequest) (*pb.ListUsersResponse, error) {
	filter := &models.SearchFilter{
		Query: req.Query,
		Page:  int(req.Page),
		Size:  int(req.PageSize),
	}

	if req.Filters != nil {
		filter.City = req.Filters.City
		filter.Married = req.Filters.Married
		if req.Filters.Status != nil {
			status := models.UserStatus(req.Filters.Status.String())
			filter.Status = &status
		}
	}

	users, total, err := s.userService.SearchUsers(ctx, filter)
	if err != nil {
		return &pb.ListUsersResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	pbUsers := make([]*pb.User, len(users))
	for i, user := range users {
		pbUsers[i] = convertUserToProto(user)
	}

	return &pb.ListUsersResponse{
		Success:    true,
		Users:      pbUsers,
		TotalCount: int32(total),
		Page:       req.Page,
		PageSize:   req.PageSize,
	}, nil
}

func (s *server) GetUsersByIds(ctx context.Context, req *pb.GetUsersByIdsRequest) (*pb.ListUsersResponse, error) {
	users, err := s.userService.GetUsersByIDs(ctx, req.Ids)
	if err != nil {
		return &pb.ListUsersResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	pbUsers := make([]*pb.User, len(users))
	for i, user := range users {
		pbUsers[i] = convertUserToProto(user)
	}

	return &pb.ListUsersResponse{
		Success: true,
		Users:   pbUsers,
	}, nil
}

func (s *server) BatchCreateUsers(stream pb.UserService_BatchCreateUsersServer) error {
	var successful, failed int32
	var errors []string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.BatchResponse{
				Success:        true,
				TotalProcessed: successful + failed,
				Successful:     successful,
				Failed:         failed,
				Errors:         errors,
			})
		}
		if err != nil {
			return err
		}

		user := &models.User{
			Name:     req.Name,
			Email:    req.Email,
			City:     req.City,
			Phone:    req.Phone,
			Married:  req.Married,
			Metadata: req.Metadata,
		}

		if err := s.userService.CreateUser(context.Background(), user); err != nil {
			failed++
			errors = append(errors, fmt.Sprintf("Failed to create user %s: %v", req.Email, err))
		} else {
			successful++
		}
	}
}

// Helper function to convert domain model to protobuf
func convertUserToProto(user *models.User) *pb.User {
	status := pb.UserStatus_ACTIVE
	switch user.Status {
	case models.StatusInactive:
		status = pb.UserStatus_INACTIVE
	case models.StatusSuspended:
		status = pb.UserStatus_SUSPENDED
	case models.StatusDeleted:
		status = pb.UserStatus_DELETED
	}

	return &pb.User{
		Id:        user.ID.Hex(),
		Name:      user.Name,
		Email:     user.Email,
		City:      user.City,
		Phone:     user.Phone,
		Married:   user.Married,
		CreatedAt: timestamppb.New(user.CreatedAt),
		UpdatedAt: timestamppb.New(user.UpdatedAt),
		Metadata:  user.Metadata,
		Status:    status,
	}
}

