package middleware

import (
	"context"
	"time"

	"user-management-system/pkg/logger"
	"user-management-system/pkg/metrics"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func LoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		logger.Info("Incoming request",
			zap.String("method", info.FullMethod),
			zap.Any("request", req),
		)

		resp, err := handler(ctx, req)

		duration := time.Since(start)

		statusCode := codes.OK
		if err != nil {
			statusCode = status.Code(err)
		}

		metrics.RecordGrpcRequest(info.FullMethod, statusCode.String(), duration.Seconds())

		if err != nil {
			logger.Error("Request failed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.String("status", statusCode.String()),
				zap.Error(err),
			)
		} else {
			logger.Info("Request completed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.String("status", statusCode.String()),
			)
		}

		return resp, err
	}
}

func StreamLoggingInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		logger.Info("Stream started",
			zap.String("method", info.FullMethod),
			zap.Bool("client_stream", info.IsClientStream),
			zap.Bool("server_stream", info.IsServerStream),
		)

		err := handler(srv, ss)

		duration := time.Since(start)

		if err != nil {
			logger.Error("Stream failed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		} else {
			logger.Info("Stream completed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
			)
		}

		return err
	}
}
