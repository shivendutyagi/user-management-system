package middleware

import (
	"context"

	"user-management-system/pkg/metrics"

	"google.golang.org/grpc"
)

func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		metrics.ActiveConnections.Inc()
		defer metrics.ActiveConnections.Dec()

		resp, err := handler(ctx, req)

		return resp, err
	}
}
