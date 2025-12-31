package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	GrpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"method", "status"},
	)

	GrpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	DbOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_operations_total",
			Help: "Total number of database operations",
		},
		[]string{"operation", "status"},
	)

	DbOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_operation_duration_seconds",
			Help:    "Duration of database operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	CacheOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_operations_total",
			Help: "Total number of cache operations",
		},
		[]string{"operation", "result"},
	)

	CacheHitRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cache_hit_ratio",
			Help: "Cache hit ratio",
		},
		[]string{"cache_type"},
	)

	KafkaMessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_produced_total",
			Help: "Total number of Kafka messages produced",
		},
		[]string{"topic"},
	)

	KafkaMessagesConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_consumed_total",
			Help: "Total number of Kafka messages consumed",
		},
		[]string{"topic", "consumer_group"},
	)

	KafkaProduceErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_produce_errors_total",
			Help: "Total number of Kafka produce errors",
		},
		[]string{"topic"},
	)

	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Number of active connections",
		},
	)

	UsersCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "users_created_total",
			Help: "Total number of users created",
		},
	)

	UsersDeletedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "users_deleted_total",
			Help: "Total number of users deleted",
		},
	)

	UsersUpdatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "users_updated_total",
			Help: "Total number of users updated",
		},
	)

	SystemErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "system_errors_total",
			Help: "Total number of system errors",
		},
		[]string{"component", "error_type"},
	)
)

func RecordGrpcRequest(method, status string, duration float64) {
	GrpcRequestsTotal.WithLabelValues(method, status).Inc()
	GrpcRequestDuration.WithLabelValues(method).Observe(duration)
}

func RecordDbOperation(operation, status string, duration float64) {
	DbOperationsTotal.WithLabelValues(operation, status).Inc()
	DbOperationDuration.WithLabelValues(operation).Observe(duration)
}

func RecordCacheOperation(operation, result string) {
	CacheOperationsTotal.WithLabelValues(operation, result).Inc()
}

func UpdateCacheHitRatio(cacheType string, ratio float64) {
	CacheHitRatio.WithLabelValues(cacheType).Set(ratio)
}

func RecordKafkaMessage(topic string, produced bool) {
	if produced {
		KafkaMessagesProduced.WithLabelValues(topic).Inc()
	}
}

func RecordKafkaError(topic string) {
	KafkaProduceErrors.WithLabelValues(topic).Inc()
}

func RecordSystemError(component, errorType string) {
	SystemErrors.WithLabelValues(component, errorType).Inc()
}
