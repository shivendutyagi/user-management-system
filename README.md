# User Management Microservices System

A production-ready, high-throughput user management system built with Go, gRPC, Kafka, MongoDB, and Redis. Designed to demonstrate enterprise-grade microservices architecture with event-driven patterns, caching strategies, and comprehensive observability.

## 🎯 Project Overview

This project showcases a complete microservices architecture with:
- **High-performance gRPC APIs** for inter-service communication
- **Event-driven architecture** using Apache Kafka
- **Redis caching layer** for sub-5ms response times
- **MongoDB** with optimized indexing and connection pooling
- **Real-time analytics** processing user events
- **Production-grade monitoring** with Prometheus and Grafana

## 📋 Table of Contents

- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [API Documentation](#-api-documentation)
- [Monitoring](#-monitoring)
- [Project Structure](#-project-structure)
- [Key Learnings & Design Decisions](#-key-learnings--design-decisions)

---

## 🏗️ Architecture

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                              │
│                      (gRPC Clients)                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     User Service (gRPC)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  API Layer   │  │ Service Layer│  │  Repository  │         │
│  │  (gRPC)      │─▶│ (Business    │─▶│  (Data)      │         │
│  │              │  │  Logic)      │  │              │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└───────┬─────────────────┬──────────────────┬───────────────────┘
        │                 │                  │
        │                 │                  │
        ▼                 ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   MongoDB    │  │    Redis     │  │    Kafka     │
│  (Primary    │  │  (Cache)     │  │  (Events)    │
│   Database)  │  │  <5ms reads  │  │              │
└──────────────┘  └──────────────┘  └──────┬───────┘
                                            │
                                            ▼
                                   ┌─────────────────┐
                                   │   Analytics     │
                                   │    Service      │
                                   │ (Event Consumer)│
                                   └─────────────────┘
                                            │
                                            ▼
                                   ┌─────────────────┐
                                   │   Prometheus    │
                                   │   & Grafana     │
                                   │  (Monitoring)   │
                                   └─────────────────┘
```

### Component Overview

#### 1. User Service (gRPC Server)
- **Port**: 50051
- **Responsibilities**:
  - CRUD operations for user management
  - Full-text search with MongoDB text indexes
  - Cache-aside pattern with Redis
  - Event publishing to Kafka
  - Health checks and metrics exposition

#### 2. Analytics Service (Event Consumer)
- **Responsibilities**:
  - Consumes user events from Kafka
  - Real-time analytics processing
  - Metrics aggregation in Redis
  - Event tracking (created, updated, deleted, viewed)

#### 3. Data Layer
- **MongoDB**: Primary data store with indexes
- **Redis**: L1 cache for frequently accessed data
- **Kafka**: Event streaming and async communication

#### 4. Observability Stack
- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **Structured Logging**: Zap for high-performance logging

---

## ✨ Features

### Core Functionality
- ✅ **User Management**
  - Create, Read, Update, Delete operations
  - Soft delete support
  - Batch operations
  - Full-text search

- ✅ **Performance Optimization**
  - Redis caching with TTL management
  - MongoDB connection pooling (100 max connections)
  - Database indexing for fast queries
  - Efficient serialization with Protocol Buffers

- ✅ **Event-Driven Architecture**
  - Kafka event publishing for all operations
  - Asynchronous analytics processing
  - Event sourcing patterns
  - At-least-once delivery guarantee

- ✅ **Production-Ready Features**
  - Health checks (gRPC health protocol)
  - Graceful shutdown
  - Panic recovery middleware
  - Request logging and tracing
  - Prometheus metrics

### Advanced Features
- 📊 Real-time analytics tracking
- 🔍 Full-text search with MongoDB
- 💾 Multi-level caching strategy
- 🔄 Automatic cache invalidation
- 📈 Performance metrics and monitoring
- 🛡️ Error handling and retry logic

---

## 🛠️ Tech Stack

### Core Technologies
- **Language**: Go 1.23+
- **API Protocol**: gRPC + Protocol Buffers
- **Database**: MongoDB 7.0
- **Cache**: Redis 7.2
- **Message Queue**: Apache Kafka 3.5
- **Monitoring**: Prometheus + Grafana

---

## 📚 API Documentation

### User Service Endpoints

#### CreateUser
Creates a new user in the system.

```bash
grpcurl -plaintext -d '{
  "name": "John Doe",
  "email": "john@example.com",
  "city": "New York",
  "phone": "555-0100",
  "married": true,
  "metadata": {"department": "Engineering"}
}' localhost:50051 user.UserService/CreateUser
```

#### GetUser
Retrieves a user by ID with Redis caching.

```bash
grpcurl -plaintext -d '{
  "id": "USER_ID_HERE"
}' localhost:50051 user.UserService/GetUser
```

#### ListUsers
Lists users with pagination and sorting.

```bash
grpcurl -plaintext -d '{
  "page": 1,
  "page_size": 10,
  "sort_by": "created_at",
  "ascending": false
}' localhost:50051 user.UserService/ListUsers
```

#### SearchUsers
Full-text search across name, email, and city.

```bash
grpcurl -plaintext -d '{
  "query": "John",
  "page": 1,
  "page_size": 10
}' localhost:50051 user.UserService/SearchUsers
```

#### UpdateUser
Updates user information with cache invalidation.

```bash
grpcurl -plaintext -d '{
  "id": "USER_ID_HERE",
  "name": "John Smith",
  "city": "Los Angeles"
}' localhost:50051 user.UserService/UpdateUser
```

#### DeleteUser
Soft or hard delete with event publishing.

```bash
grpcurl -plaintext -d '{
  "id": "USER_ID_HERE",
  "soft_delete": true
}' localhost:50051 user.UserService/DeleteUser
```

#### GetUsersByIds
Batch retrieval of multiple users.

```bash
grpcurl -plaintext -d '{
  "ids": ["ID1", "ID2", "ID3"]
}' localhost:50051 user.UserService/GetUsersByIds
```

---

## 📊 Monitoring

### Prometheus Metrics

**Key Metrics**:
- `grpc_requests_total` - Total gRPC requests
- `grpc_request_duration_seconds` - Request latency
- `cache_operations_total` - Cache hit/miss stats
- `db_operations_total` - Database operations
- `kafka_messages_produced_total` - Event publishing
- `users_created_total` - User creation count

### Grafana Dashboards

**Pre-configured Dashboards**:
1. **Service Overview** - Request rates, latency, error rates
2. **Cache Performance** - Hit ratios, operation counts
3. **Database Metrics** - Query performance, connection pool
4. **Kafka Metrics** - Message throughput, consumer lag
5. **System Health** - CPU, memory, active connections

---

## 📁 Project Structure

```
user-management-system/
├── api/
│   └── proto/
│       └── user.proto              # Protocol Buffer definitions
├── cmd/
│   ├── user-service/
│   │   └── main.go                 # User service entry point
│   └── analytics-service/
│       └── main.go                 # Analytics service entry point
├── internal/
│   ├── config/
│   │   └── config.go               # Configuration management
│   ├── models/
│   │   └── user.go                 # Domain models
│   ├── repository/
│   │   ├── mongo_repository.go     # MongoDB operations
│   │   └── redis_cache.go          # Redis caching
│   ├── service/
│   │   └── user_service.go         # Business logic
│   ├── kafka/
│   │   ├── producer.go             # Event producer
│   │   └── consumer.go             # Event consumer
│   └── middleware/
│       ├── logging.go              # Request logging
│       ├── recovery.go             # Panic recovery
│       └── metrics.go              # Metrics collection
├── pkg/
│   ├── logger/
│   │   └── logger.go               # Structured logging
│   ├── metrics/
│   │   └── metrics.go              # Prometheus metrics
│   └── pb/                         # Generated protobuf code
├── deployments/
│   ├── docker-compose.yml          # Docker Compose config
│   ├── kubernetes/                 # K8s manifests
│   ├── prometheus.yml              # Prometheus config
│   └── grafana-dashboards/         # Grafana dashboards
├── scripts/
│   └── init-mongo.js               # MongoDB initialization
├── Dockerfile.user-service
├── Dockerfile.analytics-service
├── Makefile                        # Build commands
├── go.mod
└── README.md
```

---

## 🎓 Key Learnings & Design Decisions

### Why gRPC?
- **Performance**: Binary protocol is faster than JSON
- **Type Safety**: Protocol Buffers ensure type safety
- **Streaming**: Supports bi-directional streaming
- **Language Agnostic**: Easy to add clients in other languages

### Why Kafka?
- **Decoupling**: Services don't need to know about each other
- **Scalability**: Easy to add more consumers
- **Reliability**: At-least-once delivery guarantee
- **Replay**: Can replay events for debugging

### Why Redis?
- **Speed**: Sub-millisecond latency
- **Simplicity**: Simple key-value store
- **TTL Support**: Automatic expiration
- **High Availability**: Supports clustering

### Why MongoDB?
- **Flexibility**: Schema-less design
- **Indexing**: Powerful indexing capabilities
- **Aggregation**: Built-in analytics features
- **Scaling**: Easy horizontal scaling with sharding

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Your Name**
- GitHub: [@shivendutyagi](https://github.com/shivendutyagi)
- LinkedIn: [shivendu-tyagi](https://linkedin.com/in/shivendu-tyagi)
- Email: shivendu.2420@gmail.com
