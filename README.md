# ConsulCart — Distributed Microservices Commerce Platform

ConsulCart is a complete, enterprise-grade distributed e-commerce architecture built from scratch using **Java 21**, **Spring Boot 3.4.x**, **Spring Cloud 2024.0.x**, **HashiCorp Consul**, **Apache Kafka**, and **React + Vite**.

> **CRITICAL ARCHITECTURAL GUARANTEE: NO DATABASE WHATSOEVER**  
> ConsulCart does not install or use H2, MySQL, PostgreSQL, MongoDB, Redis, JDBC, JPA, Hibernate, SQL, or any other database.  
> All business entities (Orders, Products, Payments, Notifications) reside exclusively in thread-safe Java collections (`ConcurrentHashMap`). All application state resets cleanly when individual services restart. Kafka acts strictly as a distributed event broker for message passing, not as persistent storage.

---

## 1. System Architecture & Topology

```
                       +----------------------------------+
                       |   React 18 + Vite Web Client     |
                       |       (Port 5173 / Nginx)        |
                       +----------------+-----------------+
                                        | HTTP REST
                                        v
                       +----------------+-----------------+
                       |    Spring Cloud API Gateway      |
                       |          (Port 8080)             |
                       +-------+--------+---------+-------+
                               |        |         |       |
            +------------------+        |         |       +------------------+
            |                           |         |                          |
            v                           v         v                          v
 +--------------------+       +---------+-------+ |                 +--------+-----------+
 | Inventory Service  |       |  Order Service  | |                 |  Payment Service   |
 |    (Port 8082)     |       |   (Port 8081)   | |                 |    (Port 8083)     |
 | ConcurrentHashMap  |       |ConcurrentHashMap| |                 | ConcurrentHashMap  |
 +---------+----------+       +--------+--------+ |                 +---------+----------+
           |                           |          |                           |
           |                           | Event    |                           |
           |                           v          |                           |
           |                  +--------+--------+ |                           |
           |                  |  Apache Kafka   | |                           |
           |                  |   (Port 9092)   | |                           |
           |                  |  order-created  | |                           |
           |                  +--------+--------+ |                           |
           |                           |          |                           |
           |                           v          v                           |
           |                  +--------+----------+-------+                   |
           |                  |   Notification Service    |                   |
           |                  |        (Port 8084)        |                   |
           |                  |    ConcurrentHashMap      |                   |
           |                  +---------------------------+                   |
           |                                                                  |
           +-----------------------+   +--------------------------------------+
                                   |   |
                                   v   v
                       +-----------+---+------------------+
                       |    HashiCorp Consul Discovery    |
                       |       (Port 8500 Dashboard)     |
                       +----------------------------------+
```

### Complete End-to-End Reactive Flow
1. **Catalog Browsing & Reservation**: React frontend loads products from `inventory-service` (8082) via API Gateway (8080).
2. **Order Placement**: Frontend submits `POST /api/orders` to Gateway (8080). Gateway queries Consul service registry to dynamically locate `order-service` (8081).
3. **In-Memory Calculation & Storage**: `order-service` calculates line items and totals with `BigDecimal` precision and atomically records the order in `ConcurrentHashMap`.
4. **Kafka Event Publication**: `order-service` publishes a strongly-typed `OrderCreatedEvent` payload into Apache Kafka on topic `order-created`.
5. **Real Consumer & Deduplication**: `notification-service` (8084) consumes `OrderCreatedEvent` from Kafka, verifies idempotency against processed event keys, persists the notification in memory, and formats human-readable dispatch alerts.
6. **Live UI Delivery**: React dashboard displays the notification instantly, showing full Kafka JSON payload inspection.

---

## 2. Project Directory Structure

```
ConsulCart/
├── pom.xml                     # Maven Parent Multi-Module POM (Java 21, Spring Boot 3.4.3)
├── docker-compose.yml          # Consul, Kafka KRaft, 5 Microservices, and React Frontend
├── .env.example                # Template environment variables
├── .gitignore                  # Git ignore rules
├── README.md                   # Complete architectural guide and commands
├── api-gateway/                # Spring Cloud Gateway (Port 8080)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/consulcart/gateway/
│       │   ├── ApiGatewayApplication.java
│       │   ├── config/CorsConfig.java
│       │   └── controller/GatewayHealthController.java
│       └── main/resources/application.yml
├── order-service/              # Order Management & Kafka Producer (Port 8081)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/consulcart/order/
│       │   ├── OrderServiceApplication.java
│       │   ├── controller/OrderController.java
│       │   ├── dto/ (CreateOrderRequest, UpdateOrderRequest)
│       │   ├── event/OrderCreatedEvent.java
│       │   ├── exception/ (GlobalExceptionHandler, ErrorResponse, ResourceNotFoundException)
│       │   ├── kafka/ (OrderEventProducer, KafkaTopicConfig)
│       │   ├── model/ (Order, OrderStatus)
│       │   ├── repository/OrderRepository.java (ConcurrentHashMap)
│       │   └── service/OrderService.java
│       └── main/resources/application.yml
├── inventory-service/          # Product Catalog & Stock Reservation (Port 8082)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/consulcart/inventory/
│       │   ├── InventoryServiceApplication.java
│       │   ├── controller/InventoryController.java
│       │   ├── dto/ (CreateProductRequest, UpdateProductRequest, ReserveStockRequest, ...)
│       │   ├── exception/ (InsufficientStockException, ResourceNotFoundException, ...)
│       │   ├── model/Product.java
│       │   ├── repository/ProductRepository.java (ConcurrentHashMap & Atomic Locks)
│       │   └── service/InventoryService.java
│       └── main/resources/application.yml
├── payment-service/            # Simulated Payments & Idempotency (Port 8083)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/consulcart/payment/
│       │   ├── PaymentServiceApplication.java
│       │   ├── controller/PaymentController.java
│       │   ├── dto/ (CreatePaymentRequest, PaymentResponse)
│       │   ├── exception/ (GlobalExceptionHandler, ErrorResponse, ResourceNotFoundException)
│       │   ├── model/ (Payment, PaymentStatus)
│       │   ├── repository/PaymentRepository.java (ConcurrentHashMap & Idempotency Map)
│       │   └── service/PaymentService.java
│       └── main/resources/application.yml
├── notification-service/       # Kafka Consumer & Deduplication (Port 8084)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/consulcart/notification/
│       │   ├── NotificationServiceApplication.java
│       │   ├── controller/NotificationController.java
│       │   ├── event/OrderCreatedEvent.java
│       │   ├── exception/ (GlobalExceptionHandler, ErrorResponse, ResourceNotFoundException)
│       │   ├── kafka/OrderEventConsumer.java (KafkaListener on order-created)
│       │   ├── model/Notification.java
│       │   ├── repository/NotificationRepository.java (ConcurrentHashMap & Deduplication Set)
│       │   └── service/NotificationService.java
│       └── main/resources/application.yml
└── frontend/                   # Modern React + Vite Dashboard (Port 5173)
    ├── package.json
    ├── vite.config.js
    ├── index.html
    ├── nginx.conf
    ├── Dockerfile
    └── src/
        ├── App.jsx
        ├── main.jsx
        ├── index.css
        ├── components/ (Navbar, Sidebar, MetricsCard, Modal, Toast)
        ├── pages/ (DashboardPage, InventoryPage, OrdersPage, PaymentsPage, NotificationsPage)
        └── services/api.js
```

---

## 3. Microservice Specifications & Endpoints

All downstream microservices are accessible either directly on their host port or routed through the API Gateway at `http://localhost:8080/api/...`.

| Microservice | Direct Port | Gateway Route | Primary Function | Storage Mechanism |
|---|---|---|---|---|
| **API Gateway** | `8080` | `/api/**` | Consul Load-Balancing & CORS | In-memory routing table |
| **Order Service** | `8081` | `/api/orders/**` | Order Processing & Kafka Publisher | `ConcurrentHashMap<String, Order>` |
| **Inventory Service** | `8082` | `/api/inventory/**` | Product Catalog & Stock Lock | `ConcurrentHashMap<String, Product>` |
| **Payment Service** | `8083` | `/api/payments/**` | Idempotent Payment Simulator | `ConcurrentHashMap<String, Payment>` |
| **Notification Service** | `8084` | `/api/notifications/**`| Kafka Consumer & Deduplicator | `ConcurrentHashMap<String, Notification>` |
| **Frontend** | `5173` | `/` | Responsive Management Console | Browser React State |
| **Consul UI** | `8500` | `/ui` | Discovery Dashboard & Health Checks | Consul In-Memory Raft |
| **Kafka Broker** | `9092` | `order-created` | Event Broker (KRaft mode) | Kafka Log Partitions |

### Complete REST API Reference

#### 1. Order Service Endpoints
- `POST /api/orders`: Create new order. Validates input, calculates total with `BigDecimal`, stores in `ConcurrentHashMap`, and publishes `OrderCreatedEvent` to Kafka.
- `GET /api/orders`: Retrieve all in-memory orders (sorted by most recent).
- `GET /api/orders/{id}`: Retrieve a single order by ID.
- `PUT /api/orders/{id}`: Update order status (`PENDING`, `CONFIRMED`, `PAID`, `CANCELLED`).
- `DELETE /api/orders/{id}`: Remove an order from memory.
- `GET /api/orders/health`: Returns health status and data retention explanation.

#### 2. Inventory Service Endpoints
- `GET /api/inventory`: List all seeded and created products with available vs reserved counts.
- `GET /api/inventory/{productId}`: Lookup product details by ID.
- `POST /api/inventory`: Create new product in catalog.
- `PUT /api/inventory/{productId}`: Update product price, stock, or metadata.
- `POST /api/inventory/{productId}/reserve`: Atomically reserve quantity for an order; fails if available stock < quantity.
- `POST /api/inventory/{productId}/release`: Atomically releases reserved quantity back to available stock.
- `GET /api/inventory/health`: Health status endpoint.

#### 3. Payment Service Endpoints
- `POST /api/payments`: Process simulated payment using `idempotencyKey`. If key is repeated, returns original payment record with `duplicatePrevented: true`.
- `GET /api/payments`: List all processed payments.
- `GET /api/payments/{id}`: Lookup payment record by ID.
- `GET /api/payments/order/{orderId}`: Lookup payments associated with an order ID.
- `GET /api/payments/health`: Health status endpoint.

#### 4. Notification Service Endpoints
- `GET /api/notifications`: Retrieve all notifications consumed from Kafka.
- `GET /api/notifications/{id}`: Lookup individual notification record.
- `POST /api/notifications/test`: Trigger simulated event dispatch to verify consumer flow.
- `GET /api/notifications/health`: Health status endpoint.

#### 5. API Gateway Endpoints
- `GET /api/gateway/health`: Gateway connectivity and configured route table summary.
- `/actuator/health`: Actuator health check exposed for Consul discovery.

---

## 4. Setup & Launch Instructions (Windows PowerShell)

### Prerequisites
- **JDK 21** installed and configured in `JAVA_HOME`
- **Apache Maven 3.9+**
- **Node.js 18+** and `npm`
- **Docker Desktop** (optional for Docker Compose deployment)

---

### Method A: Local Native Execution (Without Docker)

You can run ConsulCart directly on Windows PowerShell without Docker.

#### 1. Compile and Test All Modules
From the project root (`c:\Users\Hardware\OneDrive\Desktop\consul`):
```powershell
mvn clean test
```

#### 2. Start Microservices in Separate PowerShell Windows

**Window 1: Inventory Service (8082)**
```powershell
cd inventory-service
mvn spring-boot:run
```

**Window 2: Order Service (8081)**
```powershell
cd order-service
mvn spring-boot:run
```

**Window 3: Payment Service (8083)**
```powershell
cd payment-service
mvn spring-boot:run
```

**Window 4: Notification Service (8084)**
```powershell
cd notification-service
mvn spring-boot:run
```

**Window 5: API Gateway (8080)**
```powershell
cd api-gateway
mvn spring-boot:run
```

**Window 6: React Frontend (5173)**
```powershell
cd frontend
npm install
npm run dev
```

Open your browser at **`http://localhost:5173`**.

---

### Method B: Complete Stack Launch with Docker Compose

When Docker Desktop is running, launch the entire system with one command:

```powershell
# 1. Verify Docker configuration
docker compose config

# 2. Build and launch all containers
docker compose up -d --build

# 3. View logs of the Kafka event flow
docker compose logs -f order-service notification-service

# 4. Stop stack
docker compose down
```

Access Points:
- **React Frontend**: `http://localhost:5173`
- **API Gateway**: `http://localhost:8080`
- **Consul Web Dashboard**: `http://localhost:8500`

---

## 5. Automated Verification & Testing

ConsulCart includes comprehensive JUnit 5 test suites covering:
1. **Order Creation & Validation**: Validates `@NotBlank`, positive price, and `BigDecimal` multiplication accuracy.
2. **Atomic Stock Protection & Concurrency**: Multi-threaded concurrency test (`InventoryServiceTest`) running 20 simultaneous threads against 10 stock items to mathematically prove that overselling is prevented.
3. **Idempotency Protection**: Ensures duplicate payment requests with identical keys return the existing record and prevent duplicate financial charging.
4. **Kafka Event Deduplication**: Proves that duplicate `OrderCreatedEvent` messages arriving at `notification-service` are detected and suppressed.
5. **API Error Handling**: Verifies standardized HTTP 400 Bad Request responses with detailed field error collections.

To run all automated test suites:
```powershell
mvn test
```

---

## 6. Troubleshooting & Gotchas

1. **"Data vanished after restart"**:
   This is by design. ConsulCart adheres to the strict **NO DATABASE WHATSOEVER** requirement. All business data is kept in Java `ConcurrentHashMap` collections.
2. **"Kafka connection failed during local unit tests"**:
   Services are designed with graceful resilience. In unit test mode or standalone local mode without Kafka running, order creation logs a warning and completes safely without crashing.
3. **"Consul health check failing in Docker"**:
   Ensure `docker-compose.yml` healthcheck is enabled for Consul and services register with `prefer-ip-address: true`.
"# Project-on-MSA" 
