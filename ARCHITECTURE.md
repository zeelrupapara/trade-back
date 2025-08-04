# Trade-Back Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Storage Architecture](#storage-architecture)
6. [API Documentation](#api-documentation)
7. [WebSocket Protocol](#websocket-protocol)
8. [Configuration](#configuration)
9. [Deployment](#deployment)
10. [Performance Optimizations](#performance-optimizations)

## Overview

Trade-Back is a high-performance cryptocurrency trading backend built with Go, designed for ultra-low latency price processing and seamless gap-free data delivery. The system handles real-time market data from Binance, processes it through a sophisticated pipeline, and delivers it to clients via an optimized binary WebSocket protocol.

### Key Features
- **Ultra-low latency processing** (<3ms end-to-end)
- **Binary protocol** (72% smaller messages, 16x faster parsing)
- **Gap-free data delivery** with intelligent buffering
- **NATS-based message distribution** for scalability
- **TradingView integration** with custom datafeed
- **Enigma levels technical indicator** calculation
- **Dynamic symbol subscription** based on user watchlists

## System Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Binance WSS    │────▶│   Hub/Router    │────▶│  Price Store    │
│   (3 streams)   │     │                 │     │   (InfluxDB)    │
│                 │     │                 │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  NATS Message   │◀────│ Price Processor │────▶│  Redis Cache    │
│     Broker      │     │   (4 workers)   │     │                 │
│                 │     │                 │     │                 │
└────────┬────────┘     └─────────────────┘     └─────────────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│ Binary WebSocket│◀────│  WS Manager     │────▶│    Frontend     │
│    Protocol     │     │   (batching)    │     │   (React/TS)    │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Core Components

### 1. Exchange Layer (`/internal/exchange/`)

#### Hub (`hub.go`)
Central orchestrator that manages all price data flow:
- Initializes connections to all data sources
- Routes price updates to various handlers
- Manages health checks and monitoring
- Implements gap detection and recovery

#### Connection Pool (`pool.go`)
Manages multiple Binance WebSocket connections:
- Creates separate connections for trade, book ticker, and aggregated trades
- Implements connection pooling for reliability
- Handles automatic reconnection with exponential backoff
- Distributes load across multiple streams

#### Price Processor (`price_processor.go`)
Multi-threaded price processing engine:
- Processes up to 10,000 messages concurrently
- Implements batching (100 messages or 50ms timeout)
- Deduplicates prices per symbol
- Distributes work across 4 worker threads

#### Dynamic Symbol Manager (`dynamic_symbols.go`)
Automatically manages symbol subscriptions:
- Polls Redis every 5 seconds for market watch changes
- Dynamically subscribes/unsubscribes to symbols
- Reduces unnecessary data processing
- Supports per-session symbol management

### 2. WebSocket Layer (`/internal/websocket/`)

#### Binary Manager (`binary_manager.go`)
Ultra-low latency WebSocket implementation:
- Custom binary protocol for 72% smaller messages
- Message batching (50 items per batch, 50ms window)
- CRC32 checksums for data integrity
- Symbol-specific message routing
- Automatic market watch subscription

Binary Protocol Format:
```
[4 bytes: CRC32][2 bytes: count][repeated: symbol_id(2) + data(16)]
```

### 3. Storage Layer (`/internal/database/`)

#### InfluxDB Client (`influx.go`)
Time-series data storage:
- Stores price ticks, OHLCV bars, and technical indicators
- Implements efficient batch writing
- Supports multi-resolution data (1m, 5m, 15m, 1h, 4h, 1d)
- Handles ATH/ATL calculations

#### MySQL Client (`mysql.go`)
Relational data storage:
- Symbol metadata and configuration
- User sessions and authentication
- Market watch preferences
- System configuration

### 4. Cache Layer (`/internal/cache/`)

#### Redis Client (`redis.go`)
High-performance caching:
- Current price caching with TTL
- Market watch symbol lists
- Session token management
- API response caching
- Implements Redis Streams for real-time updates

### 5. Messaging Layer (`/internal/messaging/`)

#### NATS Client (`nats.go`)
Distributed messaging:
- Publishes price updates to multiple consumers
- Supports pub/sub for scalability
- Enables microservice communication
- Implements request/reply patterns

### 6. API Layer (`/internal/api/`)

#### REST Server (`server.go`)
HTTP API implementation:
- RESTful endpoints for data access
- WebSocket upgrade handling
- Authentication middleware
- Rate limiting and CORS support

#### TradingView Integration (`tradingview.go`)
Custom datafeed for charts:
- Implements UDF protocol
- Provides symbol search
- Serves historical data
- Real-time subscription support

### 7. Technical Indicators (`/internal/indicator/`)

#### Enigma Calculator (`enigma/calculator.go`)
Custom technical indicator:
- Calculates market position (0-100%)
- Tracks ATH/ATL for each symbol
- Generates Fibonacci retracement levels
- Publishes via WebSocket and NATS

## Data Flow

### 1. Market Data Ingestion
```
Binance WebSocket → Connection Pool → Price Updates → Hub
```

### 2. Processing Pipeline
```
Hub → Price Processor → Parallel Processing:
  ├── InfluxDB (historical storage)
  ├── Redis (current price cache)
  ├── NATS (message distribution)
  └── WebSocket Manager (client delivery)
```

### 3. Client Delivery
```
Price Updates → Binary Encoder → Batch Buffer → WebSocket → Frontend
```

### 4. Historical Data Flow
```
HTTP Request → API Server → Cache Check → InfluxDB Query → Response
```

## Storage Architecture

### MySQL Schema

```sql
-- Symbol metadata
CREATE TABLE symbolsmap (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(50) UNIQUE NOT NULL,
    base_currency VARCHAR(50),
    quote_currency VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    price_precision INT,
    quantity_precision INT,
    min_quantity DECIMAL(20,8),
    max_quantity DECIMAL(20,8),
    step_size DECIMAL(20,8),
    min_notional DECIMAL(20,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- User market watch
CREATE TABLE market_watch (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    display_order INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_session_symbol (session_id, symbol)
);

-- User sessions
CREATE TABLE user_sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_token VARCHAR(255) UNIQUE NOT NULL,
    user_id VARCHAR(255),
    ip_address VARCHAR(45),
    user_agent TEXT,
    is_active BOOLEAN DEFAULT true,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP
);
```

### InfluxDB Schema

```flux
// Price tick data
measurement: trades
tags: exchange, symbol
fields: price, bid, ask, volume, sequence
time: timestamp

// OHLCV data
measurement: ohlcv_1m, ohlcv_5m, ohlcv_15m, ohlcv_1h, ohlcv_4h, ohlcv_1d
tags: exchange, symbol
fields: open, high, low, close, volume, trade_count
time: timestamp

// Technical indicators
measurement: enigma
tags: symbol
fields: level, ath, atl, fib_0, fib_23_6, fib_38_2, fib_50, fib_61_8, fib_78_6, fib_100
time: timestamp
```

### Redis Schema

```
# Current prices
price:{symbol} → JSON{price, bid, ask, volume, timestamp}

# Market watch
marketwatch:{session_id} → SET[symbols]

# Session tokens
session:{token} → JSON{user_id, expires_at}

# API cache
cache:bars:{symbol}:{resolution} → JSON[bars]

# Enigma levels
enigma:{symbol} → JSON{level, ath, atl, fib_levels}
```

## API Documentation

### Authentication Endpoints

```http
POST /api/v1/auth/login
Content-Type: application/json
{
  "username": "user@example.com",
  "password": "password"
}

POST /api/v1/auth/logout
Cookie: session_token=xxx

GET /api/v1/auth/session
Cookie: session_token=xxx
```

### Market Data Endpoints

```http
GET /api/v1/symbols
Response: [{symbol, name, base_currency, quote_currency, is_active}]

GET /api/v1/symbols/{symbol}/price
Response: {symbol, price, bid, ask, volume, timestamp}

GET /api/v1/symbols/{symbol}/bars?resolution=1d&from=1234567890&to=1234567890
Response: {bars: [{time, open, high, low, close, volume}]}

GET /api/v1/symbols/{symbol}/enigma
Response: {symbol, level, ath, atl, current_price, fib_levels}
```

### Market Watch Endpoints

```http
GET /api/v1/marketwatch
Cookie: session_token=xxx
Response: {symbols: ["BTCUSDT", "ETHUSDT"]}

POST /api/v1/marketwatch/{symbol}
Cookie: session_token=xxx

DELETE /api/v1/marketwatch/{symbol}
Cookie: session_token=xxx
```

### WebSocket Endpoint

```http
GET /api/v1/ws
Upgrade: websocket
Cookie: session_token=xxx
```

## WebSocket Protocol

### Connection Flow

1. **HTTP Upgrade**: Client requests WebSocket upgrade with session token
2. **Authentication**: Server validates session and upgrades connection
3. **Auto-subscription**: Server automatically subscribes to user's market watch
4. **Binary Stream**: Server sends binary-encoded price updates

### Message Types

#### Text Messages (Control)
```json
// Subscribe
{"type": "subscribe", "symbols": ["BTCUSDT", "ETHUSDT"]}

// Unsubscribe
{"type": "unsubscribe", "symbols": ["BTCUSDT"]}

// Ping/Pong
{"type": "ping"}
{"type": "pong"}

// Auto-subscribe notification
{"type": "auto_subscribe", "data": {"symbol": "BTCUSDT"}}
```

#### Binary Messages (Market Data)
```
Structure: [CRC32(4)][Count(2)][Data(N)]
Data: [SymbolID(2)][Price(8)][Volume(8)]...

Benefits:
- 72% smaller than JSON
- 16x faster parsing
- Native number representation
- Built-in integrity checks
```

### Client Implementation

```javascript
// Binary decoding example
function decodeBinary(buffer) {
  const view = new DataView(buffer);
  const crc32 = view.getUint32(0, true);
  const count = view.getUint16(4, true);
  
  const updates = [];
  let offset = 6;
  
  for (let i = 0; i < count; i++) {
    const symbolId = view.getUint16(offset, true);
    const price = view.getFloat64(offset + 2, true);
    const volume = view.getFloat64(offset + 10, true);
    
    updates.push({ symbolId, price, volume });
    offset += 18;
  }
  
  return updates;
}
```

## Configuration

### Environment Variables

```bash
# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8080
SERVER_READ_TIMEOUT=15s
SERVER_WRITE_TIMEOUT=15s

# Database Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=trading
MYSQL_USER=trading
MYSQL_PASSWORD=secret

# InfluxDB Configuration
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=my-super-secret-auth-token
INFLUX_ORG=trading-org
INFLUX_BUCKET=trading

# Redis Configuration
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0

# NATS Configuration
NATS_URL=nats://localhost:4222

# Exchange Configuration
EXCHANGE_WS_ENDPOINT=wss://stream.binance.com:9443
EXCHANGE_REST_ENDPOINT=https://api.binance.com
EXCHANGE_MAX_CONNECTIONS=3

# Features
FEATURE_ENIGMA_ENABLED=true
FEATURE_AGGREGATION_ENABLED=true
FEATURE_BINARY_WEBSOCKET=true
```

### Configuration File (`config.yaml`)

```yaml
server:
  host: 0.0.0.0
  port: 8080
  cors:
    allowed_origins: ["http://localhost:5173"]
    allowed_methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    allowed_headers: ["Content-Type", "Authorization"]
    expose_headers: ["X-Total-Count"]
    allow_credentials: true

database:
  mysql:
    host: localhost
    port: 3306
    database: trading
    max_open_conns: 25
    max_idle_conns: 10
    conn_max_lifetime: 5m
  
  influx:
    url: http://localhost:8086
    org: trading-org
    bucket: trading
    timeout: 5s

cache:
  redis:
    addr: localhost:6379
    pool_size: 10
    min_idle_conns: 5

messaging:
  nats:
    url: nats://localhost:4222
    client_name: trade-back
    cluster_id: trading-cluster

exchange:
  ws_endpoint: wss://stream.binance.com:9443
  rest_endpoint: https://api.binance.com
  max_connections: 3
  reconnect_interval: 5s
  ping_interval: 30s
```

## Deployment

### Docker Compose Setup

```yaml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      - CONFIG_PATH=/app/configs/config.yaml
    depends_on:
      - mysql
      - influxdb
      - redis
      - nats
    volumes:
      - ./configs:/app/configs

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_DATABASE: trading
      MYSQL_USER: trading
      MYSQL_PASSWORD: tradingpass
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql

  influxdb:
    image: influxdb:2.7
    environment:
      DOCKER_INFLUXDB_INIT_MODE: setup
      DOCKER_INFLUXDB_INIT_USERNAME: admin
      DOCKER_INFLUXDB_INIT_PASSWORD: adminpass
      DOCKER_INFLUXDB_INIT_ORG: trading-org
      DOCKER_INFLUXDB_INIT_BUCKET: trading
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: my-super-secret-auth-token
    ports:
      - "8086:8086"
    volumes:
      - influxdb_data:/var/lib/influxdb2

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  nats:
    image: nats:2.10-alpine
    ports:
      - "4222:4222"
      - "8222:8222"
    command: "--http_port 8222"

volumes:
  mysql_data:
  influxdb_data:
  redis_data:
```

### Production Deployment

#### 1. Build
```bash
# Build production binary
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o trade-back ./cmd/trade-back

# Build Docker image
docker build -t trade-back:latest .
```

#### 2. Database Migrations
```bash
# Run migrations
./trade-back migrate up --config configs/config.yaml
```

#### 3. Health Checks
```bash
# Application health
curl http://localhost:8080/api/v1/health

# Component health
curl http://localhost:8080/api/v1/health/detailed
```

## Performance Optimizations

### 1. Binary Protocol
- **72% bandwidth reduction** compared to JSON
- **16x faster parsing** with direct memory access
- **Zero allocations** for number parsing

### 2. Batching Strategy
- Price updates batched every 50ms or 50 items
- InfluxDB writes batched at 100 items or 100ms
- Reduces system calls and network overhead

### 3. Connection Pooling
- Multiple Binance connections for load distribution
- Automatic failover between connections
- Exponential backoff for reconnections

### 4. Caching Layer
- Redis for current price lookups (sub-ms latency)
- API response caching for historical data
- Session caching to avoid database hits

### 5. Concurrent Processing
- 4 worker threads for price processing
- Parallel database writes
- Non-blocking WebSocket message delivery

### 6. Memory Optimizations
- Object pooling for frequent allocations
- Preallocated buffers for binary encoding
- Efficient data structures (maps with initial capacity)

### 7. Database Optimizations
- Indexed columns for frequent queries
- Batch inserts for time-series data
- Connection pooling with configurable limits

## Monitoring and Observability

### Metrics Exposed
- Message processing rate
- Latency percentiles (p50, p95, p99)
- Connection status and health
- Error rates and types
- Database query performance
- WebSocket connection count

### Logging
- Structured JSON logging
- Configurable log levels
- Error aggregation and rate limiting
- Performance metrics in logs

### Health Endpoints
```http
GET /api/v1/health
GET /api/v1/health/detailed
GET /api/v1/metrics
```

## Security Considerations

1. **Authentication**: Session-based with secure cookies
2. **Authorization**: Per-session symbol access control
3. **Data Integrity**: CRC32 checksums on binary protocol
4. **Rate Limiting**: Configurable per-endpoint limits
5. **CORS**: Restricted origin access
6. **Input Validation**: Strict parameter validation
7. **SQL Injection**: Prepared statements throughout
8. **XSS Prevention**: Proper output encoding

## Conclusion

Trade-Back represents a sophisticated approach to real-time market data processing, combining multiple optimization techniques to achieve ultra-low latency delivery. The architecture is designed for horizontal scalability, high availability, and professional-grade trading applications where every microsecond matters.