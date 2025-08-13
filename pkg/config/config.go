package config

import (
	"context"
	"fmt"
	"time"

	"github.com/sethvargo/go-envconfig"
)

// Config represents the application configuration
type Config struct {
	Server      ServerConfig      `env:", prefix=SERVER_"`
	MySQL       MySQLConfig       `env:", prefix=MYSQL_"`
	InfluxDB    InfluxConfig      `env:", prefix=INFLUXDB_"`
	Redis       RedisConfig       `env:", prefix=REDIS_"`
	NATS        NATSConfig        `env:", prefix=NATS_"`
	Exchange    ExchangeConfig    `env:", prefix=EXCHANGE_"`
	Performance PerformanceConfig `env:", prefix=PERFORMANCE_"`
	Features    FeaturesConfig    `env:", prefix=FEATURES_"`
	Security    SecurityConfig    `env:", prefix=SECURITY_"`
	WebSocket   WebSocketConfig   `env:", prefix=WEBSOCKET_"`
	Webhook     WebhookConfig     `env:", prefix=WEBHOOK_"`
	Logging     LoggingConfig     `env:", prefix=LOG_"`
	Monitoring  MonitoringConfig  `env:", prefix=MONITORING_"`

	// Maintain backward compatibility
	Database  DatabaseConfig
	Cache     CacheConfig
	Messaging MessagingConfig
}

// DatabaseConfig holds database configurations (backward compatibility)
type DatabaseConfig struct {
	MySQL  MySQLConfig
	Influx InfluxConfig
}

// CacheConfig holds cache configuration (backward compatibility)
type CacheConfig struct {
	Redis RedisConfig
}

// MessagingConfig holds messaging configuration (backward compatibility)
type MessagingConfig struct {
	NATS NATSConfig
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Host                   string        `env:"HOST, default=0.0.0.0"`
	Port                   int           `env:"PORT, default=8080"`
	BinaryWebSocketEnabled bool          `env:"BINARY_WEBSOCKET_ENABLED, default=true"`
	ReadTimeout            time.Duration `env:"READ_TIMEOUT, default=30s"`
	WriteTimeout           time.Duration `env:"WRITE_TIMEOUT, default=30s"`
	IdleTimeout            time.Duration `env:"IDLE_TIMEOUT, default=120s"`
}

// MySQLConfig holds MySQL configuration
type MySQLConfig struct {
	Host            string        `env:"HOST, default=localhost"`
	Port            int           `env:"PORT, default=3306"`
	Database        string        `env:"DATABASE, default=trading"`
	User            string        `env:"USER, default=trading"`
	Password        string        `env:"PASSWORD, default=trading123"`
	MaxOpenConns    int           `env:"MAX_OPEN_CONNS, default=25"`
	MaxIdleConns    int           `env:"MAX_IDLE_CONNS, default=5"`
	ConnMaxLifetime time.Duration `env:"CONN_MAX_LIFETIME, default=5m"`

	// Backward compatibility
	Username string
}

// InfluxConfig holds InfluxDB configuration
type InfluxConfig struct {
	URL     string        `env:"URL, default=http://localhost:8086"`
	Token   string        `env:"TOKEN, default=my-super-secret-auth-token"`
	Org     string        `env:"ORG, default=trading-org"`
	Bucket  string        `env:"BUCKET, default=trading"`
	Timeout time.Duration `env:"TIMEOUT, default=10s"`
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Host         string        `env:"HOST, default=localhost"`
	Port         int           `env:"PORT, default=6379"`
	Password     string        `env:"PASSWORD"`
	DB           int           `env:"DB, default=0"`
	PoolSize     int           `env:"POOL_SIZE, default=10"`
	MinIdleConns int           `env:"MIN_IDLE_CONNS, default=5"`
	DialTimeout  time.Duration `env:"DIAL_TIMEOUT, default=5s"`
	ReadTimeout  time.Duration `env:"READ_TIMEOUT, default=3s"`
	WriteTimeout time.Duration `env:"WRITE_TIMEOUT, default=3s"`
}

// NATSConfig holds NATS configuration
type NATSConfig struct {
	URL           string        `env:"URL, default=nats://localhost:4222"`
	MaxReconnect  int           `env:"MAX_RECONNECT, default=10"`
	ReconnectWait time.Duration `env:"RECONNECT_WAIT, default=2s"`
	DrainTimeout  time.Duration `env:"DRAIN_TIMEOUT, default=30s"`
}

// ExchangeConfig holds exchange configuration
type ExchangeConfig struct {
	MaxSymbolsPerConnection int           `env:"MAX_SYMBOLS_PER_CONNECTION, default=200"`
	ReconnectDelay          time.Duration `env:"RECONNECT_DELAY, default=1s"`
	MaxReconnectAttempts    int           `env:"MAX_RECONNECT_ATTEMPTS, default=10"`
	ConnectionTimeout       time.Duration `env:"CONNECTION_TIMEOUT, default=30s"`
	PingInterval            time.Duration `env:"PING_INTERVAL, default=30s"`

	// Exchange-specific configurations
	Binance BinanceConfig
	OANDA   OANDAConfig
}

// BinanceConfig holds Binance-specific configuration
type BinanceConfig struct {
	APIKey    string `env:"BINANCE_API_KEY"`
	SecretKey string `env:"BINANCE_SECRET_KEY"`
	StreamURL string `env:"BINANCE_STREAM_URL, default=wss://stream.binance.com:9443/stream"`
	APIURL    string `env:"BINANCE_API_URL, default=https://api.binance.com"`
}

// OANDAConfig holds OANDA-specific configuration
type OANDAConfig struct {
	APIKey         string        `env:"OANDA_API_KEY"`
	AccountID      string        `env:"OANDA_ACCOUNT_ID"`
	Environment    string        `env:"OANDA_ENVIRONMENT, default=live"` // live or practice
	APIURL         string        `env:"OANDA_API_URL"`                       // Auto-set based on environment
	StreamURL      string        `env:"OANDA_STREAM_URL"`                    // Auto-set based on environment
	MaxInstruments int           `env:"OANDA_MAX_INSTRUMENTS, default=100"`  // Max instruments per stream
	StreamTimeout  time.Duration `env:"OANDA_STREAM_TIMEOUT, default=30s"`
	RetryDelay     time.Duration `env:"OANDA_RETRY_DELAY, default=5s"`
}

// PerformanceConfig holds performance-related configuration
type PerformanceConfig struct {
	BinaryBatchSize      int           `env:"BINARY_BATCH_SIZE, default=50"`
	BinaryBatchInterval  time.Duration `env:"BINARY_BATCH_INTERVAL, default=50ms"`
	HeartbeatInterval    time.Duration `env:"HEARTBEAT_INTERVAL, default=30s"`
	MaxMessageSize       int           `env:"MAX_MESSAGE_SIZE, default=1048576"`
	WriteBufferSize      int           `env:"WRITE_BUFFER_SIZE, default=65536"`
	ReadBufferSize       int           `env:"READ_BUFFER_SIZE, default=65536"`
	PriceBufferSize      int           `env:"PRICE_BUFFER_SIZE, default=1000"`
	PriceUpdateInterval  time.Duration `env:"PRICE_UPDATE_INTERVAL, default=100ms"`
	CacheTTL             time.Duration `env:"CACHE_TTL, default=5s"`
	CacheCleanupInterval time.Duration `env:"CACHE_CLEANUP_INTERVAL, default=1m"`
}

// FeaturesConfig holds feature flags
type FeaturesConfig struct {
	EnigmaEnabled             bool          `env:"ENIGMA_ENABLED, default=true"`
	EnigmaCalculationInterval time.Duration `env:"ENIGMA_CALCULATION_INTERVAL, default=1m"`
	TradingViewEnabled        bool          `env:"TRADINGVIEW_ENABLED, default=true"`
	TradingViewResolutions    []string      `env:"TRADINGVIEW_RESOLUTIONS, default=1,5,15,30,60,240,D,W,M"`
	MarketWatchEnabled        bool          `env:"MARKET_WATCH_ENABLED, default=true"`
	MaxWatchlistSymbols       int           `env:"MAX_WATCHLIST_SYMBOLS, default=100"`
}

// SecurityConfig holds security configuration
type SecurityConfig struct {
	CORSEnabled       bool          `env:"CORS_ENABLED, default=true"`
	CORSOrigins       []string      `env:"CORS_ORIGINS, default=*"`
	CORSMethods       []string      `env:"CORS_METHODS, default=GET,POST,PUT,DELETE,OPTIONS"`
	CORSHeaders       []string      `env:"CORS_HEADERS, default=*"`
	RateLimitEnabled  bool          `env:"RATE_LIMIT_ENABLED, default=false"`
	RateLimitRequests int           `env:"RATE_LIMIT_REQUESTS, default=100"`
	RateLimitWindow   time.Duration `env:"RATE_LIMIT_WINDOW, default=1m"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string `env:"LEVEL, default=info"`
	Format string `env:"FORMAT, default=json"`
	Output string `env:"OUTPUT, default=stdout"`
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	MetricsEnabled     bool `env:"METRICS_ENABLED, default=true"`
	MetricsPort        int  `env:"METRICS_PORT, default=9090"`
	HealthCheckEnabled bool `env:"HEALTH_CHECK_ENABLED, default=true"`
}

// WebSocketConfig holds WebSocket configuration
type WebSocketConfig struct {
	Enabled           bool          `env:"ENABLED, default=true"`
	Port              int           `env:"PORT, default=8080"`
	ReadBufferSize    int           `env:"READ_BUFFER_SIZE, default=1024"`
	WriteBufferSize   int           `env:"WRITE_BUFFER_SIZE, default=1024"`
	MaxMessageSize    int64         `env:"MAX_MESSAGE_SIZE, default=512000"`
	PingInterval      time.Duration `env:"PING_INTERVAL, default=30s"`
	PongTimeout       time.Duration `env:"PONG_TIMEOUT, default=60s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT, default=10s"`
	MaxClients        int           `env:"MAX_CLIENTS, default=10000"`
	EnableCompression bool          `env:"ENABLE_COMPRESSION, default=true"`
}

// WebhookConfig holds webhook configuration
type WebhookConfig struct {
	Enabled           bool          `env:"ENABLED, default=false"`
	TradingViewSecret string        `env:"TRADINGVIEW_SECRET"`
	HMACSecret        string        `env:"HMAC_SECRET"`
	MaxBodySize       int64         `env:"MAX_BODY_SIZE, default=1048576"`
	Timeout           time.Duration `env:"TIMEOUT, default=30s"`
}

// Load loads configuration from environment variables using go-envconfig
func Load() (*Config, error) {
	ctx := context.Background()
	var cfg Config

	// Load main config
	if err := envconfig.Process(ctx, &cfg); err != nil {
		return nil, fmt.Errorf("failed to process config: %w", err)
	}

	// Load exchange configs separately due to nested structure
	var binanceCfg BinanceConfig
	if err := envconfig.Process(ctx, &binanceCfg); err != nil {
		return nil, fmt.Errorf("failed to process binance config: %w", err)
	}
	cfg.Exchange.Binance = binanceCfg

	// Load OANDA config
	var oandaCfg OANDAConfig
	if err := envconfig.Process(ctx, &oandaCfg); err != nil {
		return nil, fmt.Errorf("failed to process oanda config: %w", err)
	}

	// Auto-set OANDA URLs based on environment
	if oandaCfg.Environment == "live" {
		if oandaCfg.APIURL == "" {
			oandaCfg.APIURL = "https://api-fxtrade.oanda.com"
		}
		if oandaCfg.StreamURL == "" {
			oandaCfg.StreamURL = "https://stream-fxtrade.oanda.com"
		}
	} else {
		// Default to practice
		if oandaCfg.APIURL == "" {
			oandaCfg.APIURL = "https://api-fxpractice.oanda.com"
		}
		if oandaCfg.StreamURL == "" {
			oandaCfg.StreamURL = "https://stream-fxpractice.oanda.com"
		}
	}
	cfg.Exchange.OANDA = oandaCfg

	// Set up backward compatibility structures
	cfg.Database = DatabaseConfig{
		MySQL:  cfg.MySQL,
		Influx: cfg.InfluxDB,
	}
	cfg.Cache = CacheConfig{
		Redis: cfg.Redis,
	}
	cfg.Messaging = MessagingConfig{
		NATS: cfg.NATS,
	}

	// Update MySQL Username field for backward compatibility
	cfg.Database.MySQL.Username = cfg.MySQL.User
	cfg.MySQL.Username = cfg.MySQL.User

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// LoadFromEnv is kept for backward compatibility
func LoadFromEnv() (*Config, error) {
	return Load()
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if c.Database.MySQL.Host == "" {
		return fmt.Errorf("MySQL host is required")
	}

	if c.Database.Influx.URL == "" {
		return fmt.Errorf("InfluxDB URL is required")
	}

	if c.Cache.Redis.Host == "" {
		return fmt.Errorf("Redis host is required")
	}

	if c.Messaging.NATS.URL == "" {
		return fmt.Errorf("NATS URL is required")
	}

	return nil
}

// GetMySQLDSN returns MySQL DSN string
func (c *Config) GetMySQLDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		c.Database.MySQL.Username,
		c.Database.MySQL.Password,
		c.Database.MySQL.Host,
		c.Database.MySQL.Port,
		c.Database.MySQL.Database,
	)
}

// GetRedisAddr returns Redis address
func (c *Config) GetRedisAddr() string {
	return fmt.Sprintf("%s:%d", c.Cache.Redis.Host, c.Cache.Redis.Port)
}

// GetServerAddr returns server address
func (c *Config) GetServerAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}
