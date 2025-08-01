package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// RedisClient handles Redis caching operations
type RedisClient struct {
	client *redis.Client
	logger *logrus.Entry
	cfg    *config.RedisConfig
	ttl    time.Duration
}

// NewRedisClient creates a new Redis client
func NewRedisClient(cfg *config.RedisConfig, logger *logrus.Logger) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		// Additional settings to prevent connection issues
		PoolTimeout:     4 * time.Second,  // Timeout for getting connection from pool
		IdleTimeout:     5 * time.Minute,  // Close idle connections after this duration
		MaxRetries:      2,                // Max retries before giving up
		IdleCheckFrequency: time.Minute,  // How often to check for idle connections
	})
	
	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}
	
	return &RedisClient{
		client: client,
		logger: logger.WithField("component", "redis"),
		cfg:    cfg,
		ttl:    5 * time.Minute, // Default TTL
	}, nil
}

// Close closes the Redis connection
func (rc *RedisClient) Close() error {
	return rc.client.Close()
}

// Health checks Redis health
func (rc *RedisClient) Health(ctx context.Context) error {
	return rc.client.Ping(ctx).Err()
}

// SetTTL sets the default TTL for cache entries
func (rc *RedisClient) SetTTL(ttl time.Duration) {
	rc.ttl = ttl
}

// Price operations

// SetPrice sets the current price for a symbol
func (rc *RedisClient) SetPrice(ctx context.Context, symbol string, price *models.PriceData) error {
	key := fmt.Sprintf("price:%s", symbol)
	
	data, err := json.Marshal(price)
	if err != nil {
		return fmt.Errorf("failed to marshal price data: %w", err)
	}
	
	return rc.client.Set(ctx, key, data, rc.ttl).Err()
}

// GetPrice gets the current price for a symbol
func (rc *RedisClient) GetPrice(ctx context.Context, symbol string) (*models.PriceData, error) {
	key := fmt.Sprintf("price:%s", symbol)
	
	data, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get price: %w", err)
	}
	
	var price models.PriceData
	if err := json.Unmarshal([]byte(data), &price); err != nil {
		return nil, fmt.Errorf("failed to unmarshal price data: %w", err)
	}
	
	return &price, nil
}

// SetPriceBatch sets multiple prices in a batch
func (rc *RedisClient) SetPriceBatch(ctx context.Context, prices []*models.PriceData) error {
	pipe := rc.client.Pipeline()
	
	for _, price := range prices {
		key := fmt.Sprintf("price:%s", price.Symbol)
		
		data, err := json.Marshal(price)
		if err != nil {
			return fmt.Errorf("failed to marshal price data for %s: %w", price.Symbol, err)
		}
		
		pipe.Set(ctx, key, data, rc.ttl)
	}
	
	_, err := pipe.Exec(ctx)
	return err
}

// GetPrices gets prices for multiple symbols
func (rc *RedisClient) GetPrices(ctx context.Context, symbols []string) (map[string]*models.PriceData, error) {
	pipe := rc.client.Pipeline()
	
	// Create commands
	cmds := make(map[string]*redis.StringCmd)
	for _, symbol := range symbols {
		key := fmt.Sprintf("price:%s", symbol)
		cmds[symbol] = pipe.Get(ctx, key)
	}
	
	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to execute pipeline: %w", err)
	}
	
	// Process results
	results := make(map[string]*models.PriceData)
	for symbol, cmd := range cmds {
		data, err := cmd.Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			rc.logger.WithError(err).WithField("symbol", symbol).Warn("Failed to get price")
			continue
		}
		
		var price models.PriceData
		if err := json.Unmarshal([]byte(data), &price); err != nil {
			rc.logger.WithError(err).WithField("symbol", symbol).Warn("Failed to unmarshal price")
			continue
		}
		
		results[symbol] = &price
	}
	
	return results, nil
}

// Bar operations

// SetBars sets OHLCV bars for a symbol
func (rc *RedisClient) SetBars(ctx context.Context, symbol string, resolution string, bars []*models.Bar) error {
	key := fmt.Sprintf("bars:%s:%s", symbol, resolution)
	
	data, err := json.Marshal(bars)
	if err != nil {
		return fmt.Errorf("failed to marshal bars: %w", err)
	}
	
	return rc.client.Set(ctx, key, data, rc.ttl).Err()
}

// GetBars gets OHLCV bars for a symbol
func (rc *RedisClient) GetBars(ctx context.Context, symbol string, resolution string) ([]*models.Bar, error) {
	key := fmt.Sprintf("bars:%s:%s", symbol, resolution)
	
	data, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get bars: %w", err)
	}
	
	var bars []*models.Bar
	if err := json.Unmarshal([]byte(data), &bars); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bars: %w", err)
	}
	
	return bars, nil
}

// Enigma operations

// SetEnigmaLevel sets Enigma level data for a symbol
func (rc *RedisClient) SetEnigmaLevel(ctx context.Context, symbol string, enigma *models.EnigmaData) error {
	key := fmt.Sprintf("enigma:%s", symbol)
	
	data, err := json.Marshal(enigma)
	if err != nil {
		return fmt.Errorf("failed to marshal enigma data: %w", err)
	}
	
	return rc.client.Set(ctx, key, data, 24*time.Hour).Err() // Longer TTL for Enigma
}

// GetEnigmaLevel gets Enigma level data for a symbol
func (rc *RedisClient) GetEnigmaLevel(ctx context.Context, symbol string) (*models.EnigmaData, error) {
	key := fmt.Sprintf("enigma:%s", symbol)
	
	data, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get enigma level: %w", err)
	}
	
	var enigma models.EnigmaData
	if err := json.Unmarshal([]byte(data), &enigma); err != nil {
		return nil, fmt.Errorf("failed to unmarshal enigma data: %w", err)
	}
	
	return &enigma, nil
}

// Market watch operations

// AddToMarketWatch adds a symbol to market watch
func (rc *RedisClient) AddToMarketWatch(ctx context.Context, token string, symbol string) error {
	key := fmt.Sprintf("marketwatch:%s", token)
	return rc.client.SAdd(ctx, key, symbol).Err()
}

// RemoveFromMarketWatch removes a symbol from market watch
func (rc *RedisClient) RemoveFromMarketWatch(ctx context.Context, token string, symbol string) error {
	key := fmt.Sprintf("marketwatch:%s", token)
	return rc.client.SRem(ctx, key, symbol).Err()
}

// GetMarketWatch gets all symbols in market watch
func (rc *RedisClient) GetMarketWatch(ctx context.Context, token string) ([]string, error) {
	key := fmt.Sprintf("marketwatch:%s", token)
	return rc.client.SMembers(ctx, key).Result()
}

// Session operations

// SetSessionData stores session-specific data
func (rc *RedisClient) SetSessionData(ctx context.Context, sessionID string, data interface{}) error {
	key := fmt.Sprintf("session:%s", sessionID)
	
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal session data: %w", err)
	}
	
	return rc.client.Set(ctx, key, jsonData, 24*time.Hour).Err()
}

// GetSessionData retrieves session-specific data
func (rc *RedisClient) GetSessionData(ctx context.Context, sessionID string, dest interface{}) error {
	key := fmt.Sprintf("session:%s", sessionID)
	
	data, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return fmt.Errorf("session not found")
	}
	if err != nil {
		return fmt.Errorf("failed to get session data: %w", err)
	}
	
	return json.Unmarshal([]byte(data), dest)
}

// Utility operations

// DeletePattern deletes all keys matching a pattern
func (rc *RedisClient) DeletePattern(ctx context.Context, pattern string) error {
	var cursor uint64
	var keys []string
	
	for {
		var err error
		var batch []string
		batch, cursor, err = rc.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("failed to scan keys: %w", err)
		}
		
		keys = append(keys, batch...)
		
		if cursor == 0 {
			break
		}
	}
	
	if len(keys) > 0 {
		return rc.client.Del(ctx, keys...).Err()
	}
	
	return nil
}

// FlushDB flushes the entire database (use with caution)
func (rc *RedisClient) FlushDB(ctx context.Context) error {
	return rc.client.FlushDB(ctx).Err()
}

// SetJSON stores a JSON-encoded value
func (rc *RedisClient) SetJSON(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	
	return rc.client.Set(ctx, key, data, expiration).Err()
}

// GetJSON retrieves and decodes a JSON value
func (rc *RedisClient) GetJSON(ctx context.Context, key string, dest interface{}) (bool, error) {
	data, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	
	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return false, fmt.Errorf("failed to unmarshal value: %w", err)
	}
	
	return true, nil
}

// Get retrieves a value by key
func (rc *RedisClient) Get(ctx context.Context, key string) (string, error) {
	return rc.client.Get(ctx, key).Result()
}

// Set stores a value with expiration
func (rc *RedisClient) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return rc.client.Set(ctx, key, value, expiration).Err()
}

// Delete removes a key
func (rc *RedisClient) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return rc.client.Del(ctx, keys...).Err()
}

// GetInfo returns Redis server information
func (rc *RedisClient) GetInfo(ctx context.Context) (map[string]string, error) {
	info, err := rc.client.Info(ctx).Result()
	if err != nil {
		return nil, err
	}
	
	// Parse info string into map
	infoMap := make(map[string]string)
	// TODO: Parse Redis INFO output
	infoMap["raw"] = info
	
	return infoMap, nil
}

// Publish publishes a message to a Redis channel
func (rc *RedisClient) Publish(ctx context.Context, channel string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	
	return rc.client.Publish(ctx, channel, data).Err()
}

// Subscribe subscribes to a Redis channel
func (rc *RedisClient) Subscribe(ctx context.Context, handler func(channel string, data []byte), channels ...string) error {
	pubsub := rc.client.Subscribe(ctx, channels...)
	defer pubsub.Close()
	
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-ch:
			if msg != nil {
				handler(msg.Channel, []byte(msg.Payload))
			}
		}
	}
}

// GetAllSessionTokens returns all active session tokens that have market watches
func (rc *RedisClient) GetAllSessionTokens(ctx context.Context) ([]string, error) {
	// Get all keys that match the market watch pattern
	keys, err := rc.client.Keys(ctx, "marketwatch:*").Result()
	if err != nil {
		return nil, err
	}
	
	// Extract session tokens from keys
	tokens := make([]string, 0, len(keys))
	for _, key := range keys {
		// Key format is "marketwatch:TOKEN"
		if parts := strings.SplitN(key, ":", 2); len(parts) == 2 {
			tokens = append(tokens, parts[1])
		}
	}
	
	return tokens, nil
}