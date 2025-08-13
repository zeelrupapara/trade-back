package exchange

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// ExchangeClient defines the interface for exchange clients
type ExchangeClient interface {
	Connect(ctx context.Context) error
	Disconnect()
	IsConnected() bool
	GetSymbols() []string
	GetMessageCount() int64
	SetPriceHandler(id string, handler MessageHandler)
	RemovePriceHandler(id string)
}

// ExchangeType represents different exchange types
type ExchangeType string

const (
	ExchangeTypeBinance ExchangeType = "binance"
	ExchangeTypeOANDA   ExchangeType = "oanda"
)

// ExchangePool represents a pool of connections for a specific exchange
type ExchangePool struct {
	exchangeType ExchangeType
	clients      []ExchangeClient
	symbols      []string
	maxSymbols   int
	running      atomic.Bool
	
	// Configuration
	cfg          *config.ExchangeConfig
	logger       *logrus.Entry
	
	// Handlers
	priceHandler func(*models.PriceData)
	
	// Synchronization
	mu           sync.RWMutex
	wg           sync.WaitGroup
	done         chan struct{}
	reconnect    chan int
}

// MultiExchangeConnectionPool manages connections for multiple exchanges
type MultiExchangeConnectionPool struct {
	pools        map[ExchangeType]*ExchangePool
	buffer       *CircularBuffer
	
	// Configuration
	cfg          *config.Config
	logger       *logrus.Entry
	
	// Handlers
	priceHandler func(*models.PriceData)
	
	// State
	running      atomic.Bool
	
	// Synchronization
	mu           sync.RWMutex
	wg           sync.WaitGroup
	done         chan struct{}
}

// NewMultiExchangeConnectionPool creates a new multi-exchange connection pool
func NewMultiExchangeConnectionPool(cfg *config.Config, logger *logrus.Logger) *MultiExchangeConnectionPool {
	return &MultiExchangeConnectionPool{
		pools:        make(map[ExchangeType]*ExchangePool),
		buffer:       NewCircularBuffer(10000),
		cfg:          cfg,
		logger:       logger.WithField("component", "multi-exchange-pool"),
		done:         make(chan struct{}),
	}
}

// AddExchangeSymbols adds symbols for a specific exchange
func (mp *MultiExchangeConnectionPool) AddExchangeSymbols(exchangeType ExchangeType, symbols []string) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	
	if len(symbols) == 0 {
		return
	}
	
	pool := &ExchangePool{
		exchangeType: exchangeType,
		symbols:      symbols,
		maxSymbols:   mp.cfg.Exchange.MaxSymbolsPerConnection,
		cfg:          &mp.cfg.Exchange,
		logger:       mp.logger.WithField("exchange", exchangeType),
		done:         make(chan struct{}),
		reconnect:    make(chan int, 10),
	}
	
	// Adjust max symbols for OANDA (lower limit due to different architecture)
	if exchangeType == ExchangeTypeOANDA {
		pool.maxSymbols = mp.cfg.Exchange.OANDA.MaxInstruments
	}
	
	mp.pools[exchangeType] = pool
	
	mp.logger.WithFields(logrus.Fields{
		"exchange": exchangeType,
		"symbols":  len(symbols),
	}).Info("Added exchange symbols to multi-exchange pool")
}

// Start starts all exchange pools
func (mp *MultiExchangeConnectionPool) Start(ctx context.Context) error {
	if mp.running.Load() {
		return nil
	}
	
	mp.logger.Info("Starting multi-exchange connection pool...")
	
	mp.mu.Lock()
	defer mp.mu.Unlock()
	
	// Start each exchange pool
	for exchangeType, pool := range mp.pools {
		if err := mp.startExchangePool(ctx, exchangeType, pool); err != nil {
			mp.logger.WithError(err).WithField("exchange", exchangeType).Error("Failed to start exchange pool")
			continue
		}
	}
	
	mp.running.Store(true)
	
	// Start monitoring
	mp.wg.Add(1)
	go mp.monitorPools(ctx)
	
	mp.logger.Info("Multi-exchange connection pool started")
	return nil
}

// startExchangePool starts a specific exchange pool
func (mp *MultiExchangeConnectionPool) startExchangePool(ctx context.Context, exchangeType ExchangeType, pool *ExchangePool) error {
	// Calculate number of connections needed
	numConnections := (len(pool.symbols) + pool.maxSymbols - 1) / pool.maxSymbols
	
	for i := 0; i < numConnections; i++ {
		// Calculate symbol range for this client
		start := i * pool.maxSymbols
		end := min(start+pool.maxSymbols, len(pool.symbols))
		clientSymbols := pool.symbols[start:end]
		
		var client ExchangeClient
		var err error
		
		// Create appropriate client based on exchange type
		switch exchangeType {
		case ExchangeTypeBinance:
			binanceClient := NewBinanceClient(
				fmt.Sprintf("binance-client-%d", i),
				clientSymbols,
				&mp.cfg.Exchange,
				pool.logger.Logger,
			)
			client = binanceClient
			
		case ExchangeTypeOANDA:
			oandaClient := NewOANDAClient(
				fmt.Sprintf("oanda-client-%d", i),
				clientSymbols,
				&mp.cfg.Exchange.OANDA,
				pool.logger.Logger,
			)
			client = oandaClient
			
		default:
			return fmt.Errorf("unsupported exchange type: %s", exchangeType)
		}
		
		// Set price handler
		client.SetPriceHandler("pool", mp.handlePriceUpdate)
		
		// Connect
		if err = client.Connect(ctx); err != nil {
			pool.logger.WithError(err).WithField("client", i).Error("Failed to connect client")
			continue
		}
		
		pool.clients = append(pool.clients, client)
		
		pool.logger.WithFields(logrus.Fields{
			"client":  i,
			"symbols": len(clientSymbols),
		}).Info("Connected exchange client")
		
		// Add delay between connections to avoid rate limiting
		if i < numConnections-1 {
			time.Sleep(2 * time.Second)
		}
	}
	
	pool.running.Store(true)
	
	// Start pool monitoring
	pool.wg.Add(1)
	go mp.monitorExchangePool(ctx, exchangeType, pool)
	
	// Start reconnection handler
	pool.wg.Add(1)
	go mp.reconnectionHandler(ctx, exchangeType, pool)
	
	return nil
}

// Stop stops all exchange pools
func (mp *MultiExchangeConnectionPool) Stop() error {
	if !mp.running.Load() {
		return nil
	}
	
	mp.logger.Info("Stopping multi-exchange connection pool...")
	
	close(mp.done)
	mp.running.Store(false)
	
	mp.mu.Lock()
	defer mp.mu.Unlock()
	
	// Stop all pools
	var wg sync.WaitGroup
	for exchangeType, pool := range mp.pools {
		wg.Add(1)
		go func(exchType ExchangeType, p *ExchangePool) {
			defer wg.Done()
			mp.stopExchangePool(exchType, p)
		}(exchangeType, pool)
	}
	
	wg.Wait()
	mp.wg.Wait()
	
	mp.logger.Info("Multi-exchange connection pool stopped")
	return nil
}

// stopExchangePool stops a specific exchange pool
func (mp *MultiExchangeConnectionPool) stopExchangePool(exchangeType ExchangeType, pool *ExchangePool) {
	pool.logger.Info("Stopping exchange pool...")
	
	close(pool.done)
	pool.running.Store(false)
	
	// Disconnect all clients
	var wg sync.WaitGroup
	for i, client := range pool.clients {
		wg.Add(1)
		go func(idx int, c ExchangeClient) {
			defer wg.Done()
			c.Disconnect()
		}(i, client)
	}
	
	wg.Wait()
	pool.wg.Wait()
	
	pool.logger.Info("Exchange pool stopped")
}

// SetPriceHandler sets the price update handler
func (mp *MultiExchangeConnectionPool) SetPriceHandler(handler func(*models.PriceData)) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.priceHandler = handler
}

// GetBuffer returns the circular buffer
func (mp *MultiExchangeConnectionPool) GetBuffer() *CircularBuffer {
	return mp.buffer
}

// IsRunning returns whether the pool is running
func (mp *MultiExchangeConnectionPool) IsRunning() bool {
	return mp.running.Load()
}

// GetConnectionStatus returns status of all connections across all exchanges
func (mp *MultiExchangeConnectionPool) GetConnectionStatus() map[string]bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	
	status := make(map[string]bool)
	for exchangeType, pool := range mp.pools {
		for i, client := range pool.clients {
			clientKey := fmt.Sprintf("%s-client-%d", exchangeType, i)
			status[clientKey] = client.IsConnected()
		}
	}
	return status
}

// GetExchangeStatus returns status for a specific exchange
func (mp *MultiExchangeConnectionPool) GetExchangeStatus(exchangeType ExchangeType) map[string]interface{} {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	
	pool, exists := mp.pools[exchangeType]
	if !exists {
		return map[string]interface{}{
			"exists":    false,
			"connected": false,
		}
	}
	
	connectedClients := 0
	totalMessages := int64(0)
	
	for _, client := range pool.clients {
		if client.IsConnected() {
			connectedClients++
		}
		totalMessages += client.GetMessageCount()
	}
	
	return map[string]interface{}{
		"exists":           true,
		"running":          pool.running.Load(),
		"total_clients":    len(pool.clients),
		"connected_clients": connectedClients,
		"total_symbols":    len(pool.symbols),
		"total_messages":   totalMessages,
	}
}

// handlePriceUpdate handles price updates from all exchange clients
func (mp *MultiExchangeConnectionPool) handlePriceUpdate(price *models.PriceData) {
	// Add to buffer for gap detection
	mp.buffer.Add(price)
	
	// Call registered handler
	mp.mu.RLock()
	handler := mp.priceHandler
	mp.mu.RUnlock()
	
	if handler != nil {
		handler(price)
	}
}

// monitorPools monitors all exchange pools
func (mp *MultiExchangeConnectionPool) monitorPools(ctx context.Context) {
	defer mp.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-mp.done:
			return
		case <-ticker.C:
			// Log overall statistics
			mp.logStatistics()
		}
	}
}

// monitorExchangePool monitors a specific exchange pool
func (mp *MultiExchangeConnectionPool) monitorExchangePool(ctx context.Context, exchangeType ExchangeType, pool *ExchangePool) {
	defer pool.wg.Done()
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-pool.done:
			return
		case <-ticker.C:
			// Check all connections in this pool
			for i, client := range pool.clients {
				if !client.IsConnected() {
					pool.logger.WithField("client", i).Warn("Client disconnected, triggering reconnection")
					
					select {
					case pool.reconnect <- i:
					default:
						// Reconnection already queued
					}
				}
			}
		}
	}
}

// reconnectionHandler handles client reconnections for a specific exchange
func (mp *MultiExchangeConnectionPool) reconnectionHandler(ctx context.Context, exchangeType ExchangeType, pool *ExchangePool) {
	defer pool.wg.Done()
	
	backoff := pool.cfg.ReconnectDelay
	maxBackoff := 30 * time.Second
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-pool.done:
			return
		case clientIdx := <-pool.reconnect:
			if clientIdx < 0 || clientIdx >= len(pool.clients) {
				continue
			}
			
			client := pool.clients[clientIdx]
			
			// Exponential backoff
			attempts := 0
			for attempts < pool.cfg.MaxReconnectAttempts {
				attempts++
				
				// Wait before reconnecting
				select {
				case <-ctx.Done():
					return
				case <-pool.done:
					return
				case <-time.After(backoff):
				}
				
				// Try to reconnect
				if err := client.Connect(ctx); err != nil {
					pool.logger.WithError(err).WithFields(logrus.Fields{
						"client":   clientIdx,
						"attempt":  attempts,
						"exchange": exchangeType,
					}).Error("Reconnection failed")
					
					// Increase backoff
					backoff = time.Duration(float64(backoff) * 1.5)
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}
				
				pool.logger.WithFields(logrus.Fields{
					"client":   clientIdx,
					"exchange": exchangeType,
				}).Info("Client reconnected successfully")
				
				// Reset backoff
				backoff = pool.cfg.ReconnectDelay
				break
			}
			
			if attempts >= pool.cfg.MaxReconnectAttempts {
				pool.logger.WithFields(logrus.Fields{
					"client":   clientIdx,
					"exchange": exchangeType,
				}).Error("Max reconnection attempts reached")
			}
		}
	}
}

// logStatistics logs overall pool statistics
func (mp *MultiExchangeConnectionPool) logStatistics() {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	
	bufferStats := mp.buffer.Stats()
	
	totalClients := 0
	connectedClients := 0
	totalSymbols := 0
	
	for exchangeType, pool := range mp.pools {
		poolConnected := 0
		for _, client := range pool.clients {
			totalClients++
			totalSymbols += len(client.GetSymbols())
			if client.IsConnected() {
				connectedClients++
				poolConnected++
			}
		}
		
		pool.logger.WithFields(logrus.Fields{
			"exchange":          exchangeType,
			"clients":           len(pool.clients),
			"connected_clients": poolConnected,
			"symbols":           len(pool.symbols),
		}).Debug("Exchange pool status")
	}
	
	mp.logger.WithFields(logrus.Fields{
		"total_clients":      totalClients,
		"connected_clients":  connectedClients,
		"total_symbols":      totalSymbols,
		"buffer_size":        bufferStats.Size,
		"buffer_capacity":    bufferStats.Capacity,
		"messages_total":     bufferStats.TotalMessages,
	}).Info("Multi-exchange pool statistics")
}