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

// ConnectionPool manages multiple Binance WebSocket connections
type ConnectionPool struct {
	clients        []*BinanceClient
	activeIdx      atomic.Int32
	symbols        []string
	maxSymbols     int
	
	// State
	running        atomic.Bool
	
	// Buffering
	buffer         *CircularBuffer
	
	// Handlers
	priceHandler   func(*models.PriceData)
	
	// Configuration
	cfg            *config.ExchangeConfig
	logger         *logrus.Entry
	
	// Synchronization
	mu             sync.RWMutex
	wg             sync.WaitGroup
	
	// Channels
	done           chan struct{}
	reconnect      chan int // Client index to reconnect
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(symbols []string, cfg *config.ExchangeConfig, logger *logrus.Logger) *ConnectionPool {
	return &ConnectionPool{
		clients:    make([]*BinanceClient, 0),
		symbols:    symbols,
		maxSymbols: cfg.MaxSymbolsPerConnection,
		buffer:     NewCircularBuffer(10000), // 10k message buffer
		cfg:        cfg,
		logger:     logger.WithField("component", "connection-pool"),
		done:       make(chan struct{}),
		reconnect:  make(chan int, 10),
	}
}

// Start starts the connection pool
func (cp *ConnectionPool) Start(ctx context.Context) error {
	if cp.running.Load() {
		return nil
	}
	
	cp.logger.Info("Starting connection pool...")
	
	// Calculate number of connections needed
	numConnections := (len(cp.symbols) + cp.maxSymbols - 1) / cp.maxSymbols
	cp.logger.WithField("connections", numConnections).Info("Creating connections for symbols")
	
	// Create connections
	for i := 0; i < numConnections; i++ {
		// Calculate symbol range for this client
		start := i * cp.maxSymbols
		end := min(start+cp.maxSymbols, len(cp.symbols))
		clientSymbols := cp.symbols[start:end]
		
		client := NewBinanceClient(fmt.Sprintf("client-%d", i), clientSymbols, cp.cfg, cp.logger.Logger)
		
		// Register handler for all symbols
		client.RegisterHandler("all", cp.handlePriceUpdate)
		
		cp.clients = append(cp.clients, client)
		
		// Connect (official client handles subscription automatically)
		if err := client.Connect(ctx); err != nil {
			cp.logger.WithError(err).WithField("client", i).Error("Failed to connect client")
			continue
		}
		
		cp.logger.WithFields(logrus.Fields{
			"client":  i,
			"symbols": len(clientSymbols),
		}).Info("Client connected and subscribed")
	}
	
	cp.running.Store(true)
	
	// Start reconnection handler
	cp.wg.Add(1)
	go cp.reconnectionHandler(ctx)
	
	// Start monitoring
	cp.wg.Add(1)
	go cp.monitorConnections(ctx)
	
	return nil
}

// Stop stops the connection pool
func (cp *ConnectionPool) Stop() error {
	if !cp.running.Load() {
		return nil
	}
	
	cp.logger.Info("Stopping connection pool...")
	
	close(cp.done)
	cp.running.Store(false)
	
	// Disconnect all clients
	var wg sync.WaitGroup
	for i, client := range cp.clients {
		wg.Add(1)
		go func(idx int, c *BinanceClient) {
			defer wg.Done()
			if err := c.Close(); err != nil {
				cp.logger.WithError(err).WithField("client", idx).Error("Failed to disconnect")
			}
		}(i, client)
	}
	
	wg.Wait()
	cp.wg.Wait()
	
	cp.logger.Info("Connection pool stopped")
	return nil
}

// SetPriceHandler sets the price update handler
func (cp *ConnectionPool) SetPriceHandler(handler func(*models.PriceData)) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.priceHandler = handler
}

// GetActiveClient returns the current active client
func (cp *ConnectionPool) GetActiveClient() *BinanceClient {
	idx := cp.activeIdx.Load()
	if idx >= 0 && int(idx) < len(cp.clients) {
		return cp.clients[idx]
	}
	return nil
}

// GetBuffer returns the circular buffer
func (cp *ConnectionPool) GetBuffer() *CircularBuffer {
	return cp.buffer
}

// IsRunning returns whether the pool is running
func (cp *ConnectionPool) IsRunning() bool {
	return cp.running.Load()
}

// GetConnectionStatus returns status of all connections
func (cp *ConnectionPool) GetConnectionStatus() map[string]bool {
	status := make(map[string]bool)
	for i, client := range cp.clients {
		status[fmt.Sprintf("client-%d", i)] = client.IsConnected()
	}
	return status
}

// handlePriceUpdate handles price updates from clients
func (cp *ConnectionPool) handlePriceUpdate(price *models.PriceData) {
	// Add to buffer for gap detection
	cp.buffer.Add(price)
	
	// Call registered handler
	cp.mu.RLock()
	handler := cp.priceHandler
	cp.mu.RUnlock()
	
	if handler != nil {
		handler(price)
	}
}

// reconnectionHandler handles client reconnections
func (cp *ConnectionPool) reconnectionHandler(ctx context.Context) {
	defer cp.wg.Done()
	
	backoff := cp.cfg.ReconnectDelay
	maxBackoff := 30 * time.Second
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.done:
			return
		case clientIdx := <-cp.reconnect:
			if clientIdx < 0 || clientIdx >= len(cp.clients) {
				continue
			}
			
			client := cp.clients[clientIdx]
			
			cp.logger.WithField("client", clientIdx).Info("Attempting to reconnect client")
			
			// Exponential backoff
			attempts := 0
			for attempts < cp.cfg.MaxReconnectAttempts {
				attempts++
				
				// Wait before reconnecting
				select {
				case <-ctx.Done():
					return
				case <-cp.done:
					return
				case <-time.After(backoff):
				}
				
				// Try to connect (official client handles subscription automatically)
				if err := client.Connect(ctx); err != nil {
					cp.logger.WithError(err).WithFields(logrus.Fields{
						"client":  clientIdx,
						"attempt": attempts,
					}).Error("Reconnection failed")
					
					// Increase backoff
					backoff = time.Duration(float64(backoff) * 1.5)
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}
				
				// Get symbols for this client
				start := clientIdx * cp.maxSymbols
				end := min(start+cp.maxSymbols, len(cp.symbols))
				clientSymbols := cp.symbols[start:end]
				
				cp.logger.WithField("client", clientIdx).Info("Client reconnected successfully")
				
				// Check for gaps and request backfill if needed
				cp.checkAndBackfill(clientIdx, clientSymbols)
				
				// Reset backoff
				backoff = cp.cfg.ReconnectDelay
				break
			}
			
			if attempts >= cp.cfg.MaxReconnectAttempts {
				cp.logger.WithField("client", clientIdx).Error("Max reconnection attempts reached")
				// TODO: Alert monitoring system
			}
		}
	}
}

// monitorConnections monitors connection health
func (cp *ConnectionPool) monitorConnections(ctx context.Context) {
	defer cp.wg.Done()
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.done:
			return
		case <-ticker.C:
			// Check all connections
			for i, client := range cp.clients {
				if !client.IsConnected() {
					cp.logger.WithField("client", i).Warn("Client disconnected, triggering reconnection")
					
					select {
					case cp.reconnect <- i:
					default:
						// Reconnection already queued
					}
				}
			}
			
			// Log buffer statistics
			stats := cp.buffer.Stats()
			cp.logger.WithFields(logrus.Fields{
				"buffer_size":     stats.Size,
				"buffer_capacity": stats.Capacity,
				"messages_total":  stats.TotalMessages,
			}).Debug("Buffer statistics")
		}
	}
}

// checkAndBackfill checks for gaps and requests historical data
func (cp *ConnectionPool) checkAndBackfill(clientIdx int, symbols []string) {
	// Get latest messages from buffer for these symbols
	recentMessages := cp.buffer.GetRecentBySymbols(symbols, 100)
	
	if len(recentMessages) == 0 {
		cp.logger.WithField("client", clientIdx).Info("No recent messages to check for gaps")
		return
	}
	
	// Group by symbol to check sequences
	symbolMessages := make(map[string][]*models.PriceData)
	for _, msg := range recentMessages {
		symbolMessages[msg.Symbol] = append(symbolMessages[msg.Symbol], msg)
	}
	
	// Check each symbol for gaps
	for symbol, messages := range symbolMessages {
		if len(messages) < 2 {
			continue
		}
		
		// Check for sequence gaps
		for i := 1; i < len(messages); i++ {
			expectedSeq := messages[i-1].Sequence + 1
			actualSeq := messages[i].Sequence
			
			if actualSeq > expectedSeq {
				gap := actualSeq - expectedSeq
				cp.logger.WithFields(logrus.Fields{
					"symbol":   symbol,
					"gap_size": gap,
					"from_seq": expectedSeq,
					"to_seq":   actualSeq - 1,
				}).Warn("Sequence gap detected")
				
				// TODO: Request historical data to fill the gap
			}
		}
	}
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}