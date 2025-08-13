package exchange

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// OANDAClient handles OANDA streaming connections
type OANDAClient struct {
	id          string
	config      *config.OANDAConfig
	restClient  *OANDARESTClient
	symbols     []string
	handlers    map[string]MessageHandler
	logger      *logrus.Entry
	
	// Daily stats manager for 24hr data
	dailyStats  *OANDADailyStatsManager
	
	// Connection state
	connected   atomic.Bool
	lastPing    time.Time
	reconnecting atomic.Bool
	reconnectAttempts int
	
	// HTTP connection for streaming
	streamReq   *http.Request
	streamResp  *http.Response
	scanner     *bufio.Scanner
	
	// Channels for control
	done        chan struct{}
	stopCh      chan struct{}
	
	// Mutex for thread safety
	mu          sync.RWMutex
	
	// Metrics
	messageCount atomic.Int64
	lastReset    time.Time
}

// OANDAStreamMessage represents a streaming price update from OANDA
type OANDAStreamMessage struct {
	Type        string                 `json:"type"`
	Time        time.Time             `json:"time"`
	Bids        []OANDAQuote          `json:"bids"`
	Asks        []OANDAQuote          `json:"asks"`
	CloseoutBid string                `json:"closeoutBid"`
	CloseoutAsk string                `json:"closeoutAsk"`
	Status      string                `json:"status"`
	Instrument  string                `json:"instrument"`
	Tradeable   bool                  `json:"tradeable"`
	
	// Heartbeat
	LastTransactionID string `json:"lastTransactionID,omitempty"`
}

// NewOANDAClient creates a new OANDA streaming client
func NewOANDAClient(id string, symbols []string, config *config.OANDAConfig, logger *logrus.Logger) *OANDAClient {
	client := &OANDAClient{
		id:           id,
		symbols:      symbols,
		handlers:     make(map[string]MessageHandler),
		logger:       logger.WithFields(logrus.Fields{"component": "oanda", "client_id": id}),
		done:         make(chan struct{}),
		config:       config,
		restClient:   NewOANDARESTClient(config, logger),
		lastReset:    time.Now(),
	}
	
	// Initialize daily stats manager if we have symbols
	if len(symbols) > 0 {
		client.dailyStats = NewOANDADailyStatsManager(config, symbols, logger)
	}
	
	return client
}

// Connect establishes streaming connection to OANDA
func (oc *OANDAClient) Connect(ctx context.Context) error {
	oc.logger.Info("Connecting to OANDA streaming API...")
	
	// Create streaming request
	url := fmt.Sprintf("%s/v3/accounts/%s/pricing/stream", oc.config.StreamURL, oc.config.AccountID)
	if len(oc.symbols) > 0 {
		url += "?instruments=" + strings.Join(oc.symbols, ",")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+oc.config.APIKey)
	req.Header.Set("Accept", "application/stream+json")

	// Create HTTP client with keep-alive and no timeout for streaming
	transport := &http.Transport{
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     0, // Keep connection alive
		DisableCompression:  true,
		DisableKeepAlives:   false, // Enable keep-alive
	}
	
	client := &http.Client{
		Timeout:   0, // No timeout for streaming
		Transport: transport,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to OANDA stream: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("OANDA stream returned status %d", resp.StatusCode)
	}

	oc.streamReq = req
	oc.streamResp = resp
	oc.scanner = bufio.NewScanner(resp.Body)
	// Set larger buffer for scanner to handle large messages
	oc.scanner.Buffer(make([]byte, 0, 64*1024), 256*1024) // 256KB max message size
	oc.connected.Store(true)
	oc.lastPing = time.Now()

	oc.logger.WithField("symbols", len(oc.symbols)).Info("Connected to OANDA streaming API")

	// Start daily stats manager for 24hr data
	if oc.dailyStats != nil {
		if err := oc.dailyStats.Start(ctx); err != nil {
			oc.logger.WithError(err).Warn("Failed to start daily stats manager")
		}
	}

	// Start monitoring goroutines
	go oc.readStream(ctx)
	go oc.monitorConnection(ctx)

	return nil
}

// readStream reads streaming data from OANDA
func (oc *OANDAClient) readStream(ctx context.Context) {
	defer func() {
		oc.connected.Store(false)
		if oc.streamResp != nil {
			oc.streamResp.Body.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-oc.done:
			return
		default:
		}
		
		if !oc.scanner.Scan() {
			// Scanner stopped, check for error
			if err := oc.scanner.Err(); err != nil {
				oc.logger.WithError(err).Error("Scanner error in OANDA stream")
			} else {
				oc.logger.Warn("OANDA stream ended without error (likely timeout or connection closed by server)")
			}
			return
		}

		line := strings.TrimSpace(oc.scanner.Text())
		if line == "" {
			continue
		}

		// Parse the JSON message
		var message OANDAStreamMessage
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			oc.logger.WithError(err).WithField("line", line).Warn("Failed to parse stream message")
			continue
		}

		// Handle different message types
		switch message.Type {
		case "PRICE":
			oc.handlePriceMessage(&message)
		case "HEARTBEAT":
			oc.handleHeartbeat(&message)
		default:
			oc.logger.WithField("type", message.Type).Debug("Received unknown message type")
		}

		oc.messageCount.Add(1)
	}
}

// handlePriceMessage processes price updates from OANDA
func (oc *OANDAClient) handlePriceMessage(message *OANDAStreamMessage) {
	if !message.Tradeable || message.Status != "tradeable" {
		return
	}

	// Calculate mid price from best bid/ask
	var bidPrice, askPrice float64
	var err error

	if len(message.Bids) > 0 {
		bidPrice, err = strconv.ParseFloat(message.Bids[0].Price, 64)
		if err != nil {
			oc.logger.WithError(err).Warn("Failed to parse bid price")
			return
		}
	}

	if len(message.Asks) > 0 {
		askPrice, err = strconv.ParseFloat(message.Asks[0].Price, 64)
		if err != nil {
			oc.logger.WithError(err).Warn("Failed to parse ask price")
			return
		}
	}

	if bidPrice == 0 || askPrice == 0 {
		return
	}

	// Calculate mid price
	midPrice := (bidPrice + askPrice) / 2.0

	// Create price data with streaming prices
	priceData := &models.PriceData{
		Symbol:    message.Instrument,
		Exchange:  "oanda",
		Price:     midPrice,
		Bid:       bidPrice,
		Ask:       askPrice,
		Volume:    0, // OANDA doesn't provide volume in pricing stream
		Timestamp: message.Time,
	}
	
	// Get 24hr statistics if available
	if oc.dailyStats != nil {
		if stats := oc.dailyStats.GetStats(message.Instrument); stats != nil {
			// Add 24hr statistics to price data
			priceData.Open24h = stats.Open24h
			priceData.High24h = stats.High24h
			priceData.Low24h = stats.Low24h
			priceData.Change24h = stats.Change24h
			priceData.ChangePercent = stats.ChangePercent
			priceData.Volume = float64(stats.Volume24h) // Convert int64 to float64 for volume
			
			// Update high/low if current price exceeds them
			if midPrice > priceData.High24h {
				priceData.High24h = midPrice
			}
			if midPrice < priceData.Low24h && priceData.Low24h > 0 {
				priceData.Low24h = midPrice
			}
		}
	}

	// Call registered handlers
	oc.mu.RLock()
	defer oc.mu.RUnlock()
	
	for _, handler := range oc.handlers {
		if handler != nil {
			handler(priceData)
		}
	}
}

// handleHeartbeat processes heartbeat messages
func (oc *OANDAClient) handleHeartbeat(message *OANDAStreamMessage) {
	oc.mu.Lock()
	oc.lastPing = time.Now()
	oc.mu.Unlock()
	// Only log heartbeat in debug mode to reduce log spam
	// oc.logger.WithField("time", message.Time).Debug("Received OANDA heartbeat")
}

// monitorConnection monitors the connection health
func (oc *OANDAClient) monitorConnection(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-oc.done:
			return
		case <-ticker.C:
			// Check if we've received data recently
			if time.Since(oc.lastPing) > 90*time.Second {
				oc.logger.Warn("No heartbeat received for 90 seconds, connection may be stale")
				
				// Try to reconnect
				go oc.reconnect(ctx)
				return
			}
			
			// Log statistics
			now := time.Now()
			duration := now.Sub(oc.lastReset)
			if duration > time.Minute {
				messagesPerSecond := float64(oc.messageCount.Load()) / duration.Seconds()
				oc.logger.WithFields(logrus.Fields{
					"messages_per_second": messagesPerSecond,
					"total_messages":      oc.messageCount.Load(),
				}).Debug("OANDA client statistics")
				
				oc.messageCount.Store(0)
				oc.lastReset = now
			}
		}
	}
}

// reconnect attempts to reconnect to the OANDA stream
func (oc *OANDAClient) reconnect(ctx context.Context) {
	// Check if already reconnecting
	if !oc.reconnecting.CompareAndSwap(false, true) {
		oc.logger.Debug("Reconnection already in progress")
		return
	}
	defer oc.reconnecting.Store(false)
	
	oc.logger.Info("Attempting to reconnect to OANDA stream...")
	
	// Clean up existing resources
	if oc.streamResp != nil {
		oc.streamResp.Body.Close()
		oc.streamResp = nil
	}
	oc.connected.Store(false)
	
	// Reset done channel for new connection
	select {
	case <-oc.done:
		// Channel already closed, create new one
		oc.done = make(chan struct{})
	default:
		// Channel still open, close it and create new one
		close(oc.done)
		oc.done = make(chan struct{})
	}
	
	// Wait before reconnecting
	time.Sleep(oc.config.RetryDelay)
	
	// Attempt reconnection with exponential backoff
	maxAttempts := 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		oc.reconnectAttempts = attempt
		oc.logger.WithField("attempt", attempt).Info("Reconnecting to OANDA...")
		
		// Create new connection
		if err := oc.Connect(ctx); err != nil {
			oc.logger.WithError(err).WithField("attempt", attempt).Error("Reconnection failed")
			if attempt < maxAttempts {
				// Exponential backoff with jitter
				backoff := time.Duration(attempt*attempt) * oc.config.RetryDelay
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				time.Sleep(backoff)
			}
			continue
		}
		
		oc.logger.Info("Successfully reconnected to OANDA")
		oc.reconnectAttempts = 0
		return
	}
	
	oc.logger.Error("Failed to reconnect to OANDA after maximum attempts")
}

// SetPriceHandler sets a handler for price updates
func (oc *OANDAClient) SetPriceHandler(id string, handler MessageHandler) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.handlers[id] = handler
}

// RemovePriceHandler removes a price handler
func (oc *OANDAClient) RemovePriceHandler(id string) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	delete(oc.handlers, id)
}

// IsConnected returns whether the client is connected
func (oc *OANDAClient) IsConnected() bool {
	return oc.connected.Load()
}

// GetSymbols returns the symbols this client is subscribed to
func (oc *OANDAClient) GetSymbols() []string {
	return oc.symbols
}

// GetMessageCount returns the total message count
func (oc *OANDAClient) GetMessageCount() int64 {
	return oc.messageCount.Load()
}

// Disconnect closes the OANDA streaming connection
func (oc *OANDAClient) Disconnect() {
	oc.logger.Info("Disconnecting from OANDA stream...")
	
	close(oc.done)
	oc.connected.Store(false)
	
	if oc.streamResp != nil {
		oc.streamResp.Body.Close()
		oc.streamResp = nil
	}
	
	oc.logger.Info("Disconnected from OANDA stream")
}

// LoadSymbols loads symbols from OANDA and returns them in trade-back format
func (oc *OANDAClient) LoadSymbols(ctx context.Context) ([]models.SymbolInfo, error) {
	instruments, err := oc.restClient.GetInstruments(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load OANDA instruments: %w", err)
	}

	symbols := oc.restClient.ConvertToTradeBackSymbols(instruments)
	
	oc.logger.WithField("count", len(symbols)).Info("Loaded OANDA symbols")
	return symbols, nil
}

// GetCurrentPrices fetches current prices for the client's symbols
func (oc *OANDAClient) GetCurrentPrices(ctx context.Context) ([]*models.PriceData, error) {
	if len(oc.symbols) == 0 {
		return nil, fmt.Errorf("no symbols configured")
	}

	prices, err := oc.restClient.GetCurrentPrices(ctx, oc.symbols)
	if err != nil {
		return nil, fmt.Errorf("failed to get current prices: %w", err)
	}

	var priceDataList []*models.PriceData
	for _, price := range prices {
		if !price.Tradeable || price.Status != "tradeable" {
			continue
		}

		var bidPrice, askPrice float64
		if len(price.Bids) > 0 {
			bidPrice, _ = strconv.ParseFloat(price.Bids[0].Price, 64)
		}
		if len(price.Asks) > 0 {
			askPrice, _ = strconv.ParseFloat(price.Asks[0].Price, 64)
		}

		if bidPrice == 0 || askPrice == 0 {
			continue
		}

		midPrice := (bidPrice + askPrice) / 2.0

		priceData := &models.PriceData{
			Symbol:    price.Instrument,
			Exchange:  "oanda",
			Price:     midPrice,
			Bid:       bidPrice,
			Ask:       askPrice,
			Volume:    0,
			Timestamp: price.Time,
		}
		
		// Add 24hr statistics if available
		if oc.dailyStats != nil {
			if stats := oc.dailyStats.GetStats(price.Instrument); stats != nil {
				priceData.Open24h = stats.Open24h
				priceData.High24h = stats.High24h
				priceData.Low24h = stats.Low24h
				priceData.Change24h = stats.Change24h
				priceData.ChangePercent = stats.ChangePercent
				priceData.Volume = float64(stats.Volume24h)
				
				// Update high/low if current price exceeds them
				if midPrice > priceData.High24h {
					priceData.High24h = midPrice
				}
				if midPrice < priceData.Low24h && priceData.Low24h > 0 {
					priceData.Low24h = midPrice
				}
			}
		}

		priceDataList = append(priceDataList, priceData)
	}

	return priceDataList, nil
}