package exchange

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	binance "github.com/binance/binance-connector-go"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// MessageHandler is a function that handles incoming messages
type MessageHandler func(*models.PriceData)

// BinanceClient handles WebSocket connection to Binance using official library
type BinanceClient struct {
	id          string
	client      *binance.WebsocketStreamClient
	symbols     []string
	handlers    map[string]MessageHandler
	logger      *logrus.Entry
	
	// Connection state
	connected   atomic.Bool
	lastPing    time.Time
	
	// Channels for control
	done        chan struct{}
	stopCh      chan struct{}
	stopChannels []chan struct{} // For multiple streams
	
	// Mutex
	mu          sync.RWMutex
	
	// Configuration
	config      *config.ExchangeConfig
	
	// Metrics
	messageCount atomic.Int64
	lastReset    time.Time
	highFreqMode bool
}

// NewBinanceClient creates a new Binance client using official library
func NewBinanceClient(id string, symbols []string, config *config.ExchangeConfig, logger *logrus.Logger) *BinanceClient {
	return &BinanceClient{
		id:           id,
		symbols:      symbols,
		handlers:     make(map[string]MessageHandler),
		logger:       logger.WithFields(logrus.Fields{"component": "binance", "client_id": id}),
		done:         make(chan struct{}),
		config:       config,
		stopChannels: make([]chan struct{}, 0),
		lastReset:    time.Now(),
		highFreqMode: true, // Enable high-frequency mode by default
	}
}

// Connect establishes connection to Binance WebSocket using official library
func (bc *BinanceClient) Connect(ctx context.Context) error {
	if bc.highFreqMode {
		return bc.ConnectHighFreq(ctx)
	}
	
	// bc.logger.Info("Connecting to Binance WebSocket using official library...")
	
	// Create WebSocket client with combined=true for multiple symbols
	bc.client = binance.NewWebsocketStreamClient(true) // true = combined stream for multiple symbols
	
	// Create handlers
	tickerHandler := bc.createTickerHandler()
	errorHandler := bc.createErrorHandler()
	
	// Start WebSocket for multiple symbols (combined stream)
	doneCh, stopCh, err := bc.client.WsCombinedMarketTickersStatServe(bc.symbols, tickerHandler, errorHandler)
	if err != nil {
		return fmt.Errorf("failed to start WebSocket stream: %w", err)
	}
	
	bc.stopCh = stopCh
	bc.connected.Store(true)
	bc.lastPing = time.Now()
	
	// bc.logger.WithField("symbols", len(bc.symbols)).Info("Connected to Binance WebSocket successfully")
	
	// Start monitoring goroutine
	go bc.monitorConnection(ctx, doneCh)
	
	return nil
}

// ConnectHighFreq establishes high-frequency connections
func (bc *BinanceClient) ConnectHighFreq(ctx context.Context) error {
	// bc.logger.Info("Connecting to Binance high-frequency streams...")
	
	// Create WebSocket client with combined=true for multiple symbols
	bc.client = binance.NewWebsocketStreamClient(true)
	
	// Reset metrics
	bc.messageCount.Store(0)
	bc.lastReset = time.Now()
	
	// Start multiple streams for maximum data throughput
	streamErrors := make(chan error, 3)
	
	// 1. Trade stream - real-time individual trades (highest frequency)
	go func() {
		if err := bc.startTradeStream(ctx); err != nil {
			streamErrors <- fmt.Errorf("trade stream failed: %w", err)
		}
	}()
	
	// 2. Aggregated trade stream - aggregated trades  
	go func() {
		if err := bc.startAggTradeStream(ctx); err != nil {
			streamErrors <- fmt.Errorf("agg trade stream failed: %w", err)
		}
	}()
	
	// 3. Book ticker stream - best bid/ask updates
	go func() {
		if err := bc.startBookTickerStream(ctx); err != nil {
			streamErrors <- fmt.Errorf("book ticker stream failed: %w", err)
		}
	}()
	
	// Check for immediate errors
	select {
	case err := <-streamErrors:
		return err
	case <-time.After(2 * time.Second):
		// All streams started successfully
		bc.connected.Store(true)
		bc.lastPing = time.Now()
		// bc.logger.Info("Connected to Binance high-frequency streams successfully")
		
		// Start metrics reporter
		go bc.reportMetrics(ctx)
		
		return nil
	}
}

// createTickerHandler creates the ticker event handler
func (bc *BinanceClient) createTickerHandler() binance.WsMarketTickersStatHandler {
	return func(event *binance.WsMarketTickerStatEvent) {
		// Convert Binance event to our price data format
		priceData := bc.convertEventToPriceData(event)
		if priceData == nil {
			return
		}
		
		// Call registered handlers
		bc.mu.RLock()
		for pattern, handler := range bc.handlers {
			if pattern == "all" || pattern == event.Symbol {
				handler(priceData)
			}
		}
		bc.mu.RUnlock()
	}
}

// createErrorHandler creates the error handler
func (bc *BinanceClient) createErrorHandler() binance.ErrHandler {
	return func(err error) {
		bc.logger.WithError(err).Error("WebSocket error occurred")
		bc.connected.Store(false)
	}
}

// convertEventToPriceData converts Binance ticker event to our price data format
func (bc *BinanceClient) convertEventToPriceData(event *binance.WsMarketTickerStatEvent) *models.PriceData {
	price, err := strconv.ParseFloat(event.LastPrice, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("price", event.LastPrice).Error("Failed to parse price")
		return nil
	}
	
	bid, err := strconv.ParseFloat(event.BidPrice, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("bid", event.BidPrice).Error("Failed to parse bid price")
		bid = 0
	}
	
	ask, err := strconv.ParseFloat(event.AskPrice, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("ask", event.AskPrice).Error("Failed to parse ask price")
		ask = 0
	}
	
	volume, err := strconv.ParseFloat(event.BaseVolume, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("volume", event.BaseVolume).Error("Failed to parse volume")
		volume = 0
	}
	
	// Parse 24hr change data
	change24h, err := strconv.ParseFloat(event.PriceChange, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("change", event.PriceChange).Error("Failed to parse 24h change")
		change24h = 0
	}
	
	changePercent, err := strconv.ParseFloat(event.PriceChangePercent, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("changePercent", event.PriceChangePercent).Error("Failed to parse change percent")
		changePercent = 0
	}
	
	open24h, err := strconv.ParseFloat(event.OpenPrice, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("open", event.OpenPrice).Error("Failed to parse open price")
		open24h = 0
	}
	
	high24h, err := strconv.ParseFloat(event.HighPrice, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("high", event.HighPrice).Error("Failed to parse high price")
		high24h = 0
	}
	
	low24h, err := strconv.ParseFloat(event.LowPrice, 64)
	if err != nil {
		bc.logger.WithError(err).WithField("low", event.LowPrice).Error("Failed to parse low price")
		low24h = 0
	}
	
	priceData := &models.PriceData{
		Symbol:        event.Symbol,
		Price:         price,
		Bid:           bid,
		Ask:           ask,
		Volume:        volume,
		Change24h:     change24h,
		ChangePercent: changePercent,
		Open24h:       open24h,
		High24h:       high24h,
		Low24h:        low24h,
		Timestamp:     time.Unix(event.Time/1000, (event.Time%1000)*1e6),
		Sequence:      uint64(event.LastID), // Use LastID as sequence for gap detection
	}
	
	// DEBUG: Log 24hr change data
	if changePercent != 0 {
		bc.logger.WithFields(map[string]interface{}{
			"symbol": event.Symbol,
			"changePercent": changePercent,
			"change24h": change24h,
			"price": price,
			"open24h": open24h,
		}).Info("24hr change data from Binance")
	}
	
	return priceData
}

// RegisterHandler registers a message handler
func (bc *BinanceClient) RegisterHandler(pattern string, handler MessageHandler) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.handlers[pattern] = handler
}

// IsConnected returns the connection status
func (bc *BinanceClient) IsConnected() bool {
	return bc.connected.Load()
}

// GetSymbols returns the list of symbols this client handles
func (bc *BinanceClient) GetSymbols() []string {
	return bc.symbols
}

// monitorConnection monitors the connection and handles cleanup
func (bc *BinanceClient) monitorConnection(ctx context.Context, doneCh chan struct{}) {
	defer func() {
		bc.connected.Store(false)
	}()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-bc.done:
			return
		case <-doneCh:
			bc.logger.Warn("WebSocket connection closed by server")
			bc.connected.Store(false)
			return
		}
	}
}

// startTradeStream starts real-time trade stream
func (bc *BinanceClient) startTradeStream(ctx context.Context) error {
	// Trade handler for combined stream
	tradeHandler := func(event *binance.WsCombinedTradeEvent) {
		bc.messageCount.Add(1)
		
		// Convert to price data
		price, _ := strconv.ParseFloat(event.Data.Price, 64)
		quantity, _ := strconv.ParseFloat(event.Data.Quantity, 64)
		
		priceData := &models.PriceData{
			Symbol:    event.Data.Symbol,
			Price:     price,
			Volume:    quantity,
			Timestamp: time.Unix(event.Data.Time/1000, (event.Data.Time%1000)*1e6),
			Sequence:  uint64(event.Data.TradeID),
		}
		
		// Set bid/ask based on buyer maker flag
		if event.Data.IsBuyerMaker {
			priceData.Ask = price
			priceData.Bid = price - 0.01 // Approximate
		} else {
			priceData.Bid = price
			priceData.Ask = price + 0.01 // Approximate
		}
		
		bc.handlePriceUpdate(priceData)
	}
	
	// Error handler
	errHandler := func(err error) {
		bc.logger.WithError(err).Error("Trade stream error")
	}
	
	// Start trade stream
	doneCh, stopCh, err := bc.client.WsCombinedTradeServe(bc.symbols, tradeHandler, errHandler)
	if err != nil {
		return fmt.Errorf("failed to start trade stream: %w", err)
	}
	
	bc.stopChannels = append(bc.stopChannels, stopCh)
	
	// Monitor connection
	go bc.monitorStream(ctx, doneCh, "trade")
	
	return nil
}

// startAggTradeStream starts aggregated trade stream
func (bc *BinanceClient) startAggTradeStream(ctx context.Context) error {
	// Aggregated trade handler
	aggTradeHandler := func(event *binance.WsAggTradeEvent) {
		bc.messageCount.Add(1)
		
		// Convert to price data
		price, _ := strconv.ParseFloat(event.Price, 64)
		quantity, _ := strconv.ParseFloat(event.Quantity, 64)
		
		priceData := &models.PriceData{
			Symbol:    event.Symbol,
			Price:     price,
			Volume:    quantity,
			Timestamp: time.Unix(event.Time/1000, (event.Time%1000)*1e6),
			Sequence:  uint64(event.LastBreakdownTradeID),
		}
		
		// For aggregated trades, we don't have bid/ask
		priceData.Bid = price - 0.01
		priceData.Ask = price + 0.01
		
		bc.handlePriceUpdate(priceData)
	}
	
	// Error handler
	errHandler := func(err error) {
		bc.logger.WithError(err).Error("Aggregated trade stream error")
	}
	
	// Start aggregated trade stream
	doneCh, stopCh, err := bc.client.WsCombinedAggTradeServe(bc.symbols, aggTradeHandler, errHandler)
	if err != nil {
		return fmt.Errorf("failed to start aggregated trade stream: %w", err)
	}
	
	bc.stopChannels = append(bc.stopChannels, stopCh)
	
	// Monitor connection
	go bc.monitorStream(ctx, doneCh, "aggTrade")
	
	return nil
}

// startBookTickerStream starts best bid/ask stream
func (bc *BinanceClient) startBookTickerStream(ctx context.Context) error {
	// Book ticker handler
	bookTickerHandler := func(event *binance.WsBookTickerEvent) {
		bc.messageCount.Add(1)
		
		// Convert to price data
		bid, _ := strconv.ParseFloat(event.BestBidPrice, 64)
		ask, _ := strconv.ParseFloat(event.BestAskPrice, 64)
		bidQty, _ := strconv.ParseFloat(event.BestBidQty, 64)
		askQty, _ := strconv.ParseFloat(event.BestAskQty, 64)
		
		priceData := &models.PriceData{
			Symbol:    event.Symbol,
			Price:     (bid + ask) / 2, // Mid price
			Bid:       bid,
			Ask:       ask,
			Volume:    (bidQty + askQty) / 2, // Average of bid/ask quantity
			Timestamp: time.Now(), // Book ticker doesn't have timestamp
			Sequence:  uint64(event.UpdateID),
		}
		
		bc.handlePriceUpdate(priceData)
	}
	
	// Error handler
	errHandler := func(err error) {
		bc.logger.WithError(err).Error("Book ticker stream error")
	}
	
	// Start book ticker stream
	doneCh, stopCh, err := bc.client.WsCombinedBookTickerServe(bc.symbols, bookTickerHandler, errHandler)
	if err != nil {
		return fmt.Errorf("failed to start book ticker stream: %w", err)
	}
	
	bc.stopChannels = append(bc.stopChannels, stopCh)
	
	// Monitor connection
	go bc.monitorStream(ctx, doneCh, "bookTicker")
	
	return nil
}

// handlePriceUpdate processes price updates from any stream
func (bc *BinanceClient) handlePriceUpdate(price *models.PriceData) {
	// Call registered handlers
	bc.mu.RLock()
	for pattern, handler := range bc.handlers {
		if pattern == "all" || pattern == price.Symbol {
			handler(price)
		}
	}
	bc.mu.RUnlock()
}

// monitorStream monitors individual stream health
func (bc *BinanceClient) monitorStream(ctx context.Context, doneCh chan struct{}, streamType string) {
	select {
	case <-ctx.Done():
	case <-bc.done:
	case <-doneCh:
		bc.logger.WithField("stream", streamType).Warn("Stream closed by server")
		// TODO: Implement reconnection logic
	}
}

// reportMetrics reports message throughput metrics
func (bc *BinanceClient) reportMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-bc.done:
			return
		case <-ticker.C:
			// Reset counters
			bc.messageCount.Store(0)
			bc.lastReset = time.Now()
		}
	}
}

// GetMessageRate returns current message rate per second
func (bc *BinanceClient) GetMessageRate() float64 {
	count := bc.messageCount.Load()
	elapsed := time.Since(bc.lastReset).Seconds()
	if elapsed > 0 {
		return float64(count) / elapsed
	}
	return 0
}

// Close closes the WebSocket connection
func (bc *BinanceClient) Close() error {
	// bc.logger.Info("Closing Binance WebSocket connection...")
	
	bc.connected.Store(false)
	close(bc.done)
	
	// Signal all streams to stop
	if bc.stopCh != nil {
		select {
		case bc.stopCh <- struct{}{}:
		default:
			// Channel might be closed already
		}
	}
	
	// Stop all high-frequency streams
	for _, stopCh := range bc.stopChannels {
		select {
		case stopCh <- struct{}{}:
		default:
			// Channel might be closed already
		}
	}
	
	// bc.logger.Info("Binance WebSocket connection closed")
	return nil
}

// GetStats returns connection statistics
func (bc *BinanceClient) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"client_id":    bc.id,
		"connected":    bc.IsConnected(),
		"symbols":      len(bc.symbols),
		"last_ping":    bc.lastPing.Unix(),
		"library":      "official-binance-connector",
	}
	
	if bc.highFreqMode {
		stats["high_freq_mode"] = true
		stats["message_rate"] = bc.GetMessageRate()
		stats["active_streams"] = len(bc.stopChannels)
	}
	
	return stats
}