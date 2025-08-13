package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	apiHandlers "github.com/trade-back/internal/api/handlers"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/internal/indicator/enigma"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/internal/websocket"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// Server represents the HTTP API server
type Server struct {
	cfg        *config.Config
	logger     *logrus.Logger
	router     *mux.Router
	httpServer *http.Server
	
	// Dependencies
	influxDB      *database.InfluxClient
	mysqlDB       *database.MySQLClient
	redisCache    *cache.RedisClient
	natsClient    *messaging.NATSClient
	enigmaCalc    *enigma.Calculator
	wsManager     *websocket.BinaryManager
	historicalLoader *services.OptimizedHistoricalLoader
	instantEnigma    *services.InstantEnigmaService
	hub              *exchange.Hub
	extremeTracker   *services.UnifiedExtremeTracker
	
	// API handlers
	tradingViewAPI    *TradingViewAPI
	webhookHandler    *WebhookHandler
	historicalHandler *apiHandlers.HistoricalHandler
	enigmaHandler     *apiHandlers.EnigmaHandler
}

// NewServer creates a new API server
func NewServer(
	cfg *config.Config,
	logger *logrus.Logger,
	influxDB *database.InfluxClient,
	mysqlDB *database.MySQLClient,
	redisCache *cache.RedisClient,
	natsClient *messaging.NATSClient,
	enigmaCalc *enigma.Calculator,
	wsManager *websocket.BinaryManager,
	historicalLoader *services.OptimizedHistoricalLoader,
	instantEnigma *services.InstantEnigmaService,
	hub *exchange.Hub,
	extremeTracker *services.UnifiedExtremeTracker,
) *Server {
	s := &Server{
		cfg:        cfg,
		logger:     logger,
		influxDB:   influxDB,
		mysqlDB:    mysqlDB,
		redisCache: redisCache,
		natsClient: natsClient,
		enigmaCalc: enigmaCalc,
		wsManager:  wsManager,
		historicalLoader: historicalLoader,
		instantEnigma: instantEnigma,
		hub: hub,
		extremeTracker: extremeTracker,
	}
	
	if wsManager == nil {
		logger.Warn("WebSocket manager is nil in NewServer")
	}
	
	// Initialize API handlers
	s.tradingViewAPI = NewTradingViewAPI(influxDB, mysqlDB, redisCache, enigmaCalc, logger)
	s.webhookHandler = NewWebhookHandler(natsClient, &cfg.Webhook, logger)
	s.historicalHandler = apiHandlers.NewHistoricalHandler(influxDB, mysqlDB, natsClient, cfg, logger)
	s.enigmaHandler = apiHandlers.NewEnigmaHandler(enigmaCalc, extremeTracker, redisCache, logger)
	
	// Setup routes
	s.setupRoutes()
	
	return s
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	s.router = mux.NewRouter()
	
	// Apply middleware FIRST, before defining routes
	s.router.Use(s.loggingMiddleware)
	s.router.Use(s.recoveryMiddleware)
	
	// CORS middleware if enabled - MUST be before route definitions
	if s.cfg.Security.CORSEnabled {
		s.router.Use(s.corsMiddleware)
	}
	
	// API versioning
	apiV1 := s.router.PathPrefix("/api/v1").Subrouter()
	
	// Health check
	apiV1.HandleFunc("/health", s.handleHealth).Methods("GET")
	
	// Authentication endpoints
	apiV1.HandleFunc("/auth/login", s.handleLogin).Methods("POST")
	apiV1.HandleFunc("/auth/logout", s.handleLogout).Methods("POST")
	apiV1.HandleFunc("/auth/session", s.handleGetSession).Methods("GET")
	
	// WebSocket endpoint
	apiV1.HandleFunc("/ws", s.handleWebSocket).Methods("GET")
	
	// TradingView endpoints
	if s.cfg.Features.TradingViewEnabled {
		s.tradingViewAPI.RegisterRoutes(s.router)
	}
	
	// Webhook endpoints
	if s.cfg.Webhook.Enabled {
		s.webhookHandler.RegisterRoutes(s.router)
	}
	
	// Market data endpoints
	apiV1.HandleFunc("/symbols", s.handleGetSymbols).Methods("GET")
	apiV1.HandleFunc("/symbols/{symbol}", s.handleGetSymbol).Methods("GET")
	
	// Debug endpoint
	apiV1.HandleFunc("/debug/status", s.handleDebugStatus).Methods("GET")
	apiV1.HandleFunc("/symbols/{symbol}/price", s.handleGetPrice).Methods("GET")
	apiV1.HandleFunc("/symbols/{symbol}/bars", s.handleGetBars).Methods("GET")
	apiV1.HandleFunc("/symbols/{symbol}/enigma", s.handleGetEnigma).Methods("GET")
	
	// Market watch endpoints
	if s.cfg.Features.MarketWatchEnabled {
		apiV1.HandleFunc("/marketwatch", s.handleGetMarketWatch).Methods("GET")
		apiV1.HandleFunc("/marketwatch", s.handleAddToMarketWatch).Methods("POST")
		apiV1.HandleFunc("/marketwatch/{symbol}", s.handleRemoveFromMarketWatch).Methods("DELETE")
		
	}
	
	// Historical data endpoints
	s.historicalHandler.RegisterRoutes(s.router)
	
	// Enigma endpoints
	if s.enigmaHandler != nil {
		s.enigmaHandler.RegisterRoutes(s.router)
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Server.Host, s.cfg.Server.Port)
	
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  s.cfg.Server.ReadTimeout,
		WriteTimeout: s.cfg.Server.WriteTimeout,
		IdleTimeout:  s.cfg.Server.IdleTimeout,
	}
	
	s.logger.WithField("address", addr).Info("Starting HTTP server")
	
	err := s.httpServer.ListenAndServe()
	if err != nil {
		if strings.Contains(err.Error(), "address already in use") {
			return fmt.Errorf("port %d is already in use. Try: 1) Kill the process using it: lsof -ti:%d | xargs -r kill -9, or 2) Use a different port: --port 8081", s.cfg.Server.Port, s.cfg.Server.Port)
		}
		return err
	}
	return nil
}

// Stop gracefully stops the HTTP server
func (s *Server) Stop(ctx context.Context) error {
	// s.logger.Info("Stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}

// Middleware functions

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap ResponseWriter to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		
		next.ServeHTTP(wrapped, r)
		
		// Only log errors (4xx and 5xx status codes)
		if wrapped.statusCode >= 400 {
			s.logger.WithFields(logrus.Fields{
				"method":     r.Method,
				"path":       r.URL.Path,
				"status":     wrapped.statusCode,
				"duration":   time.Since(start),
				"remote":     r.RemoteAddr,
				"user_agent": r.UserAgent(),
			}).Error("HTTP request failed")
		}
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.logger.WithFields(logrus.Fields{
					"error": err,
					"path":  r.URL.Path,
				}).Error("Panic recovered")
				
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		
		next.ServeHTTP(w, r)
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return handlers.CORS(
		handlers.AllowedOrigins(s.cfg.Security.CORSOrigins),
		handlers.AllowedMethods(s.cfg.Security.CORSMethods),
		handlers.AllowedHeaders(s.cfg.Security.CORSHeaders),
		handlers.AllowCredentials(),
	)(next)
}

// Handler functions

// handleHealth checks the health status of all system components
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status": "healthy",
		"services": map[string]bool{
			"mysql":   s.mysqlDB != nil,
			"influx":  s.influxDB != nil,
			"redis":   s.redisCache != nil,
			"nats":    s.natsClient != nil && s.natsClient.IsConnected(),
		},
		"websocket_clients": s.wsManager.GetConnectionCount(),
		"timestamp":        time.Now().Unix(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// handleWebSocket establishes WebSocket connection for real-time data
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if s.wsManager == nil {
		s.logger.Error("WebSocket manager is nil")
		http.Error(w, "WebSocket service unavailable", http.StatusInternalServerError)
		return
	}
	s.wsManager.HandleWebSocket(w, r)
}

// handleGetSymbols retrieves list of trading symbols
func (s *Server) handleGetSymbols(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	
	
	// Check if MySQL connection exists
	if s.mysqlDB == nil {
		s.logger.Error("MySQL database connection is nil")
		http.Error(w, "Database connection not available", http.StatusInternalServerError)
		return
	}
	
	
	// Get all symbols from database
	symbols, err := s.mysqlDB.GetSymbols(ctx)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get symbols")
		http.Error(w, "Failed to retrieve symbols", http.StatusInternalServerError)
		return
	}
	
	// s.logger.WithField("count", len(symbols)).Info("Retrieved symbols from database")
	
	// Apply filters
	exchange := r.URL.Query().Get("exchange")
	instrumentType := r.URL.Query().Get("type")
	activeStr := r.URL.Query().Get("active")
	
	filteredSymbols := make([]*models.SymbolInfo, 0)
	for _, symbol := range symbols {
		// Filter by exchange
		if exchange != "" && symbol.Exchange != exchange {
			continue
		}
		
		// Filter by instrument type
		if instrumentType != "" && symbol.InstrumentType != instrumentType {
			continue
		}
		
		// Filter by active status
		if activeStr != "" {
			isActive := activeStr == "true"
			if symbol.IsActive != isActive {
				continue
			}
		}
		
		filteredSymbols = append(filteredSymbols, symbol)
	}
	
	// Return results
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"symbols": filteredSymbols,
		"count":   len(filteredSymbols),
	})
}

// handleDebugStatus returns debug information about server state
func (s *Server) handleDebugStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"mysql_connected": s.mysqlDB != nil,
		"influx_connected": s.influxDB != nil,
		"redis_connected": s.redisCache != nil,
		"nats_connected": s.natsClient != nil,
		"ws_manager_connected": s.wsManager != nil,
	}
	
	// Test MySQL connection if available
	if s.mysqlDB != nil {
		ctx := context.Background()
		if err := s.mysqlDB.Health(ctx); err != nil {
			status["mysql_health"] = "unhealthy: " + err.Error()
		} else {
			status["mysql_health"] = "healthy"
			// Try to get symbol count
			symbols, err := s.mysqlDB.GetSymbols(ctx)
			if err != nil {
				status["mysql_symbols"] = "error: " + err.Error()
			} else {
				status["mysql_symbols"] = len(symbols)
			}
		}
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleGetSymbol retrieves details for a specific symbol
func (s *Server) handleGetSymbol(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	ctx := r.Context()
	
	// Default to BINANCE exchange if not specified
	exchange := "BINANCE"
	
	// Fetch symbol info from database
	symbolInfo, err := s.mysqlDB.GetSymbol(ctx, exchange, symbol)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get symbol")
		http.Error(w, "Failed to retrieve symbol", http.StatusInternalServerError)
		return
	}
	
	if symbolInfo == nil {
		http.Error(w, "Symbol not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(symbolInfo)
}

// handleGetPrice retrieves current price for a symbol
func (s *Server) handleGetPrice(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	
	// Try to get from cache first
	price, err := s.redisCache.GetPrice(context.Background(), symbol)
	if err != nil || price == nil {
		http.Error(w, "Price not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(price)
}

// handleGetBars retrieves historical OHLCV data
func (s *Server) handleGetBars(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	
	resolution := r.URL.Query().Get("resolution")
	if resolution == "" {
		resolution = "1m"
	}
	
	// Convert frontend resolution format to backend format
	// Frontend sends: 1m, 5m, 15m, 1h, 1Dm, etc.
	// Backend expects: 1m, 5m, 15m, 1h, 1d, etc.
	if strings.HasSuffix(resolution, "Dm") {
		// Daily resolution from frontend (e.g., "1Dm" -> "1d")
		resolution = strings.TrimSuffix(resolution, "m")
		resolution = strings.ToLower(resolution)
	}
	
	// Get time range parameters
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	
	// Parse timestamps
	var from, to time.Time
	if fromStr != "" {
		fromUnix, err := strconv.ParseInt(fromStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid from timestamp", http.StatusBadRequest)
			return
		}
		from = time.Unix(fromUnix, 0)
	} else {
		// Default to 24 hours ago
		from = time.Now().Add(-24 * time.Hour)
	}
	
	if toStr != "" {
		toUnix, err := strconv.ParseInt(toStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid to timestamp", http.StatusBadRequest)
			return
		}
		to = time.Unix(toUnix, 0)
	} else {
		// Default to now
		to = time.Now()
	}
	
	// Swap if from > to
	if from.After(to) {
		from, to = to, from
	}
	
	// Log the request for debugging
	s.logger.WithFields(logrus.Fields{
		"symbol":     symbol,
		"resolution": resolution,
		"from":       from.Format(time.RFC3339),
		"to":         to.Format(time.RFC3339),
		"fromUnix":   from.Unix(),
		"toUnix":     to.Unix(),
	})
	
	// Try cache first
	ctx := context.Background()
	bars, err := s.redisCache.GetBars(ctx, symbol, resolution)
	if err == nil && bars != nil && len(bars) > 0 {
		// Filter bars by time range
		filteredBars := make([]*models.Bar, 0)
		for _, bar := range bars {
			if bar.Timestamp.Unix() >= from.Unix() && bar.Timestamp.Unix() <= to.Unix() {
				filteredBars = append(filteredBars, bar)
			}
		}
		if len(filteredBars) > 0 {
			// Convert bars to TradingView format
			tvBars := make([]map[string]interface{}, len(filteredBars))
			for i, bar := range filteredBars {
				tvBars[i] = map[string]interface{}{
					"time":   bar.Timestamp.Unix() * 1000, // TradingView expects milliseconds
					"open":   bar.Open,
					"high":   bar.High,
					"low":    bar.Low,
					"close":  bar.Close,
					"volume": bar.Volume,
				}
			}
			
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"bars": tvBars,
			})
			return
		}
	}
	
	// Fetch from InfluxDB
	if s.influxDB != nil {
		s.logger.WithFields(logrus.Fields{
			"symbol":     symbol,
			"resolution": resolution,
			"from":       from.Format(time.RFC3339),
			"to":         to.Format(time.RFC3339),
		})
		
		bars, err := s.influxDB.GetBars(ctx, symbol, from, to, resolution)
		if err != nil {
			s.logger.WithError(err).Error("Failed to get bars from InfluxDB")
			http.Error(w, "Failed to retrieve historical data", http.StatusInternalServerError)
			return
		}
		
		s.logger.WithFields(logrus.Fields{
			"symbol":     symbol,
			"resolution": resolution,
			"bar_count":  len(bars),
		})
		
		// Cache the results for future requests
		if len(bars) > 0 {
			go func() {
				if err := s.redisCache.SetBars(context.Background(), symbol, resolution, bars); err != nil {
					s.logger.WithError(err).Warn("Failed to cache bars")
				}
			}()
		}
		
		// Convert bars to TradingView format
		tvBars := make([]map[string]interface{}, len(bars))
		for i, bar := range bars {
			tvBars[i] = map[string]interface{}{
				"time":   bar.Timestamp.Unix() * 1000, // TradingView expects milliseconds
				"open":   bar.Open,
				"high":   bar.High,
				"low":    bar.Low,
				"close":  bar.Close,
				"volume": bar.Volume,
			}
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"bars": tvBars,
		})
		return
	} else {
		s.logger.Error("InfluxDB client is nil")
	}
	
	// No data available
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"bars": []interface{}{},
	})
}

// handleGetEnigma retrieves Enigma indicator values
func (s *Server) handleGetEnigma(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	
	enigma, err := s.enigmaCalc.GetEnigmaLevel(symbol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(enigma)
}

// MarketWatchSymbol represents symbol data with price information
type MarketWatchSymbol struct {
	Symbol         string                `json:"symbol"`
	Price          float64               `json:"price"`
	Bid            float64               `json:"bid"`
	BidSize        float64               `json:"bidSize"`
	Ask            float64               `json:"ask"`
	AskSize        float64               `json:"askSize"`
	High           float64               `json:"high"`
	Low            float64               `json:"low"`
	Volume         float64               `json:"volume"`
	Change         float64               `json:"change"`
	ChangePercent  float64               `json:"changePercent"`
	Timestamp      int64                 `json:"timestamp"`
	SyncStatus     string                `json:"sync_status,omitempty"`
	SyncProgress   int                   `json:"sync_progress,omitempty"`
	TotalBars      int                   `json:"total_bars,omitempty"`
	Enigma         *models.EnigmaData    `json:"enigma,omitempty"`
}

// syncRedisMarketWatch ensures Redis is in sync with MySQL for a session
func (s *Server) syncRedisMarketWatch(ctx context.Context, token string) error {
	// Get symbols from MySQL (source of truth)
	mysqlSymbols, err := s.mysqlDB.GetMarketWatchSymbols(ctx, token)
	if err != nil {
		return fmt.Errorf("failed to get MySQL symbols: %w", err)
	}
	
	// Get current Redis symbols
	redisSymbols, err := s.redisCache.GetMarketWatch(ctx, token)
	if err != nil && err.Error() != "redis: nil" {
		s.logger.WithError(err).Warn("Failed to get Redis symbols, will rebuild")
	}
	
	// Convert to maps for easy comparison
	mysqlMap := make(map[string]bool)
	for _, symbol := range mysqlSymbols {
		mysqlMap[symbol] = true
	}
	
	redisMap := make(map[string]bool)
	for _, symbol := range redisSymbols {
		redisMap[symbol] = true
	}
	
	// Add missing symbols to Redis
	for symbol := range mysqlMap {
		if !redisMap[symbol] {
			if err := s.redisCache.AddToMarketWatch(ctx, token, symbol); err != nil {
				s.logger.WithError(err).WithField("symbol", symbol).Error("Failed to add missing symbol to Redis")
			} else {
				s.logger.WithField("symbol", symbol).Debug("Added missing symbol to Redis")
			}
		}
	}
	
	// Remove extra symbols from Redis
	for symbol := range redisMap {
		if !mysqlMap[symbol] {
			if err := s.redisCache.RemoveFromMarketWatch(ctx, token, symbol); err != nil {
				s.logger.WithError(err).WithField("symbol", symbol).Error("Failed to remove extra symbol from Redis")
			} else {
				s.logger.WithField("symbol", symbol).Debug("Removed extra symbol from Redis")
			}
		}
	}
	
	return nil
}

// handleGetMarketWatch retrieves user's watchlist with price data
func (s *Server) handleGetMarketWatch(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Session-Token")
	if token == "" {
		http.Error(w, "Session token required", http.StatusUnauthorized)
		return
	}
	
	// Ensure Redis is in sync with MySQL
	ctx := context.Background()
	if err := s.syncRedisMarketWatch(ctx, token); err != nil {
		s.logger.WithError(err).Warn("Failed to sync Redis with MySQL")
	}
	
	// Get symbols with sync status from MySQL
	symbolsWithStatus, err := s.mysqlDB.GetMarketWatchWithSyncStatus(context.Background(), token)
	if err != nil {
		s.logger.WithError(err).WithField("token", token).Error("Failed to get market watch from MySQL")
		http.Error(w, "Failed to get market watch", http.StatusInternalServerError)
		return
	}
	
	// Extract just symbol names
	symbols := make([]string, 0, len(symbolsWithStatus))
	syncStatusMap := make(map[string]map[string]interface{})
	for _, item := range symbolsWithStatus {
		symbol := item["symbol"].(string)
		symbols = append(symbols, symbol)
		syncStatusMap[symbol] = item
	}
	
	// Check if client wants detailed data
	includeDetails := r.URL.Query().Get("detailed") == "true"
	
	// s.logger.WithFields(logrus.Fields{
	// 	"includeDetails": includeDetails,
	// 	"symbolCount": len(symbols),
	// 	"token": token,
	// }).Info("Processing market watch request")
	
	if !includeDetails || len(symbols) == 0 {
		// Return simple response for backward compatibility with sync status
		var simpleSymbols []interface{}
		for _, item := range symbolsWithStatus {
			simpleSymbols = append(simpleSymbols, map[string]interface{}{
				"symbol": item["symbol"],
				"sync_status": item["sync_status"],
				"sync_progress": item["sync_progress"],
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"symbols": simpleSymbols,
			"count":   len(simpleSymbols),
		})
		return
	}
	
	// Fetch price data for all symbols
	marketData := s.fetchMarketData(r.Context(), symbols)
	
	// Add sync status to market data
	for i := range marketData {
		if status, exists := syncStatusMap[marketData[i].Symbol]; exists {
			// Add sync fields to market data (with safe type assertions)
			if syncStatus, ok := status["sync_status"].(string); ok {
				marketData[i].SyncStatus = syncStatus
			}
			if syncProgress, ok := status["sync_progress"].(int); ok {
				marketData[i].SyncProgress = syncProgress
			}
			if totalBars, ok := status["total_bars"].(int); ok {
				marketData[i].TotalBars = totalBars
			}
		}
	}
	
	// Log the result for debugging
	s.logger.WithFields(logrus.Fields{
		"symbols_requested": symbols,
		"market_data_count": len(marketData),
	})
	
	response := map[string]interface{}{
		"symbols": marketData,
		"count":   len(marketData),
		"timestamp": time.Now().Unix(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.WithError(err).Error("Failed to encode response")
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// fetchMarketData fetches current market data for symbols
func (s *Server) fetchMarketData(ctx context.Context, symbols []string) []MarketWatchSymbol {
	if len(symbols) == 0 {
		return []MarketWatchSymbol{}
	}
	
	// s.logger.WithField("symbols", symbols).Info("Fetching market data")
	
	// Get live prices from Redis cache (populated by WebSocket subscriptions)
	prices, err := s.redisCache.GetPrices(ctx, symbols)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get prices from cache")
	}
	
	// Build response
	result := make([]MarketWatchSymbol, 0, len(symbols))
	for _, symbol := range symbols {
		marketSymbol := MarketWatchSymbol{
			Symbol:    symbol,
			Timestamp: time.Now().Unix(),
		}
		
		// Use live price data from WebSocket subscriptions (stored in Redis)
		if priceData, exists := prices[symbol]; exists && priceData != nil {
			marketSymbol.Price = priceData.Price
			marketSymbol.Bid = priceData.Bid
			marketSymbol.Ask = priceData.Ask
			marketSymbol.Volume = priceData.Volume
			marketSymbol.High = priceData.High24h
			marketSymbol.Low = priceData.Low24h
			marketSymbol.Change = priceData.Change24h
			marketSymbol.ChangePercent = priceData.ChangePercent
			
			// Try to get Enigma data
			if s.instantEnigma != nil && priceData.Price > 0 {
				enigma, err := s.instantEnigma.GetInstantEnigma(ctx, symbol, priceData.Price)
				if err != nil {
					// s.logger.WithError(err).WithField("symbol", symbol).Debug("Failed to get Enigma data")
				} else {
					marketSymbol.Enigma = enigma
				}
			}
		} else {
			// Symbol not in cache means it's not being subscribed to via WebSocket
			// This is expected behavior - only market watch symbols have live prices
			// s.logger.WithField("symbol", symbol).Debug("No live price data (symbol not in active subscriptions)")
		}
		
		result = append(result, marketSymbol)
	}
	
	return result
}

// Removed fetchBinanceAllTickers and BinanceTicker24hr - now using live WebSocket data from Redis cache
// This ensures we only fetch data for symbols in users' market watch lists, improving performance
// and reducing unnecessary API calls to Binance

// handleAddToMarketWatch adds symbol to watchlist
func (s *Server) handleAddToMarketWatch(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Session-Token")
	if token == "" {
		http.Error(w, "Session token required", http.StatusUnauthorized)
		return
	}
	
	var req struct {
		Symbol string `json:"symbol"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	
	if req.Symbol == "" {
		http.Error(w, "Symbol is required", http.StatusBadRequest)
		return
	}
	
	// Check max symbols limit
	existing, _ := s.redisCache.GetMarketWatch(context.Background(), token)
	if len(existing) >= s.cfg.Features.MaxWatchlistSymbols {
		http.Error(w, "Maximum watchlist symbols reached", http.StatusBadRequest)
		return
	}
	
	ctx := context.Background()
	
	// Add to MySQL first (source of truth)
	if err := s.mysqlDB.AddToMarketWatch(ctx, token, req.Symbol); err != nil {
		s.logger.WithError(err).WithField("symbol", req.Symbol).Error("Failed to add symbol to MySQL")
		http.Error(w, "Failed to add symbol", http.StatusInternalServerError)
		return
	}
	
	// Then add to Redis cache (critical for real-time updates)
	if err := s.redisCache.AddToMarketWatch(ctx, token, req.Symbol); err != nil {
		s.logger.WithError(err).WithFields(logrus.Fields{
			"symbol": req.Symbol,
			"token": token,
		}).Error("Failed to add symbol to Redis - data inconsistency warning!")
		
		// Try to recover by re-adding
		go func() {
			time.Sleep(1 * time.Second)
			if err := s.redisCache.AddToMarketWatch(context.Background(), token, req.Symbol); err != nil {
				s.logger.WithError(err).Error("Failed to recover Redis sync")
			} else {
				s.logger.Info("Successfully recovered Redis sync")
			}
		}()
	}
	
	// Notify all WebSocket clients with the same session token to auto-subscribe
	s.wsManager.AutoSubscribeSymbol(token, req.Symbol)
	
	// Add symbol to the hub for live price streaming
	if s.hub != nil {
		if err := s.hub.AddSymbol(req.Symbol); err != nil {
			s.logger.WithError(err).WithField("symbol", req.Symbol).Error("Failed to add symbol to hub")
		}
	}
	
	// Get instant Enigma and send via WebSocket
	if s.instantEnigma != nil {
		go func() {
			ctx := context.Background()
			
			// Get current price
			price, err := s.redisCache.GetPrice(ctx, req.Symbol)
			if err != nil || price == nil {
				// s.logger.WithError(err).WithField("symbol", req.Symbol).Warn("Failed to get current price for Enigma")
				return
			}
			
			// Calculate instant Enigma
			_, err = s.instantEnigma.GetInstantEnigma(ctx, req.Symbol, price.Price)
			if err != nil {
				// s.logger.WithError(err).WithField("symbol", req.Symbol).Warn("Failed to calculate instant Enigma")
			} else {
				// Enigma is automatically sent via NATS/WebSocket by the service
				// s.logger.WithField("symbol", req.Symbol).Info("Instant Enigma calculated and sent")
			}
		}()
	}
	
	// Start background historical sync
	if s.historicalLoader != nil {
		go func() {
			ctx := context.Background()
			
			// Update sync status to pending
			if err := s.mysqlDB.UpdateSyncStatus(ctx, req.Symbol, "pending", 0, 0, ""); err != nil {
				s.logger.WithError(err).Error("Failed to update sync status")
			}
			
			// Start sync with 1000 days of 1m data
			// s.logger.WithField("symbol", req.Symbol).Info("Starting background historical sync")
			
			// Retry mechanism for failed syncs
			maxRetries := 3
			retryDelay := 5 * time.Second
			
			for attempt := 1; attempt <= maxRetries; attempt++ {
				if err := s.mysqlDB.UpdateSyncStatus(ctx, req.Symbol, "syncing", 0, 0, ""); err != nil {
					s.logger.WithError(err).Error("Failed to update sync status to syncing")
				}
				
				// Pass session token in context for progress updates
				ctxWithSession := context.WithValue(ctx, "session_token", token)
				err := s.historicalLoader.LoadHistoricalDataIncremental(ctxWithSession, req.Symbol, "1m", 1000)
				if err == nil {
					// Success - mark as completed
					if err := s.mysqlDB.UpdateSyncStatus(ctx, req.Symbol, "completed", 100, 1000*1440, ""); err != nil {
						s.logger.WithError(err).Error("Failed to update sync completion")
					}
					// s.logger.WithField("symbol", req.Symbol).Info("Historical sync completed")
					return
				}
				
				// Error occurred
				s.logger.WithError(err).WithFields(logrus.Fields{
					"symbol":  req.Symbol,
					"attempt": attempt,
					"max":     maxRetries,
				}).Error("Failed to sync historical data")
				
				if attempt < maxRetries {
					// Not the last attempt, wait and retry
					s.logger.WithFields(logrus.Fields{
						"symbol": req.Symbol,
						"delay":  retryDelay,
						"attempt": attempt + 1,
					}).Info("Retrying historical sync")
					time.Sleep(retryDelay)
					retryDelay *= 2 // Exponential backoff
				} else {
					// Final attempt failed
					s.mysqlDB.UpdateSyncStatus(ctx, req.Symbol, "failed", 0, 0, "Sync failed after multiple attempts")
					if s.natsClient != nil {
						s.natsClient.PublishSyncError(req.Symbol, fmt.Sprintf("Sync failed after %d attempts: %v", maxRetries, err))
					}
				}
			}
		}()
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"symbol": req.Symbol,
	})
}

// handleRemoveFromMarketWatch removes symbol from watchlist
func (s *Server) handleRemoveFromMarketWatch(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Session-Token")
	if token == "" {
		http.Error(w, "Session token required", http.StatusUnauthorized)
		return
	}
	
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	
	ctx := context.Background()
	
	// Remove from MySQL first (source of truth)
	if err := s.mysqlDB.RemoveFromMarketWatch(ctx, token, symbol); err != nil {
		s.logger.WithError(err).WithField("symbol", symbol).Error("Failed to remove symbol from MySQL")
		http.Error(w, "Failed to remove symbol", http.StatusInternalServerError)
		return
	}
	
	// Then remove from Redis cache (critical for real-time updates)
	if err := s.redisCache.RemoveFromMarketWatch(ctx, token, symbol); err != nil {
		s.logger.WithError(err).WithFields(logrus.Fields{
			"symbol": symbol,
			"token": token,
		}).Error("Failed to remove symbol from Redis - data inconsistency warning!")
		
		// Try to recover by re-removing
		go func() {
			time.Sleep(1 * time.Second)
			if err := s.redisCache.RemoveFromMarketWatch(context.Background(), token, symbol); err != nil {
				s.logger.WithError(err).Error("Failed to recover Redis sync")
			} else {
				s.logger.Info("Successfully recovered Redis sync")
			}
		}()
	}
	
	// Notify all WebSocket clients with the same session token to auto-unsubscribe
	s.logger.WithFields(logrus.Fields{
		"token": token,
		"symbol": symbol,
		"wsManager": s.wsManager != nil,
	}).Debug("Calling AutoUnsubscribeSymbol")
	
	if s.wsManager != nil {
		s.wsManager.AutoUnsubscribeSymbol(token, symbol)
		s.logger.WithFields(logrus.Fields{
			"token": token,
			"symbol": symbol,
		}).Debug("Called AutoUnsubscribeSymbol successfully")
	} else {
		s.logger.Error("WebSocket manager is nil, cannot send auto-unsubscribe")
	}
	
	// Remove symbol from hub if no other users are watching it
	if s.hub != nil {
		// Check if any other users have this symbol
		// For now, we'll keep the symbol in hub as other users might need it
		// TODO: Implement reference counting for symbols
	}
	
	// Send symbol removal notification
	if s.wsManager != nil {
		s.wsManager.SendSymbolRemoved(token, symbol)
		s.logger.WithFields(logrus.Fields{
			"token": token,
			"symbol": symbol,
		}).Debug("Sent symbol removal notification")
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"symbol": symbol,
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Hijack implements the http.Hijacker interface to support WebSocket upgrades
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, fmt.Errorf("ResponseWriter does not implement http.Hijacker")
}