package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	apiHandlers "github.com/trade-back/internal/api/handlers"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/indicator/enigma"
	"github.com/trade-back/internal/messaging"
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
	
	// API handlers
	tradingViewAPI    *TradingViewAPI
	webhookHandler    *WebhookHandler
	historicalHandler *apiHandlers.HistoricalHandler
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
	}
	
	if wsManager == nil {
		logger.Warn("WebSocket manager is nil in NewServer")
	}
	
	// Initialize API handlers
	s.tradingViewAPI = NewTradingViewAPI(influxDB, mysqlDB, redisCache, enigmaCalc, logger)
	s.webhookHandler = NewWebhookHandler(natsClient, &cfg.Webhook, logger)
	s.historicalHandler = apiHandlers.NewHistoricalHandler(influxDB, mysqlDB, logger)
	
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
	s.logger.Info("Stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}

// Middleware functions

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap ResponseWriter to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		
		next.ServeHTTP(wrapped, r)
		
		s.logger.WithFields(logrus.Fields{
			"method":     r.Method,
			"path":       r.URL.Path,
			"status":     wrapped.statusCode,
			"duration":   time.Since(start),
			"remote":     r.RemoteAddr,
			"user_agent": r.UserAgent(),
		}).Info("HTTP request")
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
	
	// Debug logging
	s.logger.Debug("handleGetSymbols called - START")
	
	// Check if MySQL connection exists
	if s.mysqlDB == nil {
		s.logger.Error("MySQL database connection is nil")
		http.Error(w, "Database connection not available", http.StatusInternalServerError)
		return
	}
	
	s.logger.Debug("About to call mysqlDB.GetSymbols")
	
	// Get all symbols from database
	symbols, err := s.mysqlDB.GetSymbols(ctx)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get symbols")
		http.Error(w, "Failed to retrieve symbols", http.StatusInternalServerError)
		return
	}
	
	s.logger.WithField("count", len(symbols)).Info("Retrieved symbols from database")
	
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
	
	// Try cache first
	bars, err := s.redisCache.GetBars(context.Background(), symbol, resolution)
	if err == nil && bars != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"bars": bars,
		})
		return
	}
	
	// Fetch from InfluxDB
	// Implementation depends on your time range parameters
	
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
	Symbol         string  `json:"symbol"`
	Price          float64 `json:"price"`
	Bid            float64 `json:"bid"`
	BidSize        float64 `json:"bidSize"`
	Ask            float64 `json:"ask"`
	AskSize        float64 `json:"askSize"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Volume         float64 `json:"volume"`
	Change         float64 `json:"change"`
	ChangePercent  float64 `json:"changePercent"`
	Timestamp      int64   `json:"timestamp"`
}

// handleGetMarketWatch retrieves user's watchlist with price data
func (s *Server) handleGetMarketWatch(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Session-Token")
	if token == "" {
		http.Error(w, "Session token required", http.StatusUnauthorized)
		return
	}
	
	symbols, err := s.redisCache.GetMarketWatch(context.Background(), token)
	if err != nil {
		http.Error(w, "Failed to get market watch", http.StatusInternalServerError)
		return
	}
	
	// Check if client wants detailed data
	includeDetails := r.URL.Query().Get("detailed") == "true"
	
	if !includeDetails || len(symbols) == 0 {
		// Return simple response for backward compatibility
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"symbols": symbols,
			"count":   len(symbols),
		})
		return
	}
	
	// Fetch price data for all symbols
	marketData := s.fetchMarketData(r.Context(), symbols)
	
	// Log the result for debugging
	s.logger.WithFields(logrus.Fields{
		"symbols_requested": symbols,
		"market_data_count": len(marketData),
	}).Debug("Market data fetched")
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"symbols": marketData,
		"count":   len(marketData),
		"timestamp": time.Now().Unix(),
	})
}

// fetchMarketData fetches current market data for symbols
func (s *Server) fetchMarketData(ctx context.Context, symbols []string) []MarketWatchSymbol {
	if len(symbols) == 0 {
		return []MarketWatchSymbol{}
	}
	
	// Try to get all tickers from cache first
	cacheKey := "all_tickers"
	var tickers map[string]*BinanceTicker24hr
	
	cached, err := s.redisCache.Get(ctx, cacheKey)
	if err == nil && cached != "" {
		json.Unmarshal([]byte(cached), &tickers)
	}
	
	// If not cached or expired, fetch from Binance
	if tickers == nil {
		tickers = s.fetchBinanceAllTickers(ctx)
		// Cache for 5 seconds
		if data, err := json.Marshal(tickers); err == nil {
			s.redisCache.Set(ctx, cacheKey, string(data), 5*time.Second)
		}
	}
	
	// Build response
	result := make([]MarketWatchSymbol, 0, len(symbols))
	for _, symbol := range symbols {
		if ticker, exists := tickers[symbol]; exists && ticker != nil {
			result = append(result, MarketWatchSymbol{
				Symbol:        symbol,
				Price:         ticker.LastPrice,
				Bid:           ticker.BidPrice,
				BidSize:       ticker.BidQuantity,
				Ask:           ticker.AskPrice,
				AskSize:       ticker.AskQuantity,
				High:          ticker.HighPrice,
				Low:           ticker.LowPrice,
				Volume:        ticker.Volume,
				Change:        ticker.PriceChange,
				ChangePercent: ticker.PriceChangePercent,
				Timestamp:     time.Now().Unix(),
			})
		} else {
			// Add placeholder data if ticker not found
			s.logger.WithField("symbol", symbol).Warn("Ticker not found, using placeholder")
			result = append(result, MarketWatchSymbol{
				Symbol:        symbol,
				Price:         0,
				Bid:           0,
				BidSize:       0,
				Ask:           0,
				AskSize:       0,
				High:          0,
				Low:           0,
				Volume:        0,
				Change:        0,
				ChangePercent: 0,
				Timestamp:     time.Now().Unix(),
			})
		}
	}
	
	return result
}

// BinanceTicker24hr represents 24hr ticker from Binance
type BinanceTicker24hr struct {
	Symbol             string  `json:"symbol"`
	PriceChange        float64 `json:"priceChange,string"`
	PriceChangePercent float64 `json:"priceChangePercent,string"`
	LastPrice          float64 `json:"lastPrice,string"`
	BidPrice           float64 `json:"bidPrice,string"`
	BidQuantity        float64 `json:"bidQty,string"`
	AskPrice           float64 `json:"askPrice,string"`
	AskQuantity        float64 `json:"askQty,string"`
	OpenPrice          float64 `json:"openPrice,string"`
	HighPrice          float64 `json:"highPrice,string"`
	LowPrice           float64 `json:"lowPrice,string"`
	Volume             float64 `json:"volume,string"`
	Count              int64   `json:"count"`
}

// fetchBinanceAllTickers fetches all tickers from Binance
func (s *Server) fetchBinanceAllTickers(ctx context.Context) map[string]*BinanceTicker24hr {
	endpoint := "https://api.binance.com/api/v3/ticker/24hr"
	
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to create request")
		return map[string]*BinanceTicker24hr{}
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.logger.WithError(err).Error("Failed to fetch tickers")
		return map[string]*BinanceTicker24hr{}
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		s.logger.Errorf("Binance API error: status=%d", resp.StatusCode)
		return map[string]*BinanceTicker24hr{}
	}
	
	var tickers []BinanceTicker24hr
	if err := json.NewDecoder(resp.Body).Decode(&tickers); err != nil {
		s.logger.WithError(err).Error("Failed to decode response")
		return map[string]*BinanceTicker24hr{}
	}
	
	s.logger.WithField("ticker_count", len(tickers)).Debug("Fetched tickers from Binance")
	
	// Convert to map for easy lookup
	tickerMap := make(map[string]*BinanceTicker24hr)
	for i := range tickers {
		tickerMap[tickers[i].Symbol] = &tickers[i]
	}
	
	return tickerMap
}

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
	
	if err := s.redisCache.AddToMarketWatch(context.Background(), token, req.Symbol); err != nil {
		http.Error(w, "Failed to add symbol", http.StatusInternalServerError)
		return
	}
	
	// Notify all WebSocket clients with the same session token to auto-subscribe
	s.wsManager.AutoSubscribeSymbol(token, req.Symbol)
	
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
	
	if err := s.redisCache.RemoveFromMarketWatch(context.Background(), token, symbol); err != nil {
		http.Error(w, "Failed to remove symbol", http.StatusInternalServerError)
		return
	}
	
	// Notify all WebSocket clients with the same session token to auto-unsubscribe
	s.wsManager.AutoUnsubscribeSymbol(token, symbol)
	
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