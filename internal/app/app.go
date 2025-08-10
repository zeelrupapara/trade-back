package app

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/aggregation"
	"github.com/trade-back/internal/api"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/external"
	"github.com/trade-back/internal/indicator/enigma"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/internal/session"
	"github.com/trade-back/internal/symbols"
	"github.com/trade-back/internal/websocket"
	"github.com/trade-back/pkg/config"
)

// App represents the main application
type App struct {
	cfg        *config.Config
	logger     *logrus.Logger
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	
	// Core components
	hub          *exchange.Hub
	wsManager    *websocket.BinaryManager
	influxDB     *database.InfluxClient
	mysqlDB      *database.MySQLClient
	redisCache   *cache.RedisClient
	natsClient   *messaging.NATSClient
	symbolsMgr   *symbols.Manager
	
	// Services
	enigmaCalc       *enigma.Calculator
	ohlcvAgg         *aggregation.OHLCVAggregator
	sessionMgr       *session.Manager
	apiServer        *api.Server
	historicalLoader *services.OptimizedHistoricalLoader
	instantEnigma    *services.InstantEnigmaService
	coingecko        *external.CoinGeckoClient
	alphaVantage     *external.AlphaVantageClient
	extremeTracker   *services.UnifiedExtremeTracker
}

// New creates a new application instance
func New(cfg *config.Config, logger *logrus.Logger) *App {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &App{
		cfg:    cfg,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Initialize initializes all application components
func (a *App) Initialize() error {
	// Initialize database connections
	if err := a.initializeDatabase(); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	
	// Initialize symbols manager (must be after database)
	if err := a.initializeSymbols(); err != nil {
		return fmt.Errorf("failed to initialize symbols: %w", err)
	}
	
	// Initialize cache
	if err := a.initializeCache(); err != nil {
		return fmt.Errorf("failed to initialize cache: %w", err)
	}
	
	// Initialize messaging
	if err := a.initializeMessaging(); err != nil {
		return fmt.Errorf("failed to initialize messaging: %w", err)
	}
	
	// Initialize exchange connections
	if err := a.initializeExchange(); err != nil {
		return fmt.Errorf("failed to initialize exchange: %w", err)
	}
	
	// Initialize WebSocket manager
	if err := a.initializeWebSocket(); err != nil {
		return fmt.Errorf("failed to initialize WebSocket: %w", err)
	}
	
	// Initialize API server
	if err := a.initializeAPIServer(); err != nil {
		return fmt.Errorf("failed to initialize API server: %w", err)
	}
	
	// a.logger.Info("Application initialized successfully")
	return nil
}

// Start starts the application
func (a *App) Start() error {
	// Start price hub
	if err := a.hub.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start hub: %w", err)
	}
	
	// Start WebSocket manager
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.wsManager.Run(a.ctx)
	}()
	
	// Start Enigma calculator
	if a.cfg.Features.EnigmaEnabled {
		if err := a.enigmaCalc.Start(a.ctx); err != nil {
			return fmt.Errorf("failed to start enigma calculator: %w", err)
		}
	}
	
	// Start OHLCV aggregator
	if err := a.ohlcvAgg.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start OHLCV aggregator: %w", err)
	}
	
	// Start session manager
	if err := a.sessionMgr.Start(a.ctx); err != nil {
		return fmt.Errorf("failed to start session manager: %w", err)
	}
	
	// Start API server
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		if err := a.apiServer.Start(); err != nil {
			if err != http.ErrServerClosed {
				a.logger.WithError(err).Error("API server error")
			}
		}
	}()
	
	// Resume any incomplete historical data syncs
	if a.historicalLoader != nil {
		go func() {
			// Wait a bit for services to stabilize
			time.Sleep(5 * time.Second)
			if err := a.historicalLoader.ResumeIncompleteSync(a.ctx); err != nil {
				a.logger.WithError(err).Warn("Failed to resume incomplete syncs")
			}
		}()
	}
	
	// a.logger.Info("Application started successfully")
	return nil
}

// Stop gracefully stops the application
func (a *App) Stop() error {
	a.logger.Info("Stopping application...")
	
	// Cancel context to signal shutdown
	a.cancel()
	
	// Create a channel to signal when goroutines are done
	done := make(chan struct{})
	
	// Wait for goroutines in a separate goroutine
	go func() {
		a.wg.Wait()
		close(done)
	}()
	
	// Wait for goroutines with timeout (3 seconds)
	select {
	case <-done:
		// All goroutines finished
		a.logger.Info("All goroutines stopped")
	case <-time.After(3 * time.Second):
		a.logger.Warn("Timeout waiting for goroutines to finish")
	}
	
	// Stop services with individual timeouts
	a.stopServicesWithTimeout()
	
	// Close connections
	if err := a.closeConnections(); err != nil {
		a.logger.WithError(err).Error("Error closing connections")
	}
	
	a.logger.Info("Application stopped successfully")
	return nil
}

// stopServicesWithTimeout stops each service with a timeout
func (a *App) stopServicesWithTimeout() {
	// Stop API server with timeout
	if a.apiServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := a.apiServer.Stop(ctx); err != nil {
			a.logger.WithError(err).Error("Error stopping API server")
		}
		cancel()
	}
	
	// Stop hub
	if a.hub != nil {
		if err := a.hub.Stop(); err != nil {
			a.logger.WithError(err).Error("Error stopping hub")
		}
	}
	
	// Stop session manager
	if a.sessionMgr != nil {
		if err := a.sessionMgr.Stop(); err != nil {
			a.logger.WithError(err).Error("Error stopping session manager")
		}
	}
	
	// Stop OHLCV aggregator
	if a.ohlcvAgg != nil {
		if err := a.ohlcvAgg.Stop(); err != nil {
			a.logger.WithError(err).Error("Error stopping OHLCV aggregator")
		}
	}
	
	// Stop Enigma calculator
	if a.enigmaCalc != nil {
		if err := a.enigmaCalc.Stop(); err != nil {
			a.logger.WithError(err).Error("Error stopping Enigma calculator")
		}
	}
}

// GetContext returns the application context
func (a *App) GetContext() context.Context {
	return a.ctx
}

// GetConfig returns the application configuration
func (a *App) GetConfig() *config.Config {
	return a.cfg
}

// GetLogger returns the application logger
func (a *App) GetLogger() *logrus.Logger {
	return a.logger
}

// GetSymbolsManager returns the symbols manager
func (a *App) GetSymbolsManager() *symbols.Manager {
	return a.symbolsMgr
}

// Private initialization methods

func (a *App) initializeDatabase() error {
	
	// Initialize MySQL
	mysqlClient, err := database.NewMySQLClient(&a.cfg.Database.MySQL, a.logger)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	a.mysqlDB = mysqlClient
	
	// Initialize InfluxDB
	a.influxDB = database.NewInfluxClient(&a.cfg.Database.Influx, a.logger)
	
	// Test InfluxDB connection
	if err := a.influxDB.Health(a.ctx); err != nil {
		return fmt.Errorf("failed to connect to InfluxDB: %w", err)
	}
	
	return nil
}

func (a *App) initializeSymbols() error {
	
	// Create symbols manager
	a.symbolsMgr = symbols.NewManager(a.mysqlDB, a.logger)
	
	// Load all symbols from database
	if err := a.symbolsMgr.Initialize(a.ctx); err != nil {
		return fmt.Errorf("failed to initialize symbols manager: %w", err)
	}
	
	a.logger.WithFields(logrus.Fields{
		"total":  a.symbolsMgr.Count(),
		"active": a.symbolsMgr.ActiveCount(),
	}).Info("Symbols manager initialized")
	
	return nil
}

func (a *App) initializeCache() error {
	
	redisClient, err := cache.NewRedisClient(&a.cfg.Cache.Redis, a.logger)
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	a.redisCache = redisClient
	
	// Set cache TTL
	a.redisCache.SetTTL(a.cfg.Performance.CacheTTL)
	
	return nil
}

func (a *App) initializeMessaging() error {
	
	natsClient, err := messaging.NewNATSClient(&a.cfg.Messaging.NATS, a.logger)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	a.natsClient = natsClient
	
	return nil
}

func (a *App) initializeExchange() error {
	
	// Create hub
	a.hub = exchange.NewHub(a.cfg, a.logger)
	
	// Initialize hub with dependencies
	if err := a.hub.Initialize(a.natsClient, a.influxDB, a.mysqlDB, a.redisCache); err != nil {
		return fmt.Errorf("failed to initialize hub: %w", err)
	}
	
	// Create Alpha Vantage client for forex/stocks
	alphaVantageKey := "demo" // Use demo key, can be configured later
	a.alphaVantage = external.NewAlphaVantageClient(alphaVantageKey, a.logger)
	
	// Create unified extreme tracker
	a.extremeTracker = services.NewUnifiedExtremeTracker(
		a.coingecko,
		a.alphaVantage,
		a.influxDB,
		a.mysqlDB,
		a.redisCache,
		a.logger,
	)
	
	// Create services
	a.enigmaCalc = enigma.NewCalculator(
		a.influxDB,
		a.redisCache,
		a.natsClient,
		a.extremeTracker,
		&a.cfg.Features,
		a.logger,
	)
	a.ohlcvAgg = aggregation.NewOHLCVAggregator(a.influxDB, a.natsClient, a.logger)
	
	// Create session manager
	a.sessionMgr = session.NewManager(a.mysqlDB, a.natsClient, a.logger)
	
	return nil
}

func (a *App) initializeWebSocket() error {
	
	// Create binary WebSocket manager
	a.wsManager = websocket.NewBinaryManager(
		a.hub,
		a.natsClient,
		a.redisCache,
		a.mysqlDB,
		a.logger,
	)
	
	// TODO: Configure WebSocket settings if needed
	// a.wsManager.SetConfig(&a.cfg.WebSocket)
	
	return nil
}

func (a *App) initializeAPIServer() error {
	
	// Create historical loader
	a.historicalLoader = services.NewOptimizedHistoricalLoader(
		a.influxDB,
		a.mysqlDB,
		a.natsClient,
		a.logger,
	)
	
	// Create CoinGecko client (empty API key for free tier)
	a.coingecko = external.NewCoinGeckoClient("", a.logger)
	
	// Create instant Enigma service
	a.instantEnigma = services.NewInstantEnigmaService(
		a.coingecko,
		a.influxDB,
		a.redisCache,
		a.natsClient,
		a.logger,
	)
	
	// Create API server
	a.apiServer = api.NewServer(
		a.cfg,
		a.logger,
		a.influxDB,
		a.mysqlDB,
		a.redisCache,
		a.natsClient,
		a.enigmaCalc,
		a.wsManager,
		a.historicalLoader,
		a.instantEnigma,
		a.hub,
		a.extremeTracker,
	)
	
	return nil
}

func (a *App) closeConnections() error {
	
	var errs []error
	
	// Stop services first
	if a.sessionMgr != nil {
		if err := a.sessionMgr.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop session manager: %w", err))
		}
	}
	
	if a.ohlcvAgg != nil {
		if err := a.ohlcvAgg.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop OHLCV aggregator: %w", err))
		}
	}
	
	if a.enigmaCalc != nil {
		if err := a.enigmaCalc.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop enigma calculator: %w", err))
		}
	}
	
	if a.hub != nil {
		if err := a.hub.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop hub: %w", err))
		}
	}
	
	// Close database connections
	if a.mysqlDB != nil {
		if err := a.mysqlDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close MySQL: %w", err))
		}
	}
	
	if a.influxDB != nil {
		a.influxDB.Close()
		// InfluxDB Close() doesn't return an error
	}
	
	// Close cache connection
	if a.redisCache != nil {
		if err := a.redisCache.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Redis: %w", err))
		}
	}
	
	// Close messaging connection
	if a.natsClient != nil {
		if err := a.natsClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close NATS: %w", err))
		}
	}
	
	// Stop API server
	if a.apiServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.apiServer.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop API server: %w", err))
		}
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	
	return nil
}