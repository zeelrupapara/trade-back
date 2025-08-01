package app

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/aggregation"
	"github.com/trade-back/internal/api"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/indicator/enigma"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/internal/messaging"
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
	enigmaCalc   *enigma.Calculator
	ohlcvAgg     *aggregation.OHLCVAggregator
	sessionMgr   *session.Manager
	apiServer    *api.Server
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
	a.logger.Info("Initializing application components...")
	
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
	
	a.logger.Info("Application initialized successfully")
	return nil
}

// Start starts the application
func (a *App) Start() error {
	a.logger.Info("Starting application...")
	
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
	
	a.logger.Info("Application started successfully")
	return nil
}

// Stop gracefully stops the application
func (a *App) Stop() error {
	a.logger.Info("Stopping application...")
	
	// Cancel context to signal shutdown
	a.cancel()
	
	// Wait for all goroutines to finish
	a.wg.Wait()
	
	// Close connections
	if err := a.closeConnections(); err != nil {
		a.logger.WithError(err).Error("Error closing connections")
	}
	
	a.logger.Info("Application stopped successfully")
	return nil
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
	a.logger.Info("Initializing database connections...")
	
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
	a.logger.Info("Initializing symbols manager...")
	
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
	a.logger.Info("Initializing cache...")
	
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
	a.logger.Info("Initializing messaging system...")
	
	natsClient, err := messaging.NewNATSClient(&a.cfg.Messaging.NATS, a.logger)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	a.natsClient = natsClient
	
	return nil
}

func (a *App) initializeExchange() error {
	a.logger.Info("Initializing exchange connections...")
	
	// Create hub
	a.hub = exchange.NewHub(a.cfg, a.logger)
	
	// Initialize hub with dependencies
	if err := a.hub.Initialize(a.natsClient, a.influxDB, a.mysqlDB, a.redisCache); err != nil {
		return fmt.Errorf("failed to initialize hub: %w", err)
	}
	
	// Create services
	a.enigmaCalc = enigma.NewCalculator(a.influxDB, a.redisCache, a.natsClient, &a.cfg.Features, a.logger)
	a.ohlcvAgg = aggregation.NewOHLCVAggregator(a.influxDB, a.natsClient, a.logger)
	
	// Create session manager
	a.sessionMgr = session.NewManager(a.mysqlDB, a.natsClient, a.logger)
	
	return nil
}

func (a *App) initializeWebSocket() error {
	a.logger.Info("Initializing WebSocket manager...")
	
	// Create binary WebSocket manager
	a.wsManager = websocket.NewBinaryManager(
		a.hub,
		a.natsClient,
		a.redisCache,
		a.logger,
	)
	
	// TODO: Configure WebSocket settings if needed
	// a.wsManager.SetConfig(&a.cfg.WebSocket)
	
	return nil
}

func (a *App) initializeAPIServer() error {
	a.logger.Info("Initializing API server...")
	
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
	)
	
	return nil
}

func (a *App) closeConnections() error {
	a.logger.Info("Closing connections...")
	
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