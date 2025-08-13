package services

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/pkg/config"
)

// SymbolLoader loads symbols from exchanges and stores them in the database
type SymbolLoader struct {
	mysql  *database.MySQLClient
	logger *logrus.Entry
	config *config.Config

	// Exchange clients
	oandaREST   *exchange.OANDARESTClient
	binanceREST *exchange.BinanceRESTClient
}

// NewSymbolLoader creates a new symbol loader
func NewSymbolLoader(mysql *database.MySQLClient, config *config.Config, logger *logrus.Logger) *SymbolLoader {
	return &SymbolLoader{
		mysql:       mysql,
		config:      config,
		logger:      logger.WithField("component", "symbol-loader"),
		oandaREST:   exchange.NewOANDARESTClient(&config.Exchange.OANDA, logger),
		binanceREST: exchange.NewBinanceRESTClient(logger),
	}
}

// LoadAllSymbols loads symbols from all configured exchanges
func (sl *SymbolLoader) LoadAllSymbols(ctx context.Context) error {
	sl.logger.Info("Loading symbols from all exchanges...")

	var allErrors []error

	// Load Binance symbols (always available, uses public API)
	if err := sl.loadBinanceSymbols(ctx); err != nil {
		sl.logger.WithError(err).Error("Failed to load Binance symbols")
		allErrors = append(allErrors, err)
	}

	// Load OANDA symbols if configured
	if sl.config.Exchange.OANDA.APIKey != "" && sl.config.Exchange.OANDA.AccountID != "" {
		if err := sl.loadOANDASymbols(ctx); err != nil {
			sl.logger.WithError(err).Error("Failed to load OANDA symbols")
			allErrors = append(allErrors, err)
		}
	} else {
		sl.logger.Info("OANDA not configured, skipping OANDA symbol loading")
	}

	if len(allErrors) > 0 {
		sl.logger.WithField("errors", len(allErrors)).Error("Some exchanges failed to load symbols")
		return allErrors[0] // Return first error
	}

	sl.logger.Info("Completed loading symbols from all exchanges")
	return nil
}

// LoadBinanceSymbols loads Binance crypto symbols
func (sl *SymbolLoader) LoadBinanceSymbols(ctx context.Context) error {
	return sl.loadBinanceSymbols(ctx)
}

// LoadOANDASymbols loads OANDA forex symbols
func (sl *SymbolLoader) LoadOANDASymbols(ctx context.Context) error {
	return sl.loadOANDASymbols(ctx)
}

// loadOANDASymbols loads symbols from OANDA and stores them in database
func (sl *SymbolLoader) loadOANDASymbols(ctx context.Context) error {
	sl.logger.Info("Loading OANDA instruments...")

	// Fetch instruments from OANDA
	instruments, err := sl.oandaREST.GetInstruments(ctx)
	if err != nil {
		return err
	}

	// Convert to trade-back symbol format
	symbols := sl.oandaREST.ConvertToTradeBackSymbols(instruments)

	sl.logger.WithField("count", len(symbols)).Info("Fetched OANDA instruments")

	// Store symbols in database
	stored := 0
	updated := 0

	for _, symbol := range symbols {
		exists, err := sl.mysql.SymbolExists(ctx, symbol.Exchange, symbol.Symbol)
		if err != nil {
			sl.logger.WithError(err).WithFields(logrus.Fields{
				"exchange": symbol.Exchange,
				"symbol":   symbol.Symbol,
			}).Error("Failed to check if symbol exists")
			continue
		}

		if exists {
			// Update existing symbol
			err = sl.mysql.UpdateSymbol(ctx, &symbol)
			if err != nil {
				sl.logger.WithError(err).WithFields(logrus.Fields{
					"exchange": symbol.Exchange,
					"symbol":   symbol.Symbol,
				}).Error("Failed to update symbol")
				continue
			}
			updated++
		} else {
			// Insert new symbol
			err = sl.mysql.InsertSymbol(ctx, &symbol)
			if err != nil {
				sl.logger.WithError(err).WithFields(logrus.Fields{
					"exchange": symbol.Exchange,
					"symbol":   symbol.Symbol,
				}).Error("Failed to insert symbol")
				continue
			}
			stored++
		}
	}

	sl.logger.WithFields(logrus.Fields{
		"stored":  stored,
		"updated": updated,
		"total":   len(symbols),
	}).Info("Completed OANDA symbol loading")

	return nil
}

// LoadSymbolsOnStartup loads symbols from all exchanges on application startup
func (sl *SymbolLoader) LoadSymbolsOnStartup(ctx context.Context) error {
	sl.logger.Info("Loading symbols on startup...")

	// Create context with timeout for startup loading
	startupCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	return sl.LoadAllSymbols(startupCtx)
}

// loadBinanceSymbols loads symbols from Binance and stores them in database
func (sl *SymbolLoader) loadBinanceSymbols(ctx context.Context) error {
	sl.logger.Info("Loading Binance symbols...")

	// Fetch exchange info from Binance
	exchangeInfo, err := sl.binanceREST.GetExchangeInfo(ctx)
	if err != nil {
		return err
	}

	// Convert to trade-back symbol format
	symbols := sl.binanceREST.ConvertToTradeBackSymbols(exchangeInfo)

	sl.logger.WithField("count", len(symbols)).Info("Fetched Binance symbols")

	// Store symbols in database
	stored := 0
	updated := 0

	for _, symbol := range symbols {
		exists, err := sl.mysql.SymbolExists(ctx, symbol.Exchange, symbol.Symbol)
		if err != nil {
			sl.logger.WithError(err).WithFields(logrus.Fields{
				"exchange": symbol.Exchange,
				"symbol":   symbol.Symbol,
			}).Error("Failed to check if symbol exists")
			continue
		}

		if exists {
			// Update existing symbol
			err = sl.mysql.UpdateSymbol(ctx, &symbol)
			if err != nil {
				sl.logger.WithError(err).WithFields(logrus.Fields{
					"exchange": symbol.Exchange,
					"symbol":   symbol.Symbol,
				}).Error("Failed to update symbol")
				continue
			}
			updated++
		} else {
			// Insert new symbol
			err = sl.mysql.InsertSymbol(ctx, &symbol)
			if err != nil {
				sl.logger.WithError(err).WithFields(logrus.Fields{
					"exchange": symbol.Exchange,
					"symbol":   symbol.Symbol,
				}).Error("Failed to insert symbol")
				continue
			}
			stored++
		}
	}

	sl.logger.WithFields(logrus.Fields{
		"stored":  stored,
		"updated": updated,
		"total":   len(symbols),
	}).Info("Completed Binance symbol loading")

	return nil
}

// ScheduleSymbolRefresh schedules periodic symbol refresh (for production use)
func (sl *SymbolLoader) ScheduleSymbolRefresh(ctx context.Context, interval time.Duration) {
	sl.logger.WithField("interval", interval).Info("Starting symbol refresh scheduler")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sl.logger.Info("Symbol refresh scheduler stopped")
			return
		case <-ticker.C:
			sl.logger.Info("Refreshing symbols...")

			refreshCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			if err := sl.LoadAllSymbols(refreshCtx); err != nil {
				sl.logger.WithError(err).Error("Failed to refresh symbols")
			}
			cancel()
		}
	}
}
