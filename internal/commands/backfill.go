package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/pkg/config"
)

var (
	backfillSymbol      string
	backfillInterval    string
	backfillDays        int
	backfillAll         bool
	backfillMarketWatch bool
	backfillSession     string
)

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "Backfill historical market data",
	Long: `Backfill historical market data from Binance.

Examples:
  # Backfill 365 days of daily data for BTCUSDT
  trade-back backfill --symbol BTCUSDT --interval 1d --days 365

  # Backfill 30 days of 1-hour data for ETHUSDT
  trade-back backfill --symbol ETHUSDT --interval 1h --days 30

  # Backfill 2 years of daily data for all active symbols
  trade-back backfill --all --interval 1d --days 730

  # Backfill market watch symbols for a session
  trade-back backfill --marketwatch --session <session-token> --interval 1d --days 30

  # Load ATH/ATL data (requires historical data)
  trade-back backfill ath-atl`,
	RunE: runBackfill,
}

var athAtlCmd = &cobra.Command{
	Use:   "ath-atl",
	Short: "Calculate and store ATH/ATL for all symbols",
	Long:  "Calculates all-time high and all-time low values from historical data",
	RunE:  runATHATL,
}

var syncPendingCmd = &cobra.Command{
	Use:   "sync-pending",
	Short: "Sync all pending market watch symbols",
	Long:  "Syncs historical data for all market watch symbols that have pending or failed status",
	RunE:  runSyncPending,
}

func init() {
	backfillCmd.Flags().StringVar(&backfillSymbol, "symbol", "", "Symbol to backfill (e.g., BTCUSDT)")
	backfillCmd.Flags().StringVar(&backfillInterval, "interval", "5m", "Kline interval (1m, 5m, 15m, 30m, 1h, 4h, 1d, etc.)")
	backfillCmd.Flags().IntVar(&backfillDays, "days", 1825, "Number of days to backfill (default: 5 years)")
	backfillCmd.Flags().BoolVar(&backfillAll, "all", false, "Backfill all active symbols")
	backfillCmd.Flags().BoolVar(&backfillMarketWatch, "marketwatch", false, "Backfill only market watch symbols")
	backfillCmd.Flags().StringVar(&backfillSession, "session", "", "Session token for market watch (required with --marketwatch)")

	backfillCmd.AddCommand(athAtlCmd)
	backfillCmd.AddCommand(syncPendingCmd)
	rootCmd.AddCommand(backfillCmd)
}

func runBackfill(cmd *cobra.Command, args []string) error {
	// Load .env file first
	config.LoadDotEnv()

	// Validate flags
	if !backfillAll && !backfillMarketWatch && backfillSymbol == "" {
		return fmt.Errorf("one of --symbol, --all, or --marketwatch must be specified")
	}

	// Count how many modes are selected
	modesSelected := 0
	if backfillAll {
		modesSelected++
	}
	if backfillMarketWatch {
		modesSelected++
	}
	if backfillSymbol != "" {
		modesSelected++
	}

	if modesSelected > 1 {
		return fmt.Errorf("only one of --symbol, --all, or --marketwatch can be specified")
	}

	// Validate marketwatch specific requirements
	if backfillMarketWatch && backfillSession == "" {
		return fmt.Errorf("--session is required when using --marketwatch")
	}

	// Validate interval
	validIntervals := []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
	isValid := false
	for _, v := range validIntervals {
		if v == backfillInterval {
			isValid = true
			break
		}
	}
	if !isValid {
		return fmt.Errorf("invalid interval: %s. Valid intervals: %s", backfillInterval, strings.Join(validIntervals, ", "))
	}

	// Initialize configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Setup logger
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})

	// nats client init
	natsClient, err := messaging.NewNATSClient(&cfg.NATS, logger)
	if err != nil {
		return fmt.Errorf("failed to create NATS client: %w", err)
	}
	defer natsClient.Close()

	// Initialize database clients
	mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, logger)
	if err != nil {
		return fmt.Errorf("failed to create MySQL client: %w", err)
	}
	defer mysqlClient.Close()

	influxClient := database.NewInfluxClient(&cfg.Database.Influx, logger)
	defer influxClient.Close()

	// Create optimized historical loader (nil NATS for backfill - no real-time updates needed)
	loader := services.NewOptimizedHistoricalLoader(influxClient, mysqlClient, natsClient, cfg, logger)

	ctx := context.Background()

	// Execute backfill
	if backfillAll {
		// logger.WithFields(logrus.Fields{
		// 	"interval": backfillInterval,
		// 	"days":     backfillDays,
		// }).Info("Starting backfill for all symbols")

		if err := loader.LoadAllSymbolsParallel(ctx, backfillInterval, backfillDays); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	} else if backfillMarketWatch {
		// logger.WithFields(logrus.Fields{
		// 	"interval": backfillInterval,
		// 	"days":     backfillDays,
		// 	"session":  backfillSession,
		// }).Info("Starting market watch backfill")

		// Get market watch symbols for the session
		symbols, err := mysqlClient.GetMarketWatchSymbols(ctx, backfillSession)
		if err != nil {
			return fmt.Errorf("failed to get market watch symbols: %w", err)
		}

		if len(symbols) == 0 {
			logger.Warn("No symbols found in market watch for this session")
			return nil
		}

		// logger.WithField("count", len(symbols)).Info("Found market watch symbols")

		// Load historical data for market watch symbols in parallel
		if err := loader.LoadMarketWatchSymbolsParallel(ctx, backfillSession, backfillInterval, backfillDays); err != nil {
			return fmt.Errorf("market watch backfill failed: %w", err)
		}
	} else {
		// logger.WithFields(logrus.Fields{
		// 	"symbol":   backfillSymbol,
		// 	"interval": backfillInterval,
		// 	"days":     backfillDays,
		// }).Info("Starting single symbol backfill")

		if err := loader.LoadHistoricalDataIncremental(ctx, backfillSymbol, backfillInterval, backfillDays); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	}

	// logger.Info("Backfill completed successfully!")

	// Show summary
	if backfillAll {
		symbols, _ := mysqlClient.GetSymbols(ctx)
		activeCount := 0
		for _, s := range symbols {
			if s.IsActive {
				activeCount++
			}
		}
		// logger.WithFields(logrus.Fields{
		// 	"symbols":  activeCount,
		// 	"interval": backfillInterval,
		// 	"days":     backfillDays,
		// }).Info("Backfill summary")
	} else if backfillMarketWatch {
		_, _ = mysqlClient.GetMarketWatchSymbols(ctx, backfillSession)
		// symbols, _ := mysqlClient.GetMarketWatchSymbols(ctx, backfillSession)
		// logger.WithFields(logrus.Fields{
		// 	"symbols":  len(symbols),
		// 	"interval": backfillInterval,
		// 	"days":     backfillDays,
		// 	"session":  backfillSession,
		// }).Info("Market watch backfill summary")
	} else {
		// Calculate approximate data points
		_ = calculateDataPoints(backfillInterval, backfillDays)
		// dataPoints := calculateDataPoints(backfillInterval, backfillDays)
		// logger.WithFields(logrus.Fields{
		// 	"symbol":     backfillSymbol,
		// 	"interval":   backfillInterval,
		// 	"days":       backfillDays,
		// 	"dataPoints": dataPoints,
		// }).Info("Backfill summary")
	}

	return nil
}

func runATHATL(cmd *cobra.Command, args []string) error {
	// Load .env file first
	config.LoadDotEnv()

	// Initialize configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Setup logger
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})

	// Initialize database clients
	mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, logger)
	if err != nil {
		return fmt.Errorf("failed to create MySQL client: %w", err)
	}
	defer mysqlClient.Close()

	influxClient := database.NewInfluxClient(&cfg.Database.Influx, logger)
	defer influxClient.Close()

	// Init the nats client
	natsClient, err := messaging.NewNATSClient(&cfg.NATS, logger)
	if natsClient != nil {
		defer natsClient.Close()
		logger.Info("NATS client error", err)
	}

	// Create optimized historical loader (nil NATS for backfill - no real-time updates needed)
	loader := services.NewOptimizedHistoricalLoader(influxClient, mysqlClient, natsClient, cfg, logger)

	ctx := context.Background()

	// logger.Info("Calculating ATH/ATL for all symbols...")

	// Load ATH/ATL (this will also load 2 years of daily data if needed)
	if err := loader.LoadATHATL(ctx); err != nil {
		return fmt.Errorf("failed to load ATH/ATL: %w", err)
	}

	// logger.Info("ATH/ATL calculation completed!")

	// Show results
	symbols, _ := mysqlClient.GetSymbols(ctx)
	for _, sym := range symbols {
		if !sym.IsActive {
			continue
		}

		ath, atl, err := influxClient.GetATHATL(ctx, sym.Symbol)
		if err != nil {
			logger.WithError(err).WithField("symbol", sym.Symbol).Warn("Failed to get ATH/ATL")
			continue
		}

		if ath > 0 && atl > 0 {
			// logger.WithFields(logrus.Fields{
			// 	"symbol": sym.Symbol,
			// 	"ATH":    fmt.Sprintf("%.8f", ath),
			// 	"ATL":    fmt.Sprintf("%.8f", atl),
			// 	"range":  fmt.Sprintf("%.2f%%", ((ath-atl)/atl)*100),
			// }).Info("Symbol ATH/ATL")
		}
	}

	return nil
}

func calculateDataPoints(interval string, days int) int {
	minutesPerDay := 24 * 60
	totalMinutes := days * minutesPerDay

	intervalMinutes := map[string]int{
		"1m":  1,
		"3m":  3,
		"5m":  5,
		"15m": 15,
		"30m": 30,
		"1h":  60,
		"2h":  120,
		"4h":  240,
		"6h":  360,
		"8h":  480,
		"12h": 720,
		"1d":  1440,
		"3d":  4320,
		"1w":  10080,
		"1M":  43200, // Approximate
	}

	if mins, ok := intervalMinutes[interval]; ok {
		return totalMinutes / mins
	}

	return 0
}

func runSyncPending(cmd *cobra.Command, args []string) error {
	// Load .env file first
	config.LoadDotEnv()

	// Initialize configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Setup logger
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})

	// Initialize database clients
	mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, logger)
	if err != nil {
		return fmt.Errorf("failed to create MySQL client: %w", err)
	}
	defer mysqlClient.Close()

	influxClient := database.NewInfluxClient(&cfg.Database.Influx, logger)
	defer influxClient.Close()

	natsClient, err := messaging.NewNATSClient(&cfg.NATS, logger)
	if natsClient != nil {
		defer natsClient.Close()
		logger.Info("NATS client error", err)
	}

	// Create optimized historical loader (nil NATS for backfill - no real-time updates needed)
	loader := services.NewOptimizedHistoricalLoader(influxClient, mysqlClient, natsClient, cfg, logger)

	ctx := context.Background()

	// Get all pending sync symbols
	pendingSymbols, err := mysqlClient.GetPendingSyncSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending sync symbols: %w", err)
	}

	if len(pendingSymbols) == 0 {
		// logger.Info("No pending symbols to sync")
		return nil
	}

	// logger.WithField("count", len(pendingSymbols)).Info("Found pending symbols to sync")

	// Process each symbol
	successCount := 0
	failedCount := 0

	for _, item := range pendingSymbols {
		symbol := item["symbol"].(string)
		// sessionToken := item["session_token"].(string) // Not needed with new UpdateSyncStatus signature

		// logger.WithFields(logrus.Fields{
		// 	"symbol": symbol,
		// 	"status": item["sync_status"],
		// }).Info("Syncing symbol")

		// Update status to syncing
		if err := mysqlClient.UpdateSyncStatus(ctx, symbol, "syncing", 0, 0, ""); err != nil {
			logger.WithError(err).WithField("symbol", symbol).Error("Failed to update sync status")
			continue
		}

		// Load historical data (1000 days of 1m data)
		err := loader.LoadHistoricalDataIncremental(ctx, symbol, "1m", 1000)
		if err != nil {
			logger.WithError(err).WithField("symbol", symbol).Error("Failed to sync symbol")
			mysqlClient.UpdateSyncStatus(ctx, symbol, "failed", 0, 0, "Backfill failed")
			failedCount++
			continue
		}

		// Mark as completed
		if err := mysqlClient.UpdateSyncStatus(ctx, symbol, "completed", 100, 1000*1440, ""); err != nil {
			logger.WithError(err).WithField("symbol", symbol).Error("Failed to update sync completion")
		}

		successCount++
		// logger.WithField("symbol", symbol).Info("Symbol sync completed")
	}

	// logger.WithFields(logrus.Fields{
	// 	"total":   len(pendingSymbols),
	// 	"success": successCount,
	// 	"failed":  failedCount,
	// }).Info("Sync pending completed")

	return nil
}
