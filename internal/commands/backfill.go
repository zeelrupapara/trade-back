package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/pkg/config"
)

var (
	backfillSymbol   string
	backfillInterval string
	backfillDays     int
	backfillAll      bool
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

func init() {
	backfillCmd.Flags().StringVar(&backfillSymbol, "symbol", "", "Symbol to backfill (e.g., BTCUSDT)")
	backfillCmd.Flags().StringVar(&backfillInterval, "interval", "1d", "Kline interval (1m, 5m, 15m, 30m, 1h, 4h, 1d, etc.)")
	backfillCmd.Flags().IntVar(&backfillDays, "days", 30, "Number of days to backfill")
	backfillCmd.Flags().BoolVar(&backfillAll, "all", false, "Backfill all active symbols")
	
	backfillCmd.AddCommand(athAtlCmd)
	rootCmd.AddCommand(backfillCmd)
}

func runBackfill(cmd *cobra.Command, args []string) error {
	// Validate flags
	if !backfillAll && backfillSymbol == "" {
		return fmt.Errorf("either --symbol or --all must be specified")
	}
	
	if backfillAll && backfillSymbol != "" {
		return fmt.Errorf("cannot specify both --symbol and --all")
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
	
	// Initialize database clients
	mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, logger)
	if err != nil {
		return fmt.Errorf("failed to create MySQL client: %w", err)
	}
	defer mysqlClient.Close()
	
	influxClient := database.NewInfluxClient(&cfg.Database.Influx, logger)
	defer influxClient.Close()
	
	// Create historical loader
	loader := services.NewHistoricalLoader(influxClient, mysqlClient, logger)
	
	ctx := context.Background()
	
	// Execute backfill
	if backfillAll {
		logger.WithFields(logrus.Fields{
			"interval": backfillInterval,
			"days":     backfillDays,
		}).Info("Starting backfill for all active symbols")
		
		if err := loader.LoadAllSymbols(ctx, backfillInterval, backfillDays); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	} else {
		logger.WithFields(logrus.Fields{
			"symbol":   backfillSymbol,
			"interval": backfillInterval,
			"days":     backfillDays,
		}).Info("Starting backfill for single symbol")
		
		if err := loader.LoadHistoricalData(ctx, backfillSymbol, backfillInterval, backfillDays); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	}
	
	logger.Info("Backfill completed successfully!")
	
	// Show summary
	if backfillAll {
		symbols, _ := mysqlClient.GetSymbols(ctx)
		activeCount := 0
		for _, s := range symbols {
			if s.IsActive {
				activeCount++
			}
		}
		logger.WithFields(logrus.Fields{
			"symbols":  activeCount,
			"interval": backfillInterval,
			"days":     backfillDays,
		}).Info("Backfill summary")
	} else {
		// Calculate approximate data points
		dataPoints := calculateDataPoints(backfillInterval, backfillDays)
		logger.WithFields(logrus.Fields{
			"symbol":     backfillSymbol,
			"interval":   backfillInterval,
			"days":       backfillDays,
			"dataPoints": dataPoints,
		}).Info("Backfill summary")
	}
	
	return nil
}

func runATHATL(cmd *cobra.Command, args []string) error {
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
	
	// Create historical loader
	loader := services.NewHistoricalLoader(influxClient, mysqlClient, logger)
	
	ctx := context.Background()
	
	logger.Info("Calculating ATH/ATL for all symbols...")
	
	// Load ATH/ATL (this will also load 2 years of daily data if needed)
	if err := loader.LoadATHATL(ctx); err != nil {
		return fmt.Errorf("failed to load ATH/ATL: %w", err)
	}
	
	logger.Info("ATH/ATL calculation completed!")
	
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
			logger.WithFields(logrus.Fields{
				"symbol": sym.Symbol,
				"ATH":    fmt.Sprintf("%.8f", ath),
				"ATL":    fmt.Sprintf("%.8f", atl),
				"range":  fmt.Sprintf("%.2f%%", ((ath-atl)/atl)*100),
			}).Info("Symbol ATH/ATL")
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