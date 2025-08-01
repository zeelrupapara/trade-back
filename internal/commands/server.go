package commands

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/trade-back/internal/app"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/logger"
)

var (
	serverPort int
	serverHost string
	logLevel   string
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the trading backend server",
	Long: `Start the high-performance trading backend server.

This will start all components:
• WebSocket server for real-time price updates (JSON & Binary protocols)
• REST API for historical data and symbol management  
• Exchange connection manager (Binance WebSocket)
• NATS message distribution system
• Redis caching layer
• InfluxDB time-series data storage

The server supports graceful shutdown and hot-reloading of configurations.

Examples:
  trade-back server                    # Start with default settings
  trade-back server --port 9090       # Start on custom port
  trade-back server --host 0.0.0.0    # Bind to all interfaces
  trade-back server --log-level debug # Enable debug logging`,
	RunE: runServer,
}

func init() {
	rootCmd.AddCommand(serverCmd)

	// Server-specific flags
	serverCmd.Flags().IntVarP(&serverPort, "port", "p", 8080, "Server port")
	serverCmd.Flags().StringVarP(&serverHost, "host", "H", "0.0.0.0", "Server host")
	serverCmd.Flags().StringVarP(&logLevel, "log-level", "l", "info", "Log level (debug, info, warn, error)")

	// Flags are handled directly in runServer
}

func runServer(cmd *cobra.Command, args []string) error {
	// Load .env file first
	if err := config.LoadDotEnv(); err != nil {
		// Log but don't fail - .env file is optional
		fmt.Printf("Note: .env file not loaded: %v\n", err)
	}
	
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Override config with command line flags if provided
	if serverHost != "" {
		cfg.Server.Host = serverHost
	}
	if serverPort != 0 {
		cfg.Server.Port = serverPort
	}
	if logLevel != "" {
		cfg.Logging.Level = logLevel
	}

	// Setup logger
	log, _ := logger.New(&cfg.Logging)
	log.Info("🚀 Starting High-Performance Trading Backend Server")

	// Create application
	application := app.New(cfg, log)

	// Initialize application
	if err := application.Initialize(); err != nil {
		log.WithError(err).Error("Failed to initialize application")
		return err
	}

	// Start application
	if err := application.Start(); err != nil {
		log.WithError(err).Error("Failed to start application")
		return err
	}

	// Wait for interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	sig := <-interrupt
	log.WithField("signal", sig.String()).Info("🛑 Shutdown signal received")

	// Graceful shutdown
	if err := application.Stop(); err != nil {
		log.WithError(err).Error("❌ Application shutdown error")
		return err
	}

	log.Info("✅ Application shutdown complete")
	return nil
}

