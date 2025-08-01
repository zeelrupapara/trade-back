package commands

import (
	"github.com/spf13/cobra"
)

var (
	verbose bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "trade-back",
	Short: "High-Performance Trading Backend",
	Long: `A high-performance trading backend built with Go, designed for ultra-low latency 
price processing and seamless gap-free data delivery.

Features:
• Ultra-low latency processing (<3ms end-to-end)
• Binary protocol (72% smaller messages, 16x faster parsing)
• Gap-free data delivery with intelligent buffering
• NATS-based message distribution for scalability
• TradingView integration with custom datafeed
• Enigma levels technical indicator calculation`,
	Version: "1.0.0",
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
}

