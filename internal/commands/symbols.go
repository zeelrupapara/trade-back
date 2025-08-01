package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/symbols"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/logger"
)

var symbolsCmd = &cobra.Command{
	Use:   "symbols",
	Short: "Manage trading symbols",
	Long:  "Commands for managing and viewing trading symbols",
}

var listSymbolsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all symbols",
	Long:  "List all trading symbols in the system",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Load configuration
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Initialize logger
		log, err := logger.New(&cfg.Logging)
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		// Connect to MySQL
		mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, log)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %w", err)
		}
		defer mysqlClient.Close()

		// Create symbols manager
		symbolsMgr := symbols.NewManager(mysqlClient, log)
		
		// Load symbols
		ctx := context.Background()
		if err := symbolsMgr.Initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize symbols: %w", err)
		}

		// Get filters
		exchange, _ := cmd.Flags().GetString("exchange")
		currency, _ := cmd.Flags().GetString("currency")
		limit, _ := cmd.Flags().GetInt("limit")

		// Get all symbols
		allSymbols := symbolsMgr.GetAllSymbols()
		
		// Apply filters and print
		fmt.Printf("%-15s %-10s %-10s %-10s %-10s %-8s\n", 
			"Symbol", "Exchange", "Base", "Quote", "Type", "Active")
		fmt.Println(strings.Repeat("-", 75))
		
		count := 0
		for _, info := range allSymbols {
			// Filter by exchange
			if exchange != "" && info.Exchange != exchange {
				continue
			}
			
			// Filter by quote currency
			if currency != "" && info.QuoteCurrency != currency {
				continue
			}
			
			// Apply limit
			if limit > 0 && count >= limit {
				break
			}
			
			fmt.Printf("%-15s %-10s %-10s %-10s %-10s %-8v\n",
				info.Symbol,
				info.Exchange,
				info.BaseCurrency,
				info.QuoteCurrency,
				info.InstrumentType,
				info.IsActive,
			)
			count++
		}
		
		fmt.Printf("\nTotal: %d symbols\n", count)
		return nil
	},
}

var activeSymbolsCmd = &cobra.Command{
	Use:   "active",
	Short: "List active symbols",
	Long:  "List all active trading symbols",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Load configuration
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Initialize logger
		log, err := logger.New(&cfg.Logging)
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		// Connect to MySQL
		mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, log)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %w", err)
		}
		defer mysqlClient.Close()

		// Create symbols manager
		symbolsMgr := symbols.NewManager(mysqlClient, log)
		
		// Load symbols
		ctx := context.Background()
		if err := symbolsMgr.Initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize symbols: %w", err)
		}

		// Get active symbols
		activeSymbols := symbolsMgr.GetActiveSymbols()
		
		fmt.Printf("Active symbols: %d\n", len(activeSymbols))
		fmt.Println(strings.Repeat("-", 50))
		
		// Print in columns
		cols := 5
		for i := 0; i < len(activeSymbols); i += cols {
			for j := 0; j < cols && i+j < len(activeSymbols); j++ {
				fmt.Printf("%-15s", activeSymbols[i+j])
			}
			fmt.Println()
		}

		return nil
	},
}

var searchSymbolsCmd = &cobra.Command{
	Use:   "search [pattern]",
	Short: "Search for symbols",
	Long:  "Search for symbols matching a pattern",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Load configuration
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Initialize logger
		log, err := logger.New(&cfg.Logging)
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		// Connect to MySQL
		mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, log)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %w", err)
		}
		defer mysqlClient.Close()

		// Create symbols manager
		symbolsMgr := symbols.NewManager(mysqlClient, log)
		
		// Load symbols
		ctx := context.Background()
		if err := symbolsMgr.Initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize symbols: %w", err)
		}

		pattern := args[0]
		results := symbolsMgr.SearchSymbols(pattern)
		
		fmt.Printf("Found %d symbols matching '%s'\n", len(results), pattern)
		fmt.Println(strings.Repeat("-", 80))
		
		for _, info := range results {
			fmt.Printf("%-15s %-10s %-20s Active: %-5v\n",
				info.Symbol,
				info.Exchange,
				info.FullName,
				info.IsActive,
			)
		}

		return nil
	},
}

var refreshSymbolsCmd = &cobra.Command{
	Use:   "refresh",
	Short: "Refresh symbols from database",
	Long:  "Reload all symbols from the database",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Load configuration
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Initialize logger
		log, err := logger.New(&cfg.Logging)
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		// Connect to MySQL
		mysqlClient, err := database.NewMySQLClient(&cfg.Database.MySQL, log)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %w", err)
		}
		defer mysqlClient.Close()

		// Create symbols manager
		symbolsMgr := symbols.NewManager(mysqlClient, log)
		
		ctx := context.Background()
		
		fmt.Println("Loading symbols from database...")
		
		if err := symbolsMgr.LoadSymbols(ctx); err != nil {
			return fmt.Errorf("failed to load symbols: %w", err)
		}
		
		fmt.Printf("Symbols loaded successfully\n")
		fmt.Printf("Total symbols: %d\n", symbolsMgr.Count())
		fmt.Printf("Active symbols: %d\n", symbolsMgr.ActiveCount())

		return nil
	},
}

func init() {
	rootCmd.AddCommand(symbolsCmd)
	
	// Add subcommands
	symbolsCmd.AddCommand(listSymbolsCmd)
	symbolsCmd.AddCommand(activeSymbolsCmd)
	symbolsCmd.AddCommand(searchSymbolsCmd)
	symbolsCmd.AddCommand(refreshSymbolsCmd)
	
	// Flags for list command
	listSymbolsCmd.Flags().StringP("exchange", "e", "", "Filter by exchange")
	listSymbolsCmd.Flags().StringP("currency", "c", "", "Filter by quote currency")
	listSymbolsCmd.Flags().IntP("limit", "l", 0, "Limit number of results")
}