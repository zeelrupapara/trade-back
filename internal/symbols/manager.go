package symbols

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/pkg/models"
)

// Manager manages all trading symbols in the system
type Manager struct {
	symbols      map[string]*models.SymbolInfo  // symbol -> info
	symbolsByID  map[int]*models.SymbolInfo     // id -> info
	activeSymbols []string                       // list of active symbols
	
	mysql        *database.MySQLClient
	logger       *logrus.Entry
	
	mu           sync.RWMutex
	lastRefresh  time.Time
	refreshInterval time.Duration
}

// NewManager creates a new symbols manager
func NewManager(mysql *database.MySQLClient, logger *logrus.Logger) *Manager {
	return &Manager{
		symbols:         make(map[string]*models.SymbolInfo),
		symbolsByID:     make(map[int]*models.SymbolInfo),
		activeSymbols:   make([]string, 0),
		mysql:           mysql,
		logger:          logger.WithField("component", "symbols-manager"),
		refreshInterval: 5 * time.Minute,
	}
}

// Initialize loads all symbols from database and syncs with exchange
func (m *Manager) Initialize(ctx context.Context) error {
	
	// First sync symbols from Binance
	if err := m.SyncBinanceSymbols(ctx); err != nil {
		m.logger.WithError(err).Warn("Failed to sync Binance symbols, loading from database only")
	}
	
	// Load symbols from database
	if err := m.LoadSymbols(ctx); err != nil {
		return fmt.Errorf("failed to load symbols: %w", err)
	}
	
	return nil
}

// LoadSymbols loads all symbols from database
func (m *Manager) LoadSymbols(ctx context.Context) error {
	symbols, err := m.mysql.GetSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols from database: %w", err)
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Clear existing data
	m.symbols = make(map[string]*models.SymbolInfo)
	m.symbolsByID = make(map[int]*models.SymbolInfo)
	m.activeSymbols = make([]string, 0)
	
	// Load new data
	for _, symbol := range symbols {
		m.symbols[symbol.Symbol] = symbol
		m.symbolsByID[symbol.ID] = symbol
		
		if symbol.IsActive {
			m.activeSymbols = append(m.activeSymbols, symbol.Symbol)
		}
	}
	
	m.lastRefresh = time.Now()
	
	m.logger.WithFields(logrus.Fields{
		"total":  len(m.symbols),
		"active": len(m.activeSymbols),
	})
	
	return nil
}

// RefreshIfNeeded refreshes symbols if refresh interval has passed
func (m *Manager) RefreshIfNeeded(ctx context.Context) error {
	m.mu.RLock()
	needsRefresh := time.Since(m.lastRefresh) > m.refreshInterval
	m.mu.RUnlock()
	
	if needsRefresh {
		return m.LoadSymbols(ctx)
	}
	
	return nil
}

// GetSymbol returns symbol info by symbol name
func (m *Manager) GetSymbol(symbol string) (*models.SymbolInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	info, exists := m.symbols[symbol]
	return info, exists
}

// GetSymbolByID returns symbol info by ID
func (m *Manager) GetSymbolByID(id int) (*models.SymbolInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	info, exists := m.symbolsByID[id]
	return info, exists
}

// GetAllSymbols returns all symbols
func (m *Manager) GetAllSymbols() map[string]*models.SymbolInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return a copy to prevent external modifications
	result := make(map[string]*models.SymbolInfo, len(m.symbols))
	for k, v := range m.symbols {
		result[k] = v
	}
	return result
}

// GetActiveSymbols returns list of active symbol names
func (m *Manager) GetActiveSymbols() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return a copy
	result := make([]string, len(m.activeSymbols))
	copy(result, m.activeSymbols)
	return result
}

// GetActiveSymbolsInfo returns all active symbol info
func (m *Manager) GetActiveSymbolsInfo() []*models.SymbolInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make([]*models.SymbolInfo, 0, len(m.activeSymbols))
	for _, symbol := range m.activeSymbols {
		if info, exists := m.symbols[symbol]; exists {
			result = append(result, info)
		}
	}
	return result
}

// IsActive checks if a symbol is active
func (m *Manager) IsActive(symbol string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if info, exists := m.symbols[symbol]; exists {
		return info.IsActive
	}
	return false
}

// Count returns the total number of symbols
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.symbols)
}

// ActiveCount returns the number of active symbols
func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.activeSymbols)
}

// AddOrUpdateSymbol adds or updates a symbol
func (m *Manager) AddOrUpdateSymbol(ctx context.Context, symbol *models.SymbolInfo) error {
	// Save to database first
	if err := m.mysql.InsertSymbol(ctx, symbol); err != nil {
		return fmt.Errorf("failed to save symbol to database: %w", err)
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Update in-memory maps
	m.symbols[symbol.Symbol] = symbol
	m.symbolsByID[symbol.ID] = symbol
	
	// Update active symbols list
	m.rebuildActiveSymbolsList()
	
	m.logger.WithFields(logrus.Fields{
		"symbol": symbol.Symbol,
		"active": symbol.IsActive,
	})
	
	return nil
}

// SetSymbolActive sets the active status of a symbol
func (m *Manager) SetSymbolActive(ctx context.Context, symbol string, active bool) error {
	info, exists := m.GetSymbol(symbol)
	if !exists {
		return fmt.Errorf("symbol %s not found", symbol)
	}
	
	// Update the symbol info
	info.IsActive = active
	
	// Update in database
	if err := m.mysql.UpdateSymbol(ctx, info); err != nil {
		return fmt.Errorf("failed to update symbol in database: %w", err)
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Update active symbols list
	m.rebuildActiveSymbolsList()
	
	m.logger.WithFields(logrus.Fields{
		"symbol": symbol,
		"active": active,
	})
	
	return nil
}

// rebuildActiveSymbolsList rebuilds the active symbols list (must be called with lock held)
func (m *Manager) rebuildActiveSymbolsList() {
	m.activeSymbols = make([]string, 0, len(m.symbols))
	for symbol, info := range m.symbols {
		if info.IsActive {
			m.activeSymbols = append(m.activeSymbols, symbol)
		}
	}
}

// GetSymbolsByExchange returns symbols for a specific exchange
func (m *Manager) GetSymbolsByExchange(exchange string) []*models.SymbolInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make([]*models.SymbolInfo, 0)
	for _, info := range m.symbols {
		if info.Exchange == exchange {
			result = append(result, info)
		}
	}
	return result
}

// GetSymbolsByQuoteCurrency returns symbols with specific quote currency
func (m *Manager) GetSymbolsByQuoteCurrency(currency string) []*models.SymbolInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make([]*models.SymbolInfo, 0)
	for _, info := range m.symbols {
		if info.QuoteCurrency == currency {
			result = append(result, info)
		}
	}
	return result
}

// SearchSymbols searches for symbols matching a pattern
func (m *Manager) SearchSymbols(pattern string) []*models.SymbolInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make([]*models.SymbolInfo, 0)
	for _, info := range m.symbols {
		// Simple substring search - can be enhanced with fuzzy matching
		if contains(info.Symbol, pattern) || 
		   contains(info.FullName, pattern) ||
		   contains(info.BaseCurrency, pattern) {
			result = append(result, info)
		}
	}
	return result
}

// contains is a case-insensitive substring search
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		(s == substr || 
		 len(substr) > 0 && 
		 (contains(s[1:], substr) || 
		  (s[0] == substr[0] && contains(s[1:], substr[1:]))))
}

// SyncBinanceSymbols fetches all symbols from Binance and updates the database
func (m *Manager) SyncBinanceSymbols(ctx context.Context) error {
	
	// Fetch exchange info from Binance
	endpoint := "https://api.binance.com/api/v3/exchangeInfo"
	
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch exchange info: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Binance API error: status=%d", resp.StatusCode)
	}
	
	var exchangeInfo exchange.BinanceExchangeInfo
	if err := json.NewDecoder(resp.Body).Decode(&exchangeInfo); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	
	
	// Process each symbol
	var newSymbols, updatedSymbols int
	for _, binanceSymbol := range exchangeInfo.Symbols {
		// Only process TRADING status symbols
		if binanceSymbol.Status != "TRADING" {
			continue
		}
		
		// Only process spot trading symbols
		if !binanceSymbol.IsSpotTradingAllowed {
			continue
		}
		
		// Get price and quantity step sizes from filters
		var tickSize, stepSize float64
		for _, filter := range binanceSymbol.Filters {
			switch filter.FilterType {
			case "PRICE_FILTER":
				if filter.TickSize != "" {
					tickSize, _ = strconv.ParseFloat(filter.TickSize, 64)
				}
			case "LOT_SIZE":
				if filter.StepSize != "" {
					stepSize, _ = strconv.ParseFloat(filter.StepSize, 64)
				}
			}
		}
		
		// Create symbol info
		symbolInfo := &models.SymbolInfo{
			Exchange:             "binance",
			Symbol:               binanceSymbol.Symbol,
			FullName:             fmt.Sprintf("%s/%s", binanceSymbol.BaseAsset, binanceSymbol.QuoteAsset),
			InstrumentType:       "spot",
			BaseCurrency:         binanceSymbol.BaseAsset,
			QuoteCurrency:        binanceSymbol.QuoteAsset,
			IsActive:             true,
			MinPriceIncrement:    tickSize,
			MinQuantityIncrement: stepSize,
		}
		
		// Check if symbol already exists
		existing, exists := m.GetSymbol(binanceSymbol.Symbol)
		if exists {
			// Update existing symbol if needed
			if existing.MinPriceIncrement != tickSize || existing.MinQuantityIncrement != stepSize {
				symbolInfo.ID = existing.ID
				if err := m.mysql.UpdateSymbol(ctx, symbolInfo); err != nil {
					m.logger.WithError(err).WithField("symbol", binanceSymbol.Symbol).Warn("Failed to update symbol")
					continue
				}
				updatedSymbols++
			}
		} else {
			// Insert new symbol
			if err := m.mysql.InsertSymbol(ctx, symbolInfo); err != nil {
				m.logger.WithError(err).WithField("symbol", binanceSymbol.Symbol).Warn("Failed to insert symbol")
				continue
			}
			newSymbols++
		}
	}
	
	m.logger.WithFields(logrus.Fields{
		"new_symbols":     newSymbols,
		"updated_symbols": updatedSymbols,
	})
	
	return nil
}