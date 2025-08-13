package services

import (
	"regexp"
	"strings"
	"sync"
	
	"github.com/trade-back/pkg/models"
)

// AssetClassifier classifies financial symbols into asset classes
type AssetClassifier struct {
	// Known symbol mappings
	cryptoSymbols    map[string]bool
	forexPairs       map[string]bool
	stockSymbols     map[string]bool
	commoditySymbols map[string]bool
	
	mu sync.RWMutex
}

// NewAssetClassifier creates a new asset classifier
func NewAssetClassifier() *AssetClassifier {
	ac := &AssetClassifier{
		cryptoSymbols:    make(map[string]bool),
		forexPairs:       make(map[string]bool),
		stockSymbols:     make(map[string]bool),
		commoditySymbols: make(map[string]bool),
	}
	
	// Initialize with known symbols
	ac.initializeKnownSymbols()
	
	return ac
}

// ClassifySymbol determines the asset class of a symbol
func (ac *AssetClassifier) ClassifySymbol(symbol string) models.AssetClass {
	symbol = strings.ToUpper(symbol)
	
	// Priority 1: Check cached mappings
	ac.mu.RLock()
	if ac.cryptoSymbols[symbol] {
		ac.mu.RUnlock()
		return models.AssetClassCrypto
	}
	if ac.forexPairs[symbol] {
		ac.mu.RUnlock()
		return models.AssetClassForex
	}
	if ac.stockSymbols[symbol] {
		ac.mu.RUnlock()
		return models.AssetClassStock
	}
	if ac.commoditySymbols[symbol] {
		ac.mu.RUnlock()
		return models.AssetClassCommodity
	}
	ac.mu.RUnlock()
	
	// Priority 2: Pattern-based detection
	
	// Crypto patterns
	if strings.HasSuffix(symbol, "USDT") || 
	   strings.HasSuffix(symbol, "BTC") || 
	   strings.HasSuffix(symbol, "ETH") ||
	   strings.HasSuffix(symbol, "BUSD") ||
	   strings.HasSuffix(symbol, "USDC") ||
	   strings.HasSuffix(symbol, "BNB") ||
	   strings.HasSuffix(symbol, "SOL") ||
	   strings.HasSuffix(symbol, "DOT") {
		ac.AddSymbol(symbol, models.AssetClassCrypto)
		return models.AssetClassCrypto
	}
	
	// Forex patterns (6 characters, all letters)
	forexPattern := regexp.MustCompile(`^[A-Z]{6}$`)
	if forexPattern.MatchString(symbol) {
		// Check if it's a valid forex pair
		base := symbol[:3]
		quote := symbol[3:]
		if ac.isForexCurrency(base) && ac.isForexCurrency(quote) {
			ac.AddSymbol(symbol, models.AssetClassForex)
			return models.AssetClassForex
		}
	}
	
	// Forex with separator (EUR/USD, GBP-USD, EUR_USD)
	forexWithSep := regexp.MustCompile(`^[A-Z]{3}[/\-_][A-Z]{3}$`)
	if forexWithSep.MatchString(symbol) {
		ac.AddSymbol(symbol, models.AssetClassForex)
		return models.AssetClassForex
	}
	
	// Stock patterns (usually have exchange suffix)
	if strings.Contains(symbol, ".") || strings.Contains(symbol, ":") {
		// Examples: AAPL.US, NASDAQ:AAPL
		ac.AddSymbol(symbol, models.AssetClassStock)
		return models.AssetClassStock
	}
	
	// Commodity patterns
	commodityPatterns := []string{"GOLD", "SILVER", "OIL", "GAS", "WHEAT", "CORN"}
	for _, pattern := range commodityPatterns {
		if strings.Contains(symbol, pattern) {
			ac.AddSymbol(symbol, models.AssetClassCommodity)
			return models.AssetClassCommodity
		}
	}
	
	// Index patterns
	indexPatterns := []string{"SPX", "NDX", "DJI", "VIX", "FTSE", "DAX", "NIFTY"}
	for _, pattern := range indexPatterns {
		if strings.Contains(symbol, pattern) {
			ac.AddSymbol(symbol, models.AssetClassIndex)
			return models.AssetClassIndex
		}
	}
	
	// Default to crypto for common 3-4 letter symbols
	if len(symbol) >= 3 && len(symbol) <= 5 && regexp.MustCompile(`^[A-Z]+$`).MatchString(symbol) {
		// Likely a stock ticker or crypto symbol
		// Default to crypto for now (can be overridden later)
		return models.AssetClassCrypto
	}
	
	return models.AssetClassUnknown
}

// AddSymbol adds a symbol to the classifier's cache
func (ac *AssetClassifier) AddSymbol(symbol string, class models.AssetClass) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	symbol = strings.ToUpper(symbol)
	
	switch class {
	case models.AssetClassCrypto:
		ac.cryptoSymbols[symbol] = true
	case models.AssetClassForex:
		ac.forexPairs[symbol] = true
	case models.AssetClassStock:
		ac.stockSymbols[symbol] = true
	case models.AssetClassCommodity:
		ac.commoditySymbols[symbol] = true
	}
}

// isForexCurrency checks if a 3-letter code is a valid forex currency
func (ac *AssetClassifier) isForexCurrency(code string) bool {
	forexCurrencies := map[string]bool{
		"USD": true, "EUR": true, "GBP": true, "JPY": true,
		"CHF": true, "CAD": true, "AUD": true, "NZD": true,
		"CNY": true, "INR": true, "KRW": true, "SGD": true,
		"HKD": true, "NOK": true, "SEK": true, "DKK": true,
		"PLN": true, "THB": true, "IDR": true, "HUF": true,
		"CZK": true, "ILS": true, "CLP": true, "PHP": true,
		"AED": true, "COP": true, "SAR": true, "MYR": true,
		"RON": true, "RUB": true, "ZAR": true, "MXN": true,
		"BRL": true, "TWD": true, "TRY": true,
	}
	return forexCurrencies[code]
}

// initializeKnownSymbols pre-populates known symbols
func (ac *AssetClassifier) initializeKnownSymbols() {
	// Common crypto symbols
	cryptos := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
		"ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
		"LINKUSDT", "LTCUSDT", "UNIUSDT", "ATOMUSDT", "XLMUSDT",
		"VETUSDT", "FILUSDT", "ICPUSDT", "ETCUSDT", "SHIBUSDT",
	}
	for _, symbol := range cryptos {
		ac.cryptoSymbols[symbol] = true
	}
	
	// Major forex pairs
	forexPairs := []string{
		"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD",
		"USDCAD", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
		"EURCHF", "AUDJPY", "GBPCHF", "EURAUD", "EURCAD",
	}
	for _, pair := range forexPairs {
		ac.forexPairs[pair] = true
	}
	
	// Common stocks
	stocks := []string{
		"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
		"FB", "NVDA", "JPM", "V", "JNJ",
		"WMT", "PG", "UNH", "DIS", "MA",
		"HD", "BAC", "PYPL", "NFLX", "ADBE",
		// With exchange suffixes
		"AAPL.US", "GOOGL.NASDAQ", "MSFT.US",
	}
	for _, stock := range stocks {
		ac.stockSymbols[stock] = true
	}
	
	// Commodities
	commodities := []string{
		"GOLD", "SILVER", "WTI", "BRENT", "COPPER",
		"PLATINUM", "PALLADIUM", "WHEAT", "CORN", "SOYBEANS",
		"NATURALGAS", "COFFEE", "SUGAR", "COTTON",
	}
	for _, commodity := range commodities {
		ac.commoditySymbols[commodity] = true
	}
}

// NormalizeSymbol converts various symbol formats to a standard format
func (ac *AssetClassifier) NormalizeSymbol(symbol string, assetClass models.AssetClass) string {
	symbol = strings.ToUpper(symbol)
	
	switch assetClass {
	case models.AssetClassForex:
		// Remove separators from forex pairs
		symbol = strings.ReplaceAll(symbol, "/", "")
		symbol = strings.ReplaceAll(symbol, "-", "")
		symbol = strings.ReplaceAll(symbol, "_", "")
		// Ensure 6 characters
		if len(symbol) == 6 {
			return symbol
		}
		
	case models.AssetClassStock:
		// Remove exchange suffixes for normalization
		if idx := strings.Index(symbol, "."); idx > 0 {
			return symbol[:idx]
		}
		if idx := strings.Index(symbol, ":"); idx > 0 {
			return symbol[idx+1:]
		}
		
	case models.AssetClassCrypto:
		// Remove quote currency if present
		if strings.HasSuffix(symbol, "USDT") {
			return strings.TrimSuffix(symbol, "USDT")
		}
		if strings.HasSuffix(symbol, "BUSD") {
			return strings.TrimSuffix(symbol, "BUSD")
		}
		if strings.HasSuffix(symbol, "USDC") {
			return strings.TrimSuffix(symbol, "USDC")
		}
	}
	
	return symbol
}

// GetAssetInfo returns detailed information about an asset
func (ac *AssetClassifier) GetAssetInfo(symbol string) models.AssetInfo {
	assetClass := ac.ClassifySymbol(symbol)
	normalized := ac.NormalizeSymbol(symbol, assetClass)
	
	info := models.AssetInfo{
		Symbol:           symbol,
		NormalizedSymbol: normalized,
		AssetClass:       assetClass,
	}
	
	// Add descriptions based on asset class
	switch assetClass {
	case models.AssetClassCrypto:
		info.Exchange = "Cryptocurrency"
		info.Description = "Digital Asset"
	case models.AssetClassForex:
		info.Exchange = "Foreign Exchange"
		info.Description = "Currency Pair"
	case models.AssetClassStock:
		info.Exchange = "Stock Market"
		info.Description = "Equity"
	case models.AssetClassCommodity:
		info.Exchange = "Commodities Market"
		info.Description = "Raw Material"
	case models.AssetClassIndex:
		info.Exchange = "Index"
		info.Description = "Market Index"
	}
	
	return info
}