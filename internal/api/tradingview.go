package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/indicator/enigma"
	"github.com/trade-back/pkg/models"
)

// TradingViewAPI handles TradingView integration endpoints
type TradingViewAPI struct {
	influxDB   *database.InfluxClient
	mysqlDB    *database.MySQLClient
	redisCache *cache.RedisClient
	enigmaCalc *enigma.Calculator
	logger     *logrus.Entry
}

// NewTradingViewAPI creates a new TradingView API handler
func NewTradingViewAPI(
	influxDB *database.InfluxClient,
	mysqlDB *database.MySQLClient,
	redisCache *cache.RedisClient,
	enigmaCalc *enigma.Calculator,
	logger *logrus.Logger,
) *TradingViewAPI {
	return &TradingViewAPI{
		influxDB:   influxDB,
		mysqlDB:    mysqlDB,
		redisCache: redisCache,
		enigmaCalc: enigmaCalc,
		logger:     logger.WithField("component", "tradingview-api"),
	}
}

// RegisterRoutes registers TradingView API routes
func (tv *TradingViewAPI) RegisterRoutes(router *mux.Router) {
	// UDF endpoints for TradingView
	api := router.PathPrefix("/api/v1/tradingview").Subrouter()

	// Configuration
	api.HandleFunc("/config", tv.handleConfig).Methods("GET")

	// Symbol search
	api.HandleFunc("/search", tv.handleSearch).Methods("GET")

	// Symbol info
	api.HandleFunc("/symbols", tv.handleSymbolInfo).Methods("GET")

	// Historical data
	api.HandleFunc("/history", tv.handleHistory).Methods("GET")

	// Marks (for annotations)
	api.HandleFunc("/marks", tv.handleMarks).Methods("GET")

	// Time scale marks
	api.HandleFunc("/timescale_marks", tv.handleTimescaleMarks).Methods("GET")

	// Server time
	api.HandleFunc("/time", tv.handleTime).Methods("GET")

	// Custom indicators
	api.HandleFunc("/indicators/enigma/{symbol}", tv.handleEnigmaIndicator).Methods("GET")
}

// handleConfig returns TradingView configuration
func (tv *TradingViewAPI) handleConfig(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"supports_search":          true,
		"supports_group_request":   false,
		"supports_marks":           true,
		"supports_timescale_marks": true,
		"supports_time":            true,
		"exchanges": []map[string]string{
			{"value": "BINANCE", "name": "Binance", "desc": "Binance Exchange"},
		},
		"symbols_types": []map[string]string{
			{"name": "crypto", "value": "crypto"},
		},
		"supported_resolutions": []string{
			"1", "5", "15", "30", "60", "240", "D", "W", "M",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

// handleSearch handles symbol search requests
func (tv *TradingViewAPI) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	limit := r.URL.Query().Get("limit")
	exchange := r.URL.Query().Get("exchange")

	if query == "" {
		tv.sendError(w, "Query parameter is required", http.StatusBadRequest)
		return
	}

	limitInt := 50
	if limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			limitInt = l
		}
	}

	symbols, err := tv.searchSymbols(query, exchange, limitInt)
	if err != nil {
		tv.logger.WithError(err).Error("Failed to search symbols")
		tv.sendError(w, "Failed to search symbols", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(symbols)
}

// handleSymbolInfo returns detailed symbol information
func (tv *TradingViewAPI) handleSymbolInfo(w http.ResponseWriter, r *http.Request) {
	group := r.URL.Query().Get("group")
	if group == "" {
		tv.sendError(w, "Group parameter is required", http.StatusBadRequest)
		return
	}

	symbolInfo, err := tv.getSymbolInfo(group)
	if err != nil {
		tv.logger.WithError(err).Error("Failed to get symbol info")
		tv.sendError(w, "Failed to get symbol info", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"symbol":        symbolInfo.Symbol,
		"name":          symbolInfo.FullName,
		"description":   symbolInfo.FullName,
		"type":          "crypto",
		"session":       "24x7",
		"timezone":      "Etc/UTC",
		"ticker":        symbolInfo.Symbol,
		"exchange":      symbolInfo.Exchange,
		"minmov":        1,
		"pricescale":    100000000,
		"has_intraday":  true,
		"has_daily":     true,
		"has_weekly":    true,
		"has_monthly":   true,
		"currency_code": symbolInfo.QuoteCurrency,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleHistory returns historical bar data
func (tv *TradingViewAPI) handleHistory(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	resolution := r.URL.Query().Get("resolution")
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	if symbol == "" || resolution == "" || fromStr == "" || toStr == "" {
		tv.sendError(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	from, err := strconv.ParseInt(fromStr, 10, 64)
	if err != nil {
		tv.sendError(w, "Invalid from timestamp", http.StatusBadRequest)
		return
	}

	to, err := strconv.ParseInt(toStr, 10, 64)
	if err != nil {
		tv.sendError(w, "Invalid to timestamp", http.StatusBadRequest)
		return
	}

	// Convert resolution to duration
	duration := tv.resolutionToDuration(resolution)
	if duration == "" {
		tv.sendError(w, "Invalid resolution", http.StatusBadRequest)
		return
	}

	// Fetch bars from InfluxDB using the GetBars method with aggregation support
	ctx := r.Context()
	fromTime := time.Unix(from, 0)
	toTime := time.Unix(to, 0)
	
	// Convert resolution to interval format (e.g., "5" -> "5m", "60" -> "1h")
	interval := resolution
	switch resolution {
	case "1":
		interval = "1m"
	case "5":
		interval = "5m"
	case "15":
		interval = "15m"
	case "30":
		interval = "30m"
	case "60":
		interval = "1h"
	case "240":
		interval = "4h"
	case "D":
		interval = "1d"
	case "W":
		interval = "1w"
	}
	
	bars, err := tv.influxDB.GetBars(ctx, symbol, fromTime, toTime, interval)
	if err != nil {
		tv.logger.WithError(err).Error("Failed to query bars")
		tv.sendError(w, "Failed to fetch data", http.StatusInternalServerError)
		return
	}

	// bars is already []*models.Bar from GetBars, no conversion needed
	response := tv.barsToTradingViewFormat(bars)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMarks returns marks for the chart
func (tv *TradingViewAPI) handleMarks(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	resolution := r.URL.Query().Get("resolution")

	if symbol == "" || fromStr == "" || toStr == "" || resolution == "" {
		tv.sendError(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// For now, return empty marks
	// This can be extended to show trading signals, events, etc.
	marks := []interface{}{}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(marks)
}

// handleTimescaleMarks returns timescale marks
func (tv *TradingViewAPI) handleTimescaleMarks(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	resolution := r.URL.Query().Get("resolution")

	if symbol == "" || fromStr == "" || toStr == "" || resolution == "" {
		tv.sendError(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// Return empty timescale marks for now
	marks := []interface{}{}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(marks)
}

// handleTime returns server time
func (tv *TradingViewAPI) handleTime(w http.ResponseWriter, r *http.Request) {
	response := time.Now().Unix()

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%d", response)
}

// handleEnigmaIndicator returns Enigma indicator data
func (tv *TradingViewAPI) handleEnigmaIndicator(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]

	if symbol == "" {
		tv.sendError(w, "Symbol is required", http.StatusBadRequest)
		return
	}

	enigmaData, err := tv.enigmaCalc.GetEnigmaLevel(symbol)
	if err != nil {
		tv.logger.WithError(err).Error("Failed to get Enigma level")
		tv.sendError(w, "Failed to get Enigma level", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"symbol":     symbol,
		"level":      enigmaData.Level,
		"ath":        enigmaData.ATH,
		"atl":        enigmaData.ATL,
		"fib_levels": enigmaData.FibLevels,
		"timestamp":  enigmaData.Timestamp.Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Helper methods

// searchSymbols searches for symbols in the database
func (tv *TradingViewAPI) searchSymbols(query, exchange string, limit int) ([]map[string]interface{}, error) {
	whereClause := "WHERE is_active = 1 AND (symbol LIKE ? OR full_name LIKE ?)"
	args := []interface{}{"%" + query + "%", "%" + query + "%"}

	if exchange != "" {
		whereClause += " AND exchange = ?"
		args = append(args, exchange)
	}

	sqlQuery := fmt.Sprintf(`
		SELECT symbol, full_name, exchange, instrument_type
		FROM symbol_info
		%s
		LIMIT ?
	`, whereClause)

	args = append(args, limit)

	rows, err := tv.mysqlDB.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []map[string]interface{}{}
	for rows.Next() {
		var symbol, fullName, exchange, instrumentType string
		if err := rows.Scan(&symbol, &fullName, &exchange, &instrumentType); err != nil {
			tv.logger.WithError(err).Error("Failed to scan symbol")
			continue
		}

		results = append(results, map[string]interface{}{
			"symbol":      symbol,
			"full_name":   fullName,
			"description": fullName,
			"exchange":    exchange,
			"type":        instrumentType,
		})
	}

	return results, rows.Err()
}

// getSymbolInfo retrieves detailed symbol information
func (tv *TradingViewAPI) getSymbolInfo(symbol string) (*models.SymbolInfo, error) {
	var info models.SymbolInfo

	query := `
		SELECT id, exchange, symbol, full_name, instrument_type,
		       base_currency, quote_currency, is_active,
		       min_price_increment, min_quantity_increment,
		       created_at, updated_at
		FROM symbol_info
		WHERE symbol = ? AND is_active = 1
		LIMIT 1
	`

	rows, err := tv.mysqlDB.Query(query, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	if !rows.Next() {
		return nil, fmt.Errorf("symbol not found")
	}
	
	err = rows.Scan(
		&info.ID,
		&info.Exchange,
		&info.Symbol,
		&info.FullName,
		&info.InstrumentType,
		&info.BaseCurrency,
		&info.QuoteCurrency,
		&info.IsActive,
		&info.MinPriceIncrement,
		&info.MinQuantityIncrement,
		&info.CreatedAt,
		&info.UpdatedAt,
	)

	return &info, err
}

// resolutionToDuration converts TradingView resolution to InfluxDB duration
func (tv *TradingViewAPI) resolutionToDuration(resolution string) string {
	switch resolution {
	case "1":
		return "1m"
	case "5":
		return "5m"
	case "15":
		return "15m"
	case "30":
		return "30m"
	case "60":
		return "1h"
	case "240":
		return "4h"
	case "D", "1D":
		return "1d"
	case "W", "1W":
		return "1w"
	case "M", "1M":
		return "1M"
	default:
		return ""
	}
}

// barsToTradingViewFormat converts bars to TradingView format
func (tv *TradingViewAPI) barsToTradingViewFormat(bars []*models.Bar) map[string]interface{} {
	if len(bars) == 0 {
		return map[string]interface{}{
			"s": "no_data",
		}
	}

	times := make([]int64, len(bars))
	opens := make([]float64, len(bars))
	highs := make([]float64, len(bars))
	lows := make([]float64, len(bars))
	closes := make([]float64, len(bars))
	volumes := make([]float64, len(bars))

	for i, bar := range bars {
		times[i] = bar.Timestamp.Unix()
		opens[i] = bar.Open
		highs[i] = bar.High
		lows[i] = bar.Low
		closes[i] = bar.Close
		volumes[i] = bar.Volume
	}

	return map[string]interface{}{
		"s": "ok",
		"t": times,
		"o": opens,
		"h": highs,
		"l": lows,
		"c": closes,
		"v": volumes,
	}
}

// sendError sends an error response
func (tv *TradingViewAPI) sendError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"s":      "error",
		"errmsg": message,
	})
}

// GetRouter returns a configured router with all endpoints
func (tv *TradingViewAPI) GetRouter() *mux.Router {
	router := mux.NewRouter()
	tv.RegisterRoutes(router)
	return router
}
