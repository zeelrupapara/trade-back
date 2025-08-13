package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/pkg/models"
)

// PeriodLevelsHandler handles period-based level API requests
type PeriodLevelsHandler struct {
	periodService *services.PeriodExtremesService
	classifier    *services.AssetClassifier
	redis         *cache.RedisClient
	logger        *logrus.Entry
}

// NewPeriodLevelsHandler creates a new period levels handler
func NewPeriodLevelsHandler(
	periodService *services.PeriodExtremesService,
	redis *cache.RedisClient,
	logger *logrus.Logger,
) *PeriodLevelsHandler {
	return &PeriodLevelsHandler{
		periodService: periodService,
		classifier:    services.NewAssetClassifier(),
		redis:         redis,
		logger:        logger.WithField("component", "period-levels-api"),
	}
}

// RegisterRoutes registers period levels API routes
func (h *PeriodLevelsHandler) RegisterRoutes(router *mux.Router) {
	levels := router.PathPrefix("/api/v1/levels").Subrouter()
	
	// Get specific period levels for a symbol
	levels.HandleFunc("/{symbol}/daily", h.GetDailyLevels).Methods("GET")
	levels.HandleFunc("/{symbol}/weekly", h.GetWeeklyLevels).Methods("GET")
	levels.HandleFunc("/{symbol}/monthly", h.GetMonthlyLevels).Methods("GET")
	levels.HandleFunc("/{symbol}/yearly", h.GetYearlyLevels).Methods("GET")
	
	// Get all period levels for a symbol
	levels.HandleFunc("/{symbol}/all", h.GetAllPeriodLevels).Methods("GET")
	levels.HandleFunc("/{symbol}", h.GetAllPeriodLevels).Methods("GET") // Alias
	
	// Batch request for multiple symbols
	levels.HandleFunc("/batch", h.GetBatchLevels).Methods("POST")
	
	// Get period statistics
	levels.HandleFunc("/{symbol}/stats", h.GetPeriodStats).Methods("GET")
	
	// Get near-level alerts
	levels.HandleFunc("/{symbol}/alerts", h.GetNearLevelAlerts).Methods("GET")
}

// GetDailyLevels returns daily high/low levels for a symbol
func (h *PeriodLevelsHandler) GetDailyLevels(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	level, err := h.periodService.GetPeriodLevels(r.Context(), symbol, models.PeriodDaily)
	if err != nil {
		h.sendError(w, "Failed to get daily levels", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, level)
}

// GetWeeklyLevels returns weekly high/low levels for a symbol
func (h *PeriodLevelsHandler) GetWeeklyLevels(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	level, err := h.periodService.GetPeriodLevels(r.Context(), symbol, models.PeriodWeekly)
	if err != nil {
		h.sendError(w, "Failed to get weekly levels", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, level)
}

// GetMonthlyLevels returns monthly high/low levels for a symbol
func (h *PeriodLevelsHandler) GetMonthlyLevels(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	level, err := h.periodService.GetPeriodLevels(r.Context(), symbol, models.PeriodMonthly)
	if err != nil {
		h.sendError(w, "Failed to get monthly levels", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, level)
}

// GetYearlyLevels returns yearly high/low levels for a symbol
func (h *PeriodLevelsHandler) GetYearlyLevels(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	level, err := h.periodService.GetPeriodLevels(r.Context(), symbol, models.PeriodYearly)
	if err != nil {
		h.sendError(w, "Failed to get yearly levels", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, level)
}

// GetAllPeriodLevels returns all period levels for a symbol
func (h *PeriodLevelsHandler) GetAllPeriodLevels(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	levels, err := h.periodService.GetAllPeriodLevels(r.Context(), symbol)
	if err != nil {
		h.sendError(w, "Failed to get period levels", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, levels)
}

// GetBatchLevels returns levels for multiple symbols
func (h *PeriodLevelsHandler) GetBatchLevels(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Symbols []string           `json:"symbols"`
		Period  models.PeriodType  `json:"period,omitempty"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		h.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	
	if len(request.Symbols) == 0 {
		h.sendError(w, "No symbols provided", http.StatusBadRequest)
		return
	}
	
	// Default to daily if no period specified
	if request.Period == "" {
		request.Period = models.PeriodDaily
	}
	
	// Convert symbols to uppercase
	for i := range request.Symbols {
		request.Symbols[i] = strings.ToUpper(request.Symbols[i])
	}
	
	results, err := h.periodService.GetMultipleSymbolLevels(r.Context(), request.Symbols, request.Period)
	if err != nil {
		h.sendError(w, "Failed to get batch levels", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, results)
}

// GetPeriodStats returns statistics for period levels
func (h *PeriodLevelsHandler) GetPeriodStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	// Get period from query parameter, default to daily
	period := models.PeriodType(r.URL.Query().Get("period"))
	if period == "" {
		period = models.PeriodDaily
	}
	
	level, err := h.periodService.GetPeriodLevels(r.Context(), symbol, period)
	if err != nil {
		h.sendError(w, "Failed to get period levels", http.StatusInternalServerError)
		return
	}
	
	// Calculate statistics
	stats := models.PeriodStats{
		Symbol:       symbol,
		Period:       period,
		High:         level.High,
		Low:          level.Low,
		Range:        level.High - level.Low,
		RangePercent: ((level.High - level.Low) / level.Low) * 100,
	}
	
	// Get current price if available
	priceStr := r.URL.Query().Get("price")
	if priceStr != "" {
		var price float64
		if _, err := json.Marshal(priceStr); err == nil {
			stats.CurrentPrice = price
			stats.PositionInRange = level.GetPositionInRange(price)
		}
	}
	
	h.sendJSON(w, stats)
}

// GetNearLevelAlerts returns alerts for prices near Fibonacci levels
func (h *PeriodLevelsHandler) GetNearLevelAlerts(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	// Get current price from query parameter
	priceStr := r.URL.Query().Get("price")
	if priceStr == "" {
		h.sendError(w, "Price parameter required", http.StatusBadRequest)
		return
	}
	
	var currentPrice float64
	if _, err := json.Marshal(priceStr); err != nil {
		h.sendError(w, "Invalid price parameter", http.StatusBadRequest)
		return
	}
	
	// Get tolerance from query parameter (default 0.5%)
	toleranceStr := r.URL.Query().Get("tolerance")
	tolerance := currentPrice * 0.005 // 0.5% default
	if toleranceStr != "" {
		var tolerancePercent float64
		if _, err := json.Marshal(toleranceStr); err == nil {
			tolerance = currentPrice * (tolerancePercent / 100)
		}
	}
	
	// Check all periods for near levels
	periods := []models.PeriodType{
		models.PeriodDaily,
		models.PeriodWeekly,
		models.PeriodMonthly,
	}
	
	type Alert struct {
		Period    models.PeriodType `json:"period"`
		Level     string            `json:"level"`
		LevelValue float64          `json:"level_value"`
		Distance  float64           `json:"distance"`
	}
	
	alerts := make([]Alert, 0)
	
	for _, period := range periods {
		level, err := h.periodService.GetPeriodLevels(r.Context(), symbol, period)
		if err != nil {
			continue
		}
		
		if isNear, levelName := level.IsNearHighLow(currentPrice, tolerance); isNear {
			levelValue := 0.0
			switch levelName {
			case "high":
				levelValue = level.High
			case "low":
				levelValue = level.Low
			}
			
			alerts = append(alerts, Alert{
				Period:     period,
				Level:      levelName,
				LevelValue: levelValue,
				Distance:   currentPrice - levelValue,
			})
		}
	}
	
	response := map[string]interface{}{
		"symbol":        symbol,
		"current_price": currentPrice,
		"tolerance":     tolerance,
		"alerts":        alerts,
	}
	
	h.sendJSON(w, response)
}

// sendJSON sends JSON response
func (h *PeriodLevelsHandler) sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// sendError sends error response
func (h *PeriodLevelsHandler) sendError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}