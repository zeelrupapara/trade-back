package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/services"
)

// HistoricalHandler handles historical data API requests
type HistoricalHandler struct {
	loader *services.HistoricalLoader
	mysql  *database.MySQLClient
	logger *logrus.Entry
}

// NewHistoricalHandler creates a new historical data handler
func NewHistoricalHandler(
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	logger *logrus.Logger,
) *HistoricalHandler {
	return &HistoricalHandler{
		loader: services.NewHistoricalLoader(influx, mysql, logger),
		mysql:  mysql,
		logger: logger.WithField("component", "historical-api"),
	}
}

// BackfillSymbolRequest represents a request to backfill a single symbol
type BackfillSymbolRequest struct {
	Symbol   string `json:"symbol"`
	Interval string `json:"interval"`
	Days     int    `json:"days"`
}

// BackfillAllRequest represents a request to backfill all symbols
type BackfillAllRequest struct {
	Interval string `json:"interval"`
	Days     int    `json:"days"`
}

// BackfillResponse represents the response for backfill operations
type BackfillResponse struct {
	Status    string    `json:"status"`
	Message   string    `json:"message"`
	Symbol    string    `json:"symbol,omitempty"`
	Symbols   []string  `json:"symbols,omitempty"`
	Interval  string    `json:"interval"`
	Days      int       `json:"days"`
	StartTime time.Time `json:"start_time"`
	TaskID    string    `json:"task_id,omitempty"`
}

// BackfillSymbol handles POST /api/v1/historical/backfill/symbol
func (h *HistoricalHandler) BackfillSymbol(w http.ResponseWriter, r *http.Request) {
	var req BackfillSymbolRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	
	// Validate required fields
	if req.Symbol == "" || req.Interval == "" || req.Days < 1 || req.Days > 1500 {
		h.writeError(w, http.StatusBadRequest, "Invalid parameters")
		return
	}

	// Validate interval
	if !h.isValidInterval(req.Interval) {
		h.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid interval",
			"valid_intervals": []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"},
		})
		return
	}

	// Start backfill in background
	taskID := h.generateTaskID()
	startTime := time.Now()

	go func() {
		ctx := context.Background()
		h.logger.WithFields(logrus.Fields{
			"symbol":   req.Symbol,
			"interval": req.Interval,
			"days":     req.Days,
			"task_id":  taskID,
		}).Info("Starting symbol backfill via API")

		if err := h.loader.LoadHistoricalData(ctx, req.Symbol, req.Interval, req.Days); err != nil {
			h.logger.WithError(err).WithField("task_id", taskID).Error("Symbol backfill failed")
			// In production, you would update task status in database
		} else {
			h.logger.WithField("task_id", taskID).Info("Symbol backfill completed")
		}
	}()

	h.writeJSON(w, http.StatusAccepted, BackfillResponse{
		Status:    "accepted",
		Message:   "Backfill started",
		Symbol:    req.Symbol,
		Interval:  req.Interval,
		Days:      req.Days,
		StartTime: startTime,
		TaskID:    taskID,
	})
}

// BackfillAll handles POST /api/v1/historical/backfill/all
func (h *HistoricalHandler) BackfillAll(w http.ResponseWriter, r *http.Request) {
	var req BackfillAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	
	// Validate required fields
	if req.Interval == "" || req.Days < 1 || req.Days > 1500 {
		h.writeError(w, http.StatusBadRequest, "Invalid parameters")
		return
	}

	// Validate interval
	if !h.isValidInterval(req.Interval) {
		h.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid interval",
			"valid_intervals": []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"},
		})
		return
	}

	// Get active symbols
	symbols, err := h.mysql.GetSymbols(context.Background())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to get symbols")
		return
	}

	activeSymbols := make([]string, 0)
	for _, sym := range symbols {
		if sym.IsActive {
			activeSymbols = append(activeSymbols, sym.Symbol)
		}
	}

	// Start backfill in background
	taskID := h.generateTaskID()
	startTime := time.Now()

	go func() {
		ctx := context.Background()
		h.logger.WithFields(logrus.Fields{
			"symbols":  len(activeSymbols),
			"interval": req.Interval,
			"days":     req.Days,
			"task_id":  taskID,
		}).Info("Starting all symbols backfill via API")

		if err := h.loader.LoadAllSymbols(ctx, req.Interval, req.Days); err != nil {
			h.logger.WithError(err).WithField("task_id", taskID).Error("All symbols backfill failed")
		} else {
			h.logger.WithField("task_id", taskID).Info("All symbols backfill completed")
		}
	}()

	h.writeJSON(w, http.StatusAccepted, BackfillResponse{
		Status:    "accepted",
		Message:   "Backfill started for all active symbols",
		Symbols:   activeSymbols,
		Interval:  req.Interval,
		Days:      req.Days,
		StartTime: startTime,
		TaskID:    taskID,
	})
}

// CalculateATHATL handles POST /api/v1/historical/ath-atl
func (h *HistoricalHandler) CalculateATHATL(w http.ResponseWriter, r *http.Request) {
	taskID := h.generateTaskID()
	startTime := time.Now()

	go func() {
		ctx := context.Background()
		h.logger.WithField("task_id", taskID).Info("Starting ATH/ATL calculation via API")

		if err := h.loader.LoadATHATL(ctx); err != nil {
			h.logger.WithError(err).WithField("task_id", taskID).Error("ATH/ATL calculation failed")
		} else {
			h.logger.WithField("task_id", taskID).Info("ATH/ATL calculation completed")
		}
	}()

	h.writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":     "accepted",
		"message":    "ATH/ATL calculation started",
		"task_id":    taskID,
		"start_time": startTime,
	})
}

// GetBackfillStatus handles GET /api/v1/historical/status/:taskId
func (h *HistoricalHandler) GetBackfillStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["taskId"]
	
	// In a production system, you would query the task status from a database
	// For now, we'll return a mock response
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"status":  "completed", // or "running", "failed"
		"message": "Task status tracking not implemented in this version",
	})
}

// GetHistoricalDataInfo handles GET /api/v1/historical/info
func (h *HistoricalHandler) GetHistoricalDataInfo(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	
	if symbol == "" {
		// Return general info
		h.writeJSON(w, http.StatusOK, map[string]interface{}{
			"message": "Historical data API",
			"endpoints": map[string]string{
				"backfill_symbol": "POST /api/v1/historical/backfill/symbol",
				"backfill_all":    "POST /api/v1/historical/backfill/all",
				"calculate_ath":   "POST /api/v1/historical/ath-atl",
				"status":          "GET /api/v1/historical/status/:taskId",
			},
			"supported_intervals": []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"},
			"max_days": 1500,
		})
		return
	}

	// Return info for specific symbol
	// In production, you would query InfluxDB for actual data range
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"symbol": symbol,
		"message": "Symbol-specific data range info not implemented in this version",
		"hint": "Use InfluxDB queries to check available data range",
	})
}

// isValidInterval checks if the interval is valid
func (h *HistoricalHandler) isValidInterval(interval string) bool {
	validIntervals := []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
	for _, v := range validIntervals {
		if v == interval {
			return true
		}
	}
	return false
}

// generateTaskID generates a unique task ID
func (h *HistoricalHandler) generateTaskID() string {
	return "task_" + strconv.FormatInt(time.Now().UnixNano(), 36) + "_" + h.generateRandomString(6)
}

// generateRandomString generates a random string of given length
func (h *HistoricalHandler) generateRandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

// RegisterRoutes registers all historical data routes
func (h *HistoricalHandler) RegisterRoutes(router *mux.Router) {
	// Historical data endpoints
	historical := router.PathPrefix("/api/v1/historical").Subrouter()
	historical.HandleFunc("/backfill/symbol", h.BackfillSymbol).Methods("POST")
	historical.HandleFunc("/backfill/all", h.BackfillAll).Methods("POST")
	historical.HandleFunc("/ath-atl", h.CalculateATHATL).Methods("POST")
	historical.HandleFunc("/status/{taskId}", h.GetBackfillStatus).Methods("GET")
	historical.HandleFunc("/info", h.GetHistoricalDataInfo).Methods("GET")
}

// Helper methods for HTTP responses
func (h *HistoricalHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *HistoricalHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}