package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/indicator/enigma"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/pkg/models"
)

// EnigmaHandler handles Enigma-related API requests
type EnigmaHandler struct {
	calculator     *enigma.Calculator
	extremeTracker *services.UnifiedExtremeTracker
	classifier     *services.AssetClassifier
	redis          *cache.RedisClient
	logger         *logrus.Entry
}

// NewEnigmaHandler creates a new Enigma handler
func NewEnigmaHandler(
	calculator *enigma.Calculator,
	extremeTracker *services.UnifiedExtremeTracker,
	redis *cache.RedisClient,
	logger *logrus.Logger,
) *EnigmaHandler {
	return &EnigmaHandler{
		calculator:     calculator,
		extremeTracker: extremeTracker,
		classifier:     services.NewAssetClassifier(),
		redis:          redis,
		logger:         logger.WithField("component", "enigma-api"),
	}
}

// RegisterRoutes registers Enigma API routes
func (h *EnigmaHandler) RegisterRoutes(router *mux.Router) {
	enigma := router.PathPrefix("/api/v1/enigma").Subrouter()
	
	// Get Enigma levels for a symbol
	enigma.HandleFunc("/{symbol}", h.GetEnigmaLevels).Methods("GET")
	
	// Get extremes for a symbol
	enigma.HandleFunc("/{symbol}/extremes", h.GetExtremes).Methods("GET")
	
	// Batch request for multiple symbols
	enigma.HandleFunc("/batch", h.GetBatchEnigma).Methods("POST")
	
	// Get asset info
	enigma.HandleFunc("/{symbol}/info", h.GetAssetInfo).Methods("GET")
	
	// Calculate custom Fibonacci
	enigma.HandleFunc("/calculate", h.CalculateCustom).Methods("POST")
}

// GetEnigmaLevels returns Enigma Fibonacci levels for a symbol
func (h *EnigmaHandler) GetEnigmaLevels(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	// Get Enigma levels from calculator
	enigmaData, err := h.calculator.GetEnigmaLevel(symbol)
	if err != nil {
		// Try to fetch extremes and calculate
		extreme, err := h.extremeTracker.GetExtremes(r.Context(), symbol)
		if err != nil {
			h.sendError(w, "Failed to get Enigma levels", http.StatusInternalServerError)
			return
		}
		
		// Calculate Fibonacci levels
		enigmaData = h.calculateEnigma(extreme)
	}
	
	response := map[string]interface{}{
		"symbol":     symbol,
		"asset_class": h.classifier.ClassifySymbol(symbol),
		"levels": map[string]float64{
			"0":    enigmaData.FibLevels.L0,
			"23.6": enigmaData.FibLevels.L236,
			"38.2": enigmaData.FibLevels.L382,
			"50":   enigmaData.FibLevels.L50,
			"61.8": enigmaData.FibLevels.L618,
			"78.6": enigmaData.FibLevels.L786,
			"100":  enigmaData.FibLevels.L100,
		},
		"current_level": enigmaData.Level,
		"ath":          enigmaData.ATH,
		"atl":          enigmaData.ATL,
		"timestamp":    enigmaData.Timestamp,
	}
	
	h.sendJSON(w, response)
}

// GetExtremes returns all-time extremes for a symbol
func (h *EnigmaHandler) GetExtremes(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	extreme, err := h.extremeTracker.GetExtremes(r.Context(), symbol)
	if err != nil {
		h.sendError(w, "Failed to get extremes", http.StatusInternalServerError)
		return
	}
	
	h.sendJSON(w, extreme)
}

// GetBatchEnigma returns Enigma levels for multiple symbols
func (h *EnigmaHandler) GetBatchEnigma(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Symbols []string `json:"symbols"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		h.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	
	results := make(map[string]interface{})
	
	for _, symbol := range request.Symbols {
		symbol = strings.ToUpper(symbol)
		
		// Get extremes
		extreme, err := h.extremeTracker.GetExtremes(r.Context(), symbol)
		if err != nil {
			results[symbol] = map[string]string{"error": err.Error()}
			continue
		}
		
		// Calculate Enigma
		enigmaData := h.calculateEnigma(extreme)
		
		results[symbol] = map[string]interface{}{
			"asset_class": extreme.AssetClass,
			"levels": map[string]float64{
				"0":    enigmaData.FibLevels.L0,
				"23.6": enigmaData.FibLevels.L236,
				"38.2": enigmaData.FibLevels.L382,
				"50":   enigmaData.FibLevels.L50,
				"61.8": enigmaData.FibLevels.L618,
				"78.6": enigmaData.FibLevels.L786,
				"100":  enigmaData.FibLevels.L100,
			},
			"ath": extreme.ATH,
			"atl": extreme.ATL,
		}
	}
	
	h.sendJSON(w, results)
}

// GetAssetInfo returns asset classification information
func (h *EnigmaHandler) GetAssetInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := strings.ToUpper(vars["symbol"])
	
	info := h.classifier.GetAssetInfo(symbol)
	h.sendJSON(w, info)
}

// CalculateCustom calculates Fibonacci levels with custom high/low
func (h *EnigmaHandler) CalculateCustom(w http.ResponseWriter, r *http.Request) {
	var request struct {
		High float64 `json:"high"`
		Low  float64 `json:"low"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		h.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	
	if request.High <= request.Low {
		h.sendError(w, "High must be greater than low", http.StatusBadRequest)
		return
	}
	
	// Calculate Fibonacci levels
	diff := request.High - request.Low
	levels := map[string]float64{
		"0":    request.Low,
		"23.6": request.Low + (diff * 0.236),
		"38.2": request.Low + (diff * 0.382),
		"50":   request.Low + (diff * 0.5),
		"61.8": request.Low + (diff * 0.618),
		"78.6": request.Low + (diff * 0.786),
		"100":  request.High,
	}
	
	response := map[string]interface{}{
		"high":   request.High,
		"low":    request.Low,
		"range":  diff,
		"levels": levels,
	}
	
	h.sendJSON(w, response)
}

// calculateEnigma calculates Enigma data from extremes
func (h *EnigmaHandler) calculateEnigma(extreme *models.AssetExtreme) *models.EnigmaData {
	diff := extreme.ATH - extreme.ATL
	
	return &models.EnigmaData{
		Symbol: extreme.Symbol,
		ATH:    extreme.ATH,
		ATL:    extreme.ATL,
		FibLevels: models.FibonacciLevels{
			L0:   extreme.ATL,
			L236: extreme.ATL + (diff * 0.236),
			L382: extreme.ATL + (diff * 0.382),
			L50:  extreme.ATL + (diff * 0.5),
			L618: extreme.ATL + (diff * 0.618),
			L786: extreme.ATL + (diff * 0.786),
			L100: extreme.ATH,
		},
		Timestamp: time.Now(),
	}
}

// sendJSON sends JSON response
func (h *EnigmaHandler) sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// sendError sends error response
func (h *EnigmaHandler) sendError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}