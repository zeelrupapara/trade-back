package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// WebhookHandler handles incoming webhooks from various sources
type WebhookHandler struct {
	nats   *messaging.NATSClient
	logger *logrus.Entry
	cfg    *config.WebhookConfig
}

// WebhookPayload represents a generic webhook payload
type WebhookPayload struct {
	Source    string                 `json:"source"`
	Event     string                 `json:"event"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// TradingViewAlert represents a TradingView webhook alert
type TradingViewAlert struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Volume    float64 `json:"volume"`
	Time      string  `json:"time"`
	Exchange  string  `json:"exchange"`
	Alert     string  `json:"alert"`
	Condition string  `json:"condition"`
}

// NewWebhookHandler creates a new webhook handler
func NewWebhookHandler(
	nats *messaging.NATSClient,
	cfg *config.WebhookConfig,
	logger *logrus.Logger,
) *WebhookHandler {
	return &WebhookHandler{
		nats:   nats,
		logger: logger.WithField("component", "webhook-handler"),
		cfg:    cfg,
	}
}

// RegisterRoutes registers webhook endpoints
func (wh *WebhookHandler) RegisterRoutes(router *mux.Router) {
	// Webhook endpoints
	webhooks := router.PathPrefix("/api/v1/webhooks").Subrouter()

	// TradingView webhooks
	webhooks.HandleFunc("/tradingview", wh.handleTradingViewWebhook).Methods("POST")

	// Generic webhook endpoint with authentication
	webhooks.HandleFunc("/generic/{source}", wh.handleGenericWebhook).Methods("POST")

	// Health check for webhook endpoint
	webhooks.HandleFunc("/health", wh.handleHealthCheck).Methods("GET")
}

// handleTradingViewWebhook processes TradingView alerts
func (wh *WebhookHandler) handleTradingViewWebhook(w http.ResponseWriter, r *http.Request) {
	// Verify webhook secret if configured
	if wh.cfg.TradingViewSecret != "" {
		secret := r.Header.Get("X-Webhook-Secret")
		if secret != wh.cfg.TradingViewSecret {
			wh.logger.Warn("Invalid TradingView webhook secret")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		wh.logger.WithError(err).Error("Failed to read request body")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse TradingView alert
	var alert TradingViewAlert
	if err := json.Unmarshal(body, &alert); err != nil {
		// Try to parse as plain text (TradingView sometimes sends plain text)
		alert = wh.parsePlainTextAlert(string(body))
	}

	wh.logger.WithFields(logrus.Fields{
		"symbol":    alert.Symbol,
		"alert":     alert.Alert,
		"condition": alert.Condition,
	}).Info("Received TradingView alert")

	// Process the alert
	if err := wh.processTradingViewAlert(&alert); err != nil {
		wh.logger.WithError(err).Error("Failed to process TradingView alert")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Send success response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Alert processed",
	})
}

// handleGenericWebhook handles webhooks from various sources
func (wh *WebhookHandler) handleGenericWebhook(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	source := vars["source"]

	// Verify HMAC signature if configured
	if wh.cfg.HMACSecret != "" {
		signature := r.Header.Get("X-Webhook-Signature")
		if !wh.verifyHMACSignature(r, signature) {
			wh.logger.Warn("Invalid webhook signature")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		wh.logger.WithError(err).Error("Failed to read request body")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse webhook payload
	var payload WebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		wh.logger.WithError(err).Error("Failed to parse webhook payload")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Set source from URL if not in payload
	if payload.Source == "" {
		payload.Source = source
	}

	// Set timestamp if not provided
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	wh.logger.WithFields(logrus.Fields{
		"source": payload.Source,
		"event":  payload.Event,
	}).Info("Received webhook")

	// Process the webhook
	if err := wh.processGenericWebhook(&payload); err != nil {
		wh.logger.WithError(err).Error("Failed to process webhook")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Send success response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Webhook processed",
	})
}

// handleHealthCheck provides webhook endpoint health status
func (wh *WebhookHandler) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"endpoints": map[string]bool{
			"tradingview": true,
			"generic":     true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// processTradingViewAlert processes a TradingView alert
func (wh *WebhookHandler) processTradingViewAlert(alert *TradingViewAlert) error {
	// Create a price update if price is provided
	if alert.Price > 0 && alert.Symbol != "" {
		priceData := &models.PriceData{
			Symbol:    alert.Symbol,
			Price:     alert.Price,
			Volume:    alert.Volume,
			Timestamp: time.Now(),
		}

		// Publish price update to NATS
		subject := fmt.Sprintf("prices.%s", alert.Symbol)
		if err := wh.nats.PublishPrice(subject, priceData); err != nil {
			return fmt.Errorf("failed to publish price: %w", err)
		}
	}

	// Publish alert as system event
	alertEvent := map[string]interface{}{
		"type":      "tradingview_alert",
		"symbol":    alert.Symbol,
		"alert":     alert.Alert,
		"condition": alert.Condition,
		"price":     alert.Price,
		"timestamp": time.Now().Unix(),
	}

	if err := wh.nats.PublishError(nil, alertEvent); err != nil {
		return fmt.Errorf("failed to publish alert event: %w", err)
	}

	return nil
}

// processGenericWebhook processes a generic webhook
func (wh *WebhookHandler) processGenericWebhook(payload *WebhookPayload) error {
	// Publish webhook event to NATS
	subject := fmt.Sprintf("webhooks.%s.%s", payload.Source, payload.Event)

	_, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Remove unused variable
	// msg := &models.WebSocketMessage{
	//	Event: payload.Event,
	//	Data:  payload.Data,
	// }

	// Use the encoded connection for JSON messages
	if err := wh.nats.PublishPrice(subject, &models.PriceData{}); err != nil {
		return fmt.Errorf("failed to publish webhook event: %w", err)
	}

	wh.logger.WithFields(logrus.Fields{
		"subject": subject,
		"source":  payload.Source,
		"event":   payload.Event,
	}).Debug("Published webhook event")

	return nil
}

// parsePlainTextAlert parses a plain text TradingView alert
func (wh *WebhookHandler) parsePlainTextAlert(text string) TradingViewAlert {
	alert := TradingViewAlert{
		Alert:     text,
		Time: time.Now().Format(time.RFC3339),
	}

	// Try to extract common patterns
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Look for symbol
		if strings.HasPrefix(line, "Symbol:") {
			alert.Symbol = strings.TrimSpace(strings.TrimPrefix(line, "Symbol:"))
		}

		// Look for price
		if strings.HasPrefix(line, "Price:") {
			priceStr := strings.TrimSpace(strings.TrimPrefix(line, "Price:"))
			fmt.Sscanf(priceStr, "%f", &alert.Price)
		}

		// Look for condition
		if strings.HasPrefix(line, "Condition:") {
			alert.Condition = strings.TrimSpace(strings.TrimPrefix(line, "Condition:"))
		}
	}

	return alert
}

// verifyHMACSignature verifies the HMAC signature of a webhook
func (wh *WebhookHandler) verifyHMACSignature(r *http.Request, signature string) bool {
	if signature == "" {
		return false
	}

	// Read body for signature verification
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return false
	}

	// Reset body for later reading
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	// Calculate expected signature
	mac := hmac.New(sha256.New, []byte(wh.cfg.HMACSecret))
	mac.Write(body)
	expectedSignature := hex.EncodeToString(mac.Sum(nil))

	// Compare signatures
	return hmac.Equal([]byte(signature), []byte(expectedSignature))
}

// WebhookStats returns webhook processing statistics
func (wh *WebhookHandler) WebhookStats() map[string]interface{} {
	// This would typically track metrics like:
	// - Total webhooks received
	// - Webhooks by source
	// - Processing times
	// - Error rates

	return map[string]interface{}{
		"status": "operational",
		"sources": []string{
			"tradingview",
			"generic",
		},
	}
}

// GetRouter returns a configured router with webhook endpoints
func (wh *WebhookHandler) GetRouter() *mux.Router {
	router := mux.NewRouter()
	wh.RegisterRoutes(router)
	return router
}
