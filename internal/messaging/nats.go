package messaging

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// NATSClient handles NATS messaging operations
type NATSClient struct {
	conn    *nats.Conn
	js      nats.JetStreamContext
	encoder *nats.EncodedConn
	logger  *logrus.Entry
	cfg     *config.NATSConfig

	// Subscriptions
	subs   map[string]*nats.Subscription
	subsMu sync.RWMutex
}

// NewNATSClient creates a new NATS client
func NewNATSClient(cfg *config.NATSConfig, logger *logrus.Logger) (*NATSClient, error) {
	opts := []nats.Option{
		nats.MaxReconnects(cfg.MaxReconnect),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.WithError(err).Warn("NATS disconnected")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected")
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Info("NATS connection closed")
		}),
	}

	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Create encoded connection for JSON
	encoder, err := nats.NewEncodedConn(conn, nats.JSON_ENCODER)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create encoded connection: %w", err)
	}

	nc := &NATSClient{
		conn:    conn,
		js:      js,
		encoder: encoder,
		logger:  logger.WithField("component", "nats"),
		cfg:     cfg,
		subs:    make(map[string]*nats.Subscription),
	}

	// Initialize streams
	if err := nc.initializeStreams(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize streams: %w", err)
	}

	return nc, nil
}

// Close closes the NATS connection
func (nc *NATSClient) Close() error {
	nc.subsMu.Lock()
	for _, sub := range nc.subs {
		sub.Unsubscribe()
	}
	nc.subs = make(map[string]*nats.Subscription)
	nc.subsMu.Unlock()

	nc.encoder.Close()
	nc.conn.Close()
	return nil
}

// IsConnected checks if NATS is connected
func (nc *NATSClient) IsConnected() bool {
	return nc.conn.IsConnected()
}

// initializeStreams creates JetStream streams
func (nc *NATSClient) initializeStreams() error {
	// Price stream for real-time price updates
	_, err := nc.js.AddStream(&nats.StreamConfig{
		Name:     "PRICES",
		Subjects: []string{"prices.>"},
		Storage:  nats.MemoryStorage,
		MaxAge:   24 * time.Hour,
		MaxMsgs:  1000000,
		Replicas: 1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("failed to create PRICES stream: %w", err)
	}

	// Enigma stream for technical indicators
	_, err = nc.js.AddStream(&nats.StreamConfig{
		Name:     "ENIGMA",
		Subjects: []string{"enigma.>"},
		Storage:  nats.FileStorage,
		MaxAge:   7 * 24 * time.Hour,
		MaxMsgs:  100000,
		Replicas: 1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("failed to create ENIGMA stream: %w", err)
	}

	// Sessions stream for trading session events
	_, err = nc.js.AddStream(&nats.StreamConfig{
		Name:     "SESSIONS",
		Subjects: []string{"sessions.>"},
		Storage:  nats.MemoryStorage,
		MaxAge:   24 * time.Hour,
		MaxMsgs:  10000,
		Replicas: 1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("failed to create SESSIONS stream: %w", err)
	}

	// System stream for health and monitoring
	_, err = nc.js.AddStream(&nats.StreamConfig{
		Name:     "SYSTEM",
		Subjects: []string{"system.>"},
		Storage:  nats.MemoryStorage,
		MaxAge:   1 * time.Hour,
		MaxMsgs:  10000,
		Replicas: 1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("failed to create SYSTEM stream: %w", err)
	}

	// Sync stream for historical data sync progress
	_, err = nc.js.AddStream(&nats.StreamConfig{
		Name:     "SYNC",
		Subjects: []string{"sync.>"},
		Storage:  nats.MemoryStorage,
		MaxAge:   1 * time.Hour,
		MaxMsgs:  10000,
		Replicas: 1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("failed to create SYNC stream: %w", err)
	}

	// Levels stream for period level updates (daily/weekly/monthly ATH/ATL)
	_, err = nc.js.AddStream(&nats.StreamConfig{
		Name:     "LEVELS",
		Subjects: []string{"levels.>"},
		Storage:  nats.MemoryStorage,
		MaxAge:   24 * time.Hour,
		MaxMsgs:  50000,
		Replicas: 1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("failed to create LEVELS stream: %w", err)
	}

	// nc.logger.Info("JetStream streams initialized")
	return nil
}

// Price operations

// PublishPrice publishes a price update
func (nc *NATSClient) PublishPrice(subject string, price *models.PriceData) error {
	data, err := json.Marshal(price)
	if err != nil {
		return fmt.Errorf("failed to marshal price: %w", err)
	}

	// Use PublishAsync for non-blocking publish with timeout
	future, err := nc.js.PublishAsync(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish price: %w", err)
	}

	// Wait for acknowledgment with timeout
	select {
	case <-future.Ok():
		return nil
	case err := <-future.Err():
		return fmt.Errorf("failed to publish price: %w", err)
	case <-time.After(2 * time.Second):
		return fmt.Errorf("publish timeout for subject %s", subject)
	}
}

// PublishPriceBatch publishes multiple price updates
func (nc *NATSClient) PublishPriceBatch(prices []*models.PriceData) error {
	for _, price := range prices {
		subject := fmt.Sprintf("prices.%s", price.Symbol)
		if err := nc.PublishPrice(subject, price); err != nil {
			return err
		}
	}
	return nil
}

// SubscribePrices subscribes to price updates
func (nc *NATSClient) SubscribePrices(handler func(*models.PriceData), symbols ...string) error {
	subject := "prices.>"
	if len(symbols) > 0 {
		// Subscribe to specific symbols
		for _, symbol := range symbols {
			subj := fmt.Sprintf("prices.%s", symbol)
			sub, err := nc.encoder.Subscribe(subj, func(price *models.PriceData) {
				handler(price)
			})
			if err != nil {
				return fmt.Errorf("failed to subscribe to %s: %w", subj, err)
			}

			nc.subsMu.Lock()
			nc.subs[subj] = sub
			nc.subsMu.Unlock()
		}
		return nil
	}

	// Subscribe to all prices
	sub, err := nc.encoder.Subscribe(subject, func(price *models.PriceData) {
		handler(price)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to prices: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// Enigma operations

// PublishEnigma publishes Enigma level update
func (nc *NATSClient) PublishEnigma(symbol string, enigma *models.EnigmaData) error {
	subject := fmt.Sprintf("enigma.%s", symbol)
	data, err := json.Marshal(enigma)
	if err != nil {
		return fmt.Errorf("failed to marshal enigma: %w", err)
	}
	_, err = nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish enigma: %w", err)
	}
	return nil
}

// SubscribeEnigma subscribes to Enigma updates
func (nc *NATSClient) SubscribeEnigma(handler func(*models.EnigmaData), symbols ...string) error {
	subject := "enigma.>"
	if len(symbols) > 0 {
		// Subscribe to specific symbols
		for _, symbol := range symbols {
			subj := fmt.Sprintf("enigma.%s", symbol)
			sub, err := nc.encoder.Subscribe(subj, func(enigma *models.EnigmaData) {
				handler(enigma)
			})
			if err != nil {
				return fmt.Errorf("failed to subscribe to %s: %w", subj, err)
			}

			nc.subsMu.Lock()
			nc.subs[subj] = sub
			nc.subsMu.Unlock()
		}
		return nil
	}

	// Subscribe to all enigma updates
	sub, err := nc.encoder.Subscribe(subject, func(enigma *models.EnigmaData) {
		handler(enigma)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to enigma: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// Session operations

// PublishSessionChange publishes trading session change
func (nc *NATSClient) PublishSessionChange(event *models.SessionChangeEvent) error {
	subject := "sessions.change"
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal session change: %w", err)
	}
	_, err = nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish session change: %w", err)
	}
	return nil
}

// SubscribeSessions subscribes to session events
func (nc *NATSClient) SubscribeSessions(handler func(*models.SessionChangeEvent)) error {
	subject := "sessions.>"

	sub, err := nc.encoder.Subscribe(subject, func(event *models.SessionChangeEvent) {
		handler(event)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to sessions: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// System operations

// PublishHealthStatus publishes health status
func (nc *NATSClient) PublishHealthStatus(status *models.HealthStatus) error {
	subject := "system.health"
	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal health status: %w", err)
	}
	_, err = nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish health status: %w", err)
	}
	return nil
}

// PublishError publishes system error
func (nc *NATSClient) PublishError(err error, context map[string]interface{}) error {
	subject := "system.errors"

	errorData := map[string]interface{}{
		"error":     err.Error(),
		"context":   context,
		"timestamp": time.Now().Unix(),
	}

	data, marshalErr := json.Marshal(errorData)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal error data: %w", marshalErr)
	}
	_, pubErr := nc.js.Publish(subject, data)
	if pubErr != nil {
		return fmt.Errorf("failed to publish error: %w", pubErr)
	}
	return nil
}

// Request-Reply operations

// RequestPrice requests current price for a symbol
func (nc *NATSClient) RequestPrice(symbol string, timeout time.Duration) (*models.PriceData, error) {
	subject := fmt.Sprintf("request.price.%s", symbol)

	var price models.PriceData
	err := nc.encoder.Request(subject, nil, &price, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to request price: %w", err)
	}

	return &price, nil
}

// RespondToRequests sets up responder for price requests
func (nc *NATSClient) RespondToRequests(getPriceFunc func(string) (*models.PriceData, error)) error {
	subject := "request.price.*"

	sub, err := nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		// Extract symbol from subject
		symbol := msg.Subject[len("request.price."):]

		price, err := getPriceFunc(symbol)
		if err != nil {
			nc.logger.WithError(err).WithField("symbol", symbol).Error("Failed to get price for request")
			return
		}

		data, err := json.Marshal(price)
		if err != nil {
			nc.logger.WithError(err).Error("Failed to marshal price response")
			return
		}

		msg.Respond(data)
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to requests: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// SubscribeRaw subscribes to a raw NATS subject with a simple byte handler
func (nc *NATSClient) SubscribeRaw(subject string, handler func([]byte)) error {
	sub, err := nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to subject %s: %w", subject, err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// Queue groups for load balancing

// SubscribeQueue subscribes with queue group for load balancing
func (nc *NATSClient) SubscribeQueue(subject, queue string, handler func([]byte)) error {
	sub, err := nc.conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to queue: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject+":"+queue] = sub
	nc.subsMu.Unlock()

	return nil
}

// Unsubscribe unsubscribes from a subject
func (nc *NATSClient) Unsubscribe(subject string) error {
	nc.subsMu.Lock()
	defer nc.subsMu.Unlock()

	if sub, exists := nc.subs[subject]; exists {
		if err := sub.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe: %w", err)
		}
		delete(nc.subs, subject)
	}

	return nil
}

// Drain drains the connection (graceful shutdown)
func (nc *NATSClient) Drain() error {
	return nc.conn.Drain()
}

// GetStats returns NATS connection statistics
func (nc *NATSClient) GetStats() nats.Statistics {
	return nc.conn.Stats()
}

// GetConn returns the underlying NATS connection
func (nc *NATSClient) GetConn() *nats.Conn {
	return nc.conn
}

// Sync operations

// PublishSyncProgress publishes sync progress update
func (nc *NATSClient) PublishSyncProgress(symbol string, progress int, totalBars int) error {
	subject := fmt.Sprintf("sync.progress.%s", symbol)
	data, err := json.Marshal(map[string]interface{}{
		"symbol":    symbol,
		"progress":  progress,
		"totalBars": totalBars,
		"timestamp": time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal sync progress: %w", err)
	}

	// Log for debugging
	nc.logger.WithFields(logrus.Fields{
		"subject":   subject,
		"symbol":    symbol,
		"progress":  progress,
		"totalBars": totalBars,
	}).Debug("Publishing sync progress to NATS")

	_, err = nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish sync progress: %w", err)
	}
	return nil
}

// PublishSyncComplete publishes sync completion notification
func (nc *NATSClient) PublishSyncComplete(symbol string, totalBars int, startTime, endTime time.Time) error {
	subject := fmt.Sprintf("sync.complete.%s", symbol)
	data, err := json.Marshal(map[string]interface{}{
		"symbol":    symbol,
		"totalBars": totalBars,
		"startTime": startTime,
		"endTime":   endTime,
		"timestamp": time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal sync complete: %w", err)
	}

	_, err = nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish sync complete: %w", err)
	}
	return nil
}

// SubscribeSyncUpdates subscribes to sync progress updates
func (nc *NATSClient) SubscribeSyncUpdates(handler func(string, int, int)) error {
	subject := "sync.progress.*"

	sub, err := nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			nc.logger.WithError(err).Error("Failed to unmarshal sync progress")
			return
		}

		symbol, _ := data["symbol"].(string)
		progress := int(data["progress"].(float64))
		totalBars := int(data["totalBars"].(float64))

		// Log for debugging
		nc.logger.WithFields(logrus.Fields{
			"subject":   msg.Subject,
			"symbol":    symbol,
			"progress":  progress,
			"totalBars": totalBars,
		}).Debug("Received sync progress from NATS")

		handler(symbol, progress, totalBars)
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to sync updates: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// SubscribeSyncComplete subscribes to sync completion notifications
func (nc *NATSClient) SubscribeSyncComplete(handler func(string, int, time.Time, time.Time)) error {
	subject := "sync.complete.*"

	sub, err := nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			nc.logger.WithError(err).Error("Failed to unmarshal sync complete")
			return
		}

		symbol, _ := data["symbol"].(string)
		totalBars := int(data["totalBars"].(float64))
		startTime, _ := time.Parse(time.RFC3339, data["startTime"].(string))
		endTime, _ := time.Parse(time.RFC3339, data["endTime"].(string))

		handler(symbol, totalBars, startTime, endTime)
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to sync complete: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}

// PublishSyncError publishes sync error notification
func (nc *NATSClient) PublishSyncError(symbol string, errorMsg string) error {
	subject := fmt.Sprintf("sync.error.%s", symbol)
	data, err := json.Marshal(map[string]interface{}{
		"symbol":    symbol,
		"error":     errorMsg,
		"timestamp": time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal sync error: %w", err)
	}

	_, err = nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish sync error: %w", err)
	}
	return nil
}

// PublishJSON publishes arbitrary JSON data to a subject
func (nc *NATSClient) PublishJSON(subject string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	
	_, err = nc.js.Publish(subject, jsonData)
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", subject, err)
	}
	
	return nil
}

// SubscribeSyncErrors subscribes to sync error notifications
func (nc *NATSClient) SubscribeSyncErrors(handler func(string, string)) error {
	subject := "sync.error.*"

	sub, err := nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			nc.logger.WithError(err).Error("Failed to unmarshal sync error")
			return
		}

		symbol, _ := data["symbol"].(string)
		errorMsg, _ := data["error"].(string)

		handler(symbol, errorMsg)
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to sync errors: %w", err)
	}

	nc.subsMu.Lock()
	nc.subs[subject] = sub
	nc.subsMu.Unlock()

	return nil
}
