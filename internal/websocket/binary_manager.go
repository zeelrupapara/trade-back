package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// BinaryManager handles binary WebSocket connections for ultra-fast data transmission
type BinaryManager struct {
	clients     map[*BinaryClient]bool
	register    chan *BinaryClient
	unregister  chan *BinaryClient
	broadcast   chan []byte
	batchQueue  chan *models.PriceData
	mu          sync.RWMutex
	
	// Performance optimization
	batchSize     int
	batchInterval time.Duration
	heartbeatInt  time.Duration
	
	// Subscriber count index for performance
	subscriberCount map[string]int
	subscriberMu    sync.RWMutex
	
	// Sync status cache for periodic broadcasting
	syncStatusCache map[string]*models.SyncStatus
	syncStatusMu    sync.RWMutex
	
	// Dependencies
	hub           *exchange.Hub
	nats          *messaging.NATSClient
	redis         *cache.RedisClient
	mysql         *database.MySQLClient
	logger        *logrus.Entry
}

// BinaryClient represents a WebSocket client with binary protocol support
type BinaryClient struct {
	id           string
	sessionToken string // Session token for market watch auto-subscription
	conn         *websocket.Conn
	send         chan []byte
	manager      *BinaryManager
	symbols      map[string]bool
	lastSeen     time.Time
	mu           sync.RWMutex
}

// NewBinaryManager creates a new binary WebSocket manager
func NewBinaryManager(
	hub *exchange.Hub,
	nats *messaging.NATSClient,
	cache *cache.RedisClient,
	mysql *database.MySQLClient,
	logger *logrus.Logger,
) *BinaryManager {
	bm := &BinaryManager{
		clients:         make(map[*BinaryClient]bool),
		register:        make(chan *BinaryClient),
		unregister:      make(chan *BinaryClient),
		broadcast:       make(chan []byte, 1000),
		batchQueue:      make(chan *models.PriceData, 10000),
		batchSize:       50,  // Batch up to 50 price updates
		batchInterval:   50 * time.Millisecond, // Send batch every 50ms
		heartbeatInt:    30 * time.Second,
		subscriberCount: make(map[string]int),
		syncStatusCache: make(map[string]*models.SyncStatus),
		hub:             hub,
		nats:            nats,
		redis:           cache,
		mysql:           mysql,
		logger:          logger.WithField("component", "ws-binary"),
	}
	
	// Subscribe to NATS updates
	if err := bm.subscribeToUpdates(); err != nil {
		logger.WithError(err).Error("Failed to subscribe to updates")
	}
	
	return bm
}

// Run starts the binary manager's main loop
func (bm *BinaryManager) Run(ctx context.Context) {
	ticker := time.NewTicker(bm.batchInterval)
	heartbeat := time.NewTicker(bm.heartbeatInt)
	syncStatusTicker := time.NewTicker(3 * time.Second) // Broadcast sync status every 3 seconds
	defer ticker.Stop()
	defer heartbeat.Stop()
	defer syncStatusTicker.Stop()
	
	var batchBuffer []*models.PriceData
	
	for {
		select {
		case <-ctx.Done():
			return
			
		case client := <-bm.register:
			bm.mu.Lock()
			bm.clients[client] = true
			bm.mu.Unlock()
			
		case client := <-bm.unregister:
			if _, ok := bm.clients[client]; ok {
				bm.mu.Lock()
				delete(bm.clients, client)
				bm.mu.Unlock()
				
				// Clean up subscriber counts
				client.mu.RLock()
				symbols := make([]string, 0, len(client.symbols))
				for symbol := range client.symbols {
					symbols = append(symbols, symbol)
				}
				client.mu.RUnlock()
				
				if len(symbols) > 0 {
					bm.subscriberMu.Lock()
					for _, symbol := range symbols {
						bm.subscriberCount[symbol]--
						if bm.subscriberCount[symbol] <= 0 {
							delete(bm.subscriberCount, symbol)
						}
					}
					bm.subscriberMu.Unlock()
				}
				
				close(client.send)
			}
			
		case price := <-bm.batchQueue:
			batchBuffer = append(batchBuffer, price)
			
			// Send batch if full
			if len(batchBuffer) >= bm.batchSize {
				bm.sendBatch(batchBuffer)
				batchBuffer = batchBuffer[:0] // Reset buffer
			}
			
		case <-ticker.C:
			// Send accumulated batch
			if len(batchBuffer) > 0 {
				bm.sendBatch(batchBuffer)
				batchBuffer = batchBuffer[:0]
			}
			
		case <-heartbeat.C:
			// Send heartbeat to all clients
			heartbeatData, _ := models.EncodeHeartbeat()
			bm.broadcastBinary(heartbeatData)
			
		case <-syncStatusTicker.C:
			// Broadcast sync status for all market watch symbols every 3 seconds
			bm.broadcastSyncStatus()
			
		case data := <-bm.broadcast:
			bm.broadcastBinary(data)
		}
	}
}

// sendBatch efficiently sends multiple price updates in one binary message
func (bm *BinaryManager) sendBatch(prices []*models.PriceData) {
	if len(prices) == 0 {
		return
	}
	
	// Group prices by symbol for targeted delivery
	pricesBySymbol := make(map[string][]*models.PriceData)
	for _, price := range prices {
		pricesBySymbol[price.Symbol] = append(pricesBySymbol[price.Symbol], price)
	}
	
	// Send to each client only the prices they're subscribed to
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		// Collect prices this client is subscribed to
		var clientPrices []*models.PriceData
		client.mu.RLock()
		for symbol := range client.symbols {
			if symbolPrices, exists := pricesBySymbol[symbol]; exists {
				clientPrices = append(clientPrices, symbolPrices...)
			}
		}
		client.mu.RUnlock()
		
		// Skip if client has no subscribed prices
		if len(clientPrices) == 0 {
			continue
		}
		
		// Encode and send batch to this client
		binaryData, err := models.BatchEncodePrices(clientPrices)
		if err != nil {
			bm.logger.WithError(err).WithField("client", client.id).Error("Failed to encode price batch")
			// Send error notification to client
			if errorData, encErr := models.EncodeError("Failed to encode price data"); encErr == nil {
				select {
				case client.send <- errorData:
				default:
					// Client buffer full
				}
			}
			continue
		}
		
		select {
		case client.send <- binaryData:
		default:
			// Client buffer full, disconnect
			close(client.send)
			delete(bm.clients, client)
		}
	}
}

// broadcastBinary sends binary data to all connected clients (used for heartbeat, etc)
func (bm *BinaryManager) broadcastBinary(data []byte) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		select {
		case client.send <- data:
		default:
			// Client buffer full, disconnect
			close(client.send)
			delete(bm.clients, client)
		}
	}
}

// AddPriceUpdate queues a price update for binary transmission
func (bm *BinaryManager) AddPriceUpdate(price *models.PriceData) {
	// Quick check if any client is subscribed to this symbol
	if !bm.hasSubscribers(price.Symbol) {
		return
	}
	
	select {
	case bm.batchQueue <- price:
	default:
		// Queue full, skip this update (prevents blocking)
	}
}

// hasSubscribers checks if any client is subscribed to a symbol
func (bm *BinaryManager) hasSubscribers(symbol string) bool {
	bm.subscriberMu.RLock()
	count := bm.subscriberCount[symbol]
	bm.subscriberMu.RUnlock()
	return count > 0
}

// SendEnigmaUpdate sends Enigma level updates
func (bm *BinaryManager) SendEnigmaUpdate(enigma *models.EnigmaData) {
	binaryData, err := models.EncodeEnigmaData(enigma)
	if err != nil {
		bm.logger.WithError(err).Error("Failed to encode enigma data")
		return
	}
	
	// Send to all clients subscribed to this symbol
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	subscribedClients := 0
	for client := range bm.clients {
		if client.IsSubscribed(enigma.Symbol) {
			subscribedClients++
			select {
			case client.send <- binaryData:
			default:
				// Client buffer full
			}
		}
	}
	
}

// SendSyncProgress sends sync progress updates to clients
func (bm *BinaryManager) SendSyncProgress(symbol string, progress int, totalBars int) {
	binaryData, err := models.EncodeSyncProgress(symbol, progress, totalBars)
	if err != nil {
		bm.logger.WithError(err).Error("Failed to encode sync progress")
		return
	}
	
	// Send to all clients subscribed to this symbol
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	subscribedClients := 0
	for client := range bm.clients {
		if client.IsSubscribed(symbol) {
			subscribedClients++
			select {
			case client.send <- binaryData:
			default:
				// Client buffer full
				bm.logger.Warn("Client buffer full for sync progress")
			}
		}
	}
	
	if subscribedClients > 0 {
		bm.logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"progress": progress,
			"totalBars": totalBars,
			"clients": subscribedClients,
		}).Debug("Sent sync progress to WebSocket clients")
	}
}

// SendSyncComplete sends sync completion notification
func (bm *BinaryManager) SendSyncComplete(symbol string, totalBars int, startTime, endTime time.Time) {
	binaryData, err := models.EncodeSyncComplete(symbol, totalBars, startTime, endTime)
	if err != nil {
		return
	}
	
	// Send to all clients subscribed to this symbol
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		if client.IsSubscribed(symbol) {
			select {
			case client.send <- binaryData:
			default:
				// Client buffer full
			}
		}
	}
}

// SendSyncError sends sync error notification
func (bm *BinaryManager) SendSyncError(symbol string, errorMsg string) {
	binaryData, err := models.EncodeSyncError(symbol, errorMsg)
	if err != nil {
		return
	}
	
	// Send to all clients subscribed to this symbol
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		if client.IsSubscribed(symbol) {
			select {
			case client.send <- binaryData:
			default:
				// Client buffer full
			}
		}
	}
}

// SendSymbolRemoved sends symbol removal notification
func (bm *BinaryManager) SendSymbolRemoved(sessionToken, symbol string) {
	binaryData, err := models.EncodeSymbolRemoved(symbol)
	if err != nil {
		return
	}
	
	// Send to all clients with the same session token
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		if client.sessionToken == sessionToken {
			select {
			case client.send <- binaryData:
			default:
				// Client buffer full
			}
		}
	}
}

// RegisterClient adds a new binary client
func (bm *BinaryManager) RegisterClient(client *BinaryClient) {
	bm.register <- client
}

// UnregisterClient removes a binary client
func (bm *BinaryManager) UnregisterClient(client *BinaryClient) {
	bm.unregister <- client
}

// GetConnectionCount returns the number of active connections
func (bm *BinaryManager) GetConnectionCount() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return len(bm.clients)
}

// NewBinaryClient creates a new binary WebSocket client
func NewBinaryClient(id string, sessionToken string, conn *websocket.Conn, manager *BinaryManager) *BinaryClient {
	return &BinaryClient{
		id:           id,
		sessionToken: sessionToken,
		conn:         conn,
		send:         make(chan []byte, 256),
		manager:      manager,
		symbols:      make(map[string]bool),
		lastSeen:     time.Now(),
	}
}

// WritePump pumps binary messages to the WebSocket connection
func (c *BinaryClient) WritePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	
	for {
		select {
		case data, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			
			// Check if it's JSON (control message) or binary data
			messageType := websocket.BinaryMessage
			// Simple check: if it starts with '{' it's likely JSON
			if len(data) > 0 && data[0] == '{' {
				messageType = websocket.TextMessage
			}
			
			if err := c.conn.WriteMessage(messageType, data); err != nil {
				// Only log if it's not a normal close
				if !websocket.IsCloseError(err, websocket.CloseNoStatusReceived, websocket.CloseNormalClosure) {
					c.manager.logger.WithError(err).Debug("Write error")
				}
				return
			}
			
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			// Send ping frame
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump pumps binary messages from the WebSocket connection
func (c *BinaryClient) ReadPump() {
	defer func() {
		c.manager.UnregisterClient(c)
		c.conn.Close()
	}()
	
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	
	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			// Check if it's a normal close or an unexpected error
			if websocket.IsUnexpectedCloseError(err, 
				websocket.CloseGoingAway, 
				websocket.CloseAbnormalClosure,
				websocket.CloseNoStatusReceived,
				websocket.CloseNormalClosure) {
				// Only log actual errors, not normal disconnections
				if !websocket.IsCloseError(err, websocket.CloseNoStatusReceived, websocket.CloseNormalClosure) {
					c.manager.logger.WithError(err).Debug("WebSocket closed")
				}
			}
			break
		}
		
		c.lastSeen = time.Now()
		
		switch messageType {
		case websocket.BinaryMessage:
			c.handleBinaryMessage(message)
		case websocket.TextMessage:
			// Handle JSON fallback for control messages
			c.handleTextMessage(message)
		}
	}
}

// handleBinaryMessage processes incoming binary messages from clients
func (c *BinaryClient) handleBinaryMessage(data []byte) {
	msgType, err := models.GetMessageType(data)
	if err != nil {
		return
	}
	
	switch msgType {
	case models.MsgTypeHeartbeat:
		// Client heartbeat - just update last seen
		c.lastSeen = time.Now()
		
	case models.MsgTypeMarketWatch:
		// Handle market watch updates in binary format
		// Implementation depends on your market watch binary protocol
		
	default:
		c.manager.logger.WithFields(logrus.Fields{
			"client": c.id,
			"type":   msgType,
		}).Warn("Unknown binary message type")
	}
}


// Subscribe adds symbols to client's subscription list
func (c *BinaryClient) Subscribe(symbols []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Update subscriber count in manager
	c.manager.subscriberMu.Lock()
	defer c.manager.subscriberMu.Unlock()
	
	for _, symbol := range symbols {
		if !c.symbols[symbol] {
			c.symbols[symbol] = true
			c.manager.subscriberCount[symbol]++
		}
	}
}

// Unsubscribe removes symbols from client's subscription list
func (c *BinaryClient) Unsubscribe(symbols []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Update subscriber count in manager
	c.manager.subscriberMu.Lock()
	defer c.manager.subscriberMu.Unlock()
	
	for _, symbol := range symbols {
		if c.symbols[symbol] {
			delete(c.symbols, symbol)
			c.manager.subscriberCount[symbol]--
			if c.manager.subscriberCount[symbol] <= 0 {
				delete(c.manager.subscriberCount, symbol)
			}
		}
	}
}

// IsSubscribed checks if client is subscribed to a symbol
func (c *BinaryClient) IsSubscribed(symbol string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.symbols[symbol]
}

// GetSubscriptions returns all subscribed symbols
func (c *BinaryClient) GetSubscriptions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	symbols := make([]string, 0, len(c.symbols))
	for symbol := range c.symbols {
		symbols = append(symbols, symbol)
	}
	return symbols
}

// WebSocket upgrade and NATS subscription methods

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// TODO: Implement proper origin checking
		return true
	},
}

// HandleWebSocket handles WebSocket upgrade requests
func (bm *BinaryManager) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	if bm == nil {
		http.Error(w, "WebSocket manager not initialized", http.StatusInternalServerError)
		return
	}
	
	if bm.logger == nil {
		http.Error(w, "Logger not initialized", http.StatusInternalServerError)
		return
	}
	
	
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		bm.logger.WithError(err).Error("Failed to upgrade connection")
		return
	}
	
	
	// Extract session token from header or query parameter
	sessionToken := r.Header.Get("X-Session-Token")
	if sessionToken == "" {
		sessionToken = r.URL.Query().Get("token")
	}
	
	clientID := fmt.Sprintf("client-%d", time.Now().UnixNano())
	client := NewBinaryClient(clientID, sessionToken, conn, bm)
	
	// bm.logger.WithFields(logrus.Fields{
	// 	"clientID": clientID,
	// 	"sessionToken": sessionToken,
	// 	"hasToken": sessionToken != "",
	// }).Info("New WebSocket client connected")
	
	// Register client
	bm.RegisterClient(client)
	
	// Start client goroutines
	go client.WritePump()
	go client.ReadPump()
	
	// Auto-subscribe to market watch symbols if session token provided
	if sessionToken != "" {
		go bm.autoSubscribeMarketWatch(client)
	}
}

// subscribeToUpdates subscribes to NATS updates
func (bm *BinaryManager) subscribeToUpdates() error {
	// Subscribe to price updates
	if err := bm.nats.SubscribePrices(func(price *models.PriceData) {
		bm.AddPriceUpdate(price)
	}); err != nil {
		return fmt.Errorf("failed to subscribe to prices: %w", err)
	}
	
	// Subscribe to Enigma updates
	if err := bm.nats.SubscribeEnigma(func(enigma *models.EnigmaData) {
		bm.SendEnigmaUpdate(enigma)
	}); err != nil {
		return fmt.Errorf("failed to subscribe to enigma: %w", err)
	}
	
	// Subscribe to sync progress updates
	if err := bm.nats.SubscribeSyncUpdates(func(symbol string, progress int, totalBars int) {
		// Cache the sync status
		bm.syncStatusMu.Lock()
		bm.syncStatusCache[symbol] = &models.SyncStatus{
			Symbol:    symbol,
			Status:    "syncing",
			Progress:  progress,
			TotalBars: totalBars,
			UpdatedAt: time.Now(),
		}
		bm.syncStatusMu.Unlock()
		
		bm.SendSyncProgress(symbol, progress, totalBars)
	}); err != nil {
		return fmt.Errorf("failed to subscribe to sync updates: %w", err)
	}
	
	// Subscribe to sync complete notifications
	if err := bm.nats.SubscribeSyncComplete(func(symbol string, totalBars int, startTime, endTime time.Time) {
		// Update cache to completed
		bm.syncStatusMu.Lock()
		bm.syncStatusCache[symbol] = &models.SyncStatus{
			Symbol:    symbol,
			Status:    "completed",
			Progress:  100,
			TotalBars: totalBars,
			UpdatedAt: time.Now(),
		}
		bm.syncStatusMu.Unlock()
		
		bm.SendSyncComplete(symbol, totalBars, startTime, endTime)
	}); err != nil {
		return fmt.Errorf("failed to subscribe to sync complete: %w", err)
	}
	
	// Subscribe to sync error notifications
	if err := bm.nats.SubscribeSyncErrors(func(symbol string, errorMsg string) {
		// Update cache to failed
		bm.syncStatusMu.Lock()
		bm.syncStatusCache[symbol] = &models.SyncStatus{
			Symbol:    symbol,
			Status:    "failed",
			Progress:  0,
			Error:     errorMsg,
			UpdatedAt: time.Now(),
		}
		bm.syncStatusMu.Unlock()
		
		bm.SendSyncError(symbol, errorMsg)
	}); err != nil {
		return fmt.Errorf("failed to subscribe to sync errors: %w", err)
	}
	
	return nil
}

// shutdown gracefully shuts down the manager
func (bm *BinaryManager) shutdown() {
	// bm.logger.Info("Shutting down binary WebSocket manager")
	
	// Close all client connections
	bm.mu.Lock()
	for client := range bm.clients {
		close(client.send)
		client.conn.Close()
	}
	bm.clients = make(map[*BinaryClient]bool)
	bm.mu.Unlock()
}

// handleTextMessage processes JSON messages (for control/subscription)
func (c *BinaryClient) handleTextMessage(data []byte) {
	// Define message structure
	type ControlMessage struct {
		Type         string   `json:"type"`
		Symbols      []string `json:"symbols,omitempty"`
		SessionToken string   `json:"session_token,omitempty"`
	}
	
	var msg ControlMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		c.manager.logger.WithError(err).WithField("client", c.id).Error("Invalid JSON message")
		return
	}
	
	switch msg.Type {
	case "auth":
		// Update session token
		c.mu.Lock()
		c.sessionToken = msg.SessionToken
		c.mu.Unlock()
		
		// Auto-subscribe to market watch
		if msg.SessionToken != "" {
			go c.manager.autoSubscribeMarketWatch(c)
		}
		
		// Send auth confirmation
		authMsg := map[string]string{
			"type": "auth_confirmed",
			"message": "Session token updated",
		}
		if data, err := json.Marshal(authMsg); err == nil {
			select {
			case c.send <- data:
			default:
			}
		}
		
	case "subscribe":
		c.Subscribe(msg.Symbols)
		// c.manager.logger.WithFields(logrus.Fields{
		// 	"client":  c.id,
		// 	"symbols": msg.Symbols,
		// }).Debug("Client subscribed")
		
		// Send current prices for subscribed symbols
		go c.sendCurrentPrices(msg.Symbols)
		
	case "unsubscribe":
		c.Unsubscribe(msg.Symbols)
		// c.manager.logger.WithFields(logrus.Fields{
		// 	"client":  c.id,
		// 	"symbols": msg.Symbols,
		// }).Debug("Client unsubscribed")
		
	case "ping":
		// Send pong
		pongMsg := map[string]string{"type": "pong"}
		if data, err := json.Marshal(pongMsg); err == nil {
			select {
			case c.send <- data:
			default:
				// Buffer full
			}
		}
		
	default:
		c.manager.logger.WithFields(logrus.Fields{
			"client": c.id,
			"type":   msg.Type,
		}).Warn("Unknown message type")
	}
}

// sendCurrentPrices sends current prices for requested symbols
func (c *BinaryClient) sendCurrentPrices(symbols []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	prices, err := c.manager.redis.GetPrices(ctx, symbols)
	if err != nil {
		c.manager.logger.WithError(err).Error("Failed to get current prices")
		return
	}
	
	// Send each price individually for immediate delivery
	for _, price := range prices {
		if price != nil {
			c.manager.AddPriceUpdate(price)
		}
	}
}

// AutoSubscribeSymbol automatically subscribes all clients with the given session token to a symbol
func (bm *BinaryManager) AutoSubscribeSymbol(sessionToken, symbol string) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		if client.sessionToken == sessionToken {
			// Send subscribe message to client
			msg := map[string]interface{}{
				"type": "auto_subscribe",
				"data": map[string]interface{}{
					"symbol": symbol,
				},
			}
			
			if data, err := json.Marshal(msg); err == nil {
				select {
				case client.send <- data:
					// Subscribe the client
					client.Subscribe([]string{symbol})
					// Send current price
					go client.sendCurrentPrices([]string{symbol})
				default:
					// Client buffer full
				}
			}
		}
	}
}

// AutoUnsubscribeSymbol automatically unsubscribes all clients with the given session token from a symbol
func (bm *BinaryManager) AutoUnsubscribeSymbol(sessionToken, symbol string) {
	// bm.logger.WithFields(logrus.Fields{
	// 	"sessionToken": sessionToken,
	// 	"symbol": symbol,
	// 	"clientCount": len(bm.clients),
	// }).Debug("AutoUnsubscribeSymbol called")
	
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	clientsFound := 0
	for client := range bm.clients {
		// bm.logger.WithFields(logrus.Fields{
		// 	"clientToken": client.sessionToken,
		// 	"targetToken": sessionToken,
		// 	"match": client.sessionToken == sessionToken,
		// }).Debug("Checking client for auto-unsubscribe")
		
		if client.sessionToken == sessionToken {
			clientsFound++
			// Send unsubscribe message to client
			msg := map[string]interface{}{
				"type": "auto_unsubscribe",
				"data": map[string]interface{}{
					"symbol": symbol,
				},
			}
			
			if data, err := json.Marshal(msg); err == nil {
				select {
				case client.send <- data:
					// Unsubscribe the client
					client.Unsubscribe([]string{symbol})
				default:
					// Client buffer full
				}
			}
		}
	}
	
	// bm.logger.WithFields(logrus.Fields{
	// 	"sessionToken": sessionToken,
	// 	"symbol": symbol,
	// 	"clientsMatched": clientsFound,
	// }).Debug("AutoUnsubscribeSymbol completed")
}

// sendInitialSyncStatus sends the current sync status for symbols to a client
func (bm *BinaryManager) sendInitialSyncStatus(client *BinaryClient, symbols []string) {
	if client.sessionToken == "" || len(symbols) == 0 {
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Get sync status from MySQL for all symbols
	symbolsWithStatus, err := bm.mysql.GetMarketWatchWithSyncStatus(ctx, client.sessionToken)
	if err != nil {
		bm.logger.WithError(err).Error("Failed to get sync status from MySQL")
		return
	}
	
	// Send sync status for each symbol
	for _, item := range symbolsWithStatus {
		symbol, ok := item["symbol"].(string)
		if !ok {
			continue
		}
		
		// Get sync status and progress
		syncStatus, _ := item["sync_status"].(string)
		var progress int
		if p, ok := item["sync_progress"].(int64); ok {
			progress = int(p)
		} else if p, ok := item["sync_progress"].(float64); ok {
			progress = int(p)
		}
		
		// Send sync progress update
		if syncStatus == "syncing" || syncStatus == "completed" {
			bm.SendSyncProgress(symbol, progress, 0)
		} else if syncStatus == "failed" {
			// Send sync error
			if bm.nats != nil {
				bm.nats.PublishSyncError(symbol, "Sync failed")
			}
		}
		
		// If completed, also send completion notification
		if syncStatus == "completed" {
			endTime := time.Now()
			startTime := endTime.AddDate(0, 0, -1000) // Approximate
			bm.SendSyncComplete(symbol, 1000*1440, startTime, endTime)
		}
	}
	
	bm.logger.WithFields(logrus.Fields{
		"client": client.id,
		"symbols": len(symbols),
	}).Debug("Sent initial sync status")
}

// autoSubscribeMarketWatch automatically subscribes a client to their market watch symbols
func (bm *BinaryManager) autoSubscribeMarketWatch(client *BinaryClient) {
	if client.sessionToken == "" {
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Get market watch symbols from Redis
	symbols, err := bm.redis.GetMarketWatch(ctx, client.sessionToken)
	if err != nil {
		bm.logger.WithError(err).WithField("client", client.id).Error("Failed to get market watch symbols")
		return
	}
	
	if len(symbols) == 0 {
		return
	}
	
	// Send auto-subscribe message
	msg := map[string]interface{}{
		"type": "market_watch_subscribe",
		"symbols": symbols,
	}
	
	if data, err := json.Marshal(msg); err == nil {
		select {
		case client.send <- data:
			// Subscribe the client to all symbols
			client.Subscribe(symbols)
			// Load initial sync status into cache
			go bm.loadInitialSyncStatus(symbols)
			// Send current prices
			go client.sendCurrentPrices(symbols)
			// Send initial sync status for each symbol
			go bm.sendInitialSyncStatus(client, symbols)
			
			// bm.logger.WithFields(logrus.Fields{
			// 	"client": client.id,
			// 	"symbols": symbols,
			// 	"count": len(symbols),
			// }).Debug("Auto-subscribed to market watch symbols")
		default:
			// Client buffer full
		}
	}
}

// broadcastSyncStatus sends sync status updates for all market watch symbols to all clients
func (bm *BinaryManager) broadcastSyncStatus() {
	// Get all unique symbols from connected clients
	symbolsToCheck := make(map[string]bool)
	
	bm.mu.RLock()
	for client := range bm.clients {
		if client.sessionToken != "" {
			client.mu.RLock()
			for symbol := range client.symbols {
				symbolsToCheck[symbol] = true
			}
			client.mu.RUnlock()
		}
	}
	bm.mu.RUnlock()
	
	if len(symbolsToCheck) == 0 {
		return
	}
	
	// Fetch latest sync status from MySQL for all symbols
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	for symbol := range symbolsToCheck {
		// Check if we have cached status
		bm.syncStatusMu.RLock()
		status, hasCached := bm.syncStatusCache[symbol]
		bm.syncStatusMu.RUnlock()
		
		// If no cached status or status is old, fetch from MySQL
		if !hasCached || time.Since(status.UpdatedAt) > 10*time.Second {
			// Get sync status from MySQL
			syncStatus, err := bm.mysql.GetSyncStatus(ctx, symbol)
			if err != nil {
				// If error, create a pending status
				syncStatus = &models.SyncStatus{
					Symbol:    symbol,
					Status:    "pending",
					Progress:  0,
					TotalBars: 0,
					UpdatedAt: time.Now(),
				}
			}
			
			// Update cache
			bm.syncStatusMu.Lock()
			bm.syncStatusCache[symbol] = syncStatus
			bm.syncStatusMu.Unlock()
			
			status = syncStatus
		}
		
		// Send sync progress to all subscribed clients
		if status != nil && status.Status == "syncing" {
			bm.SendSyncProgress(status.Symbol, status.Progress, status.TotalBars)
		}
	}
}

// loadInitialSyncStatus loads sync status from MySQL for given symbols
func (bm *BinaryManager) loadInitialSyncStatus(symbols []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	for _, symbol := range symbols {
		syncStatus, err := bm.mysql.GetSyncStatus(ctx, symbol)
		if err != nil {
			// Log error but continue
			bm.logger.WithError(err).WithField("symbol", symbol).Debug("Failed to get sync status from MySQL")
			continue
		}
		
		// Cache the status
		bm.syncStatusMu.Lock()
		bm.syncStatusCache[symbol] = syncStatus
		bm.syncStatusMu.Unlock()
	}
}