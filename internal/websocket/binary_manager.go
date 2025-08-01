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
	
	// Dependencies
	hub           *exchange.Hub
	nats          *messaging.NATSClient
	redis         *cache.RedisClient
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
	logger *logrus.Logger,
) *BinaryManager {
	bm := &BinaryManager{
		clients:       make(map[*BinaryClient]bool),
		register:      make(chan *BinaryClient),
		unregister:    make(chan *BinaryClient),
		broadcast:     make(chan []byte, 1000),
		batchQueue:    make(chan *models.PriceData, 10000),
		batchSize:     50,  // Batch up to 50 price updates
		batchInterval: 50 * time.Millisecond, // Send batch every 50ms
		heartbeatInt:  30 * time.Second,
		hub:           hub,
		nats:          nats,
		redis:         cache,
		logger:        logger.WithField("component", "ws-binary"),
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
	defer ticker.Stop()
	defer heartbeat.Stop()
	
	var batchBuffer []*models.PriceData
	
	for {
		select {
		case <-ctx.Done():
			return
			
		case client := <-bm.register:
			bm.mu.Lock()
			bm.clients[client] = true
			bm.mu.Unlock()
			bm.logger.Infof("Binary client connected: %s", client.id)
			
		case client := <-bm.unregister:
			if _, ok := bm.clients[client]; ok {
				bm.mu.Lock()
				delete(bm.clients, client)
				bm.mu.Unlock()
				close(client.send)
				bm.logger.Infof("Binary client disconnected: %s", client.id)
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
			bm.logger.Infof("Failed to encode price batch for client %s: %v", client.id, err)
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
		bm.logger.Infof("Binary queue full, skipping price update for %s", price.Symbol)
	}
}

// hasSubscribers checks if any client is subscribed to a symbol
func (bm *BinaryManager) hasSubscribers(symbol string) bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		if client.IsSubscribed(symbol) {
			return true
		}
	}
	return false
}

// SendEnigmaUpdate sends Enigma level updates
func (bm *BinaryManager) SendEnigmaUpdate(enigma *models.EnigmaData) {
	binaryData, err := models.EncodeEnigmaData(enigma)
	if err != nil {
		bm.logger.Infof("Failed to encode enigma data: %v", err)
		return
	}
	
	select {
	case bm.broadcast <- binaryData:
	default:
		bm.logger.Infof("Broadcast channel full, skipping enigma update")
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
			
			// Send binary message
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				c.manager.logger.WithError(err).Error("Binary write error")
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
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.manager.logger.WithError(err).Error("Binary read error")
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
		c.manager.logger.Infof("Invalid binary message from client %s: %v", c.id, err)
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
	
	for _, symbol := range symbols {
		c.symbols[symbol] = true
	}
}

// Unsubscribe removes symbols from client's subscription list
func (c *BinaryClient) Unsubscribe(symbols []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for _, symbol := range symbols {
		delete(c.symbols, symbol)
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
	
	bm.logger.Info("WebSocket connection attempt")
	
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		bm.logger.WithError(err).Error("Failed to upgrade connection")
		return
	}
	
	bm.logger.Info("WebSocket connection upgraded successfully")
	
	// Extract session token from header or query parameter
	sessionToken := r.Header.Get("X-Session-Token")
	if sessionToken == "" {
		sessionToken = r.URL.Query().Get("token")
	}
	
	clientID := fmt.Sprintf("client-%d", time.Now().UnixNano())
	client := NewBinaryClient(clientID, sessionToken, conn, bm)
	
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
	
	return nil
}

// shutdown gracefully shuts down the manager
func (bm *BinaryManager) shutdown() {
	bm.logger.Info("Shutting down binary WebSocket manager")
	
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
		c.manager.logger.WithFields(logrus.Fields{
			"client":  c.id,
			"symbols": msg.Symbols,
		}).Info("Client subscribed")
		
		// Send current prices for subscribed symbols
		go c.sendCurrentPrices(msg.Symbols)
		
	case "unsubscribe":
		c.Unsubscribe(msg.Symbols)
		c.manager.logger.WithFields(logrus.Fields{
			"client":  c.id,
			"symbols": msg.Symbols,
		}).Info("Client unsubscribed")
		
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
				"symbol": symbol,
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
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	for client := range bm.clients {
		if client.sessionToken == sessionToken {
			// Send unsubscribe message to client
			msg := map[string]interface{}{
				"type": "auto_unsubscribe",
				"symbol": symbol,
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
			// Send current prices
			go client.sendCurrentPrices(symbols)
			
			bm.logger.WithFields(logrus.Fields{
				"client": client.id,
				"symbols": symbols,
				"count": len(symbols),
			}).Info("Auto-subscribed to market watch symbols")
		default:
			// Client buffer full
		}
	}
}