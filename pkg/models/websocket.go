package models

// WebSocketMessage represents generic WebSocket message structure
type WebSocketMessage struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

// SubscriptionRequest represents client subscription request
type SubscriptionRequest struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols,omitempty"`
	Token   string   `json:"token,omitempty"`
}

// ErrorResponse represents error message structure
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// HealthStatus represents system health information
type HealthStatus struct {
	Status      string                 `json:"status"`
	Timestamp   string                 `json:"timestamp"`
	Services    map[string]ServiceHealth `json:"services"`
	Connections int                    `json:"connections"`
	Version     string                 `json:"version"`
}

// ServiceHealth represents individual service health
type ServiceHealth struct {
	Status  string `json:"status"`
	Latency int64  `json:"latency_ms,omitempty"`
	Error   string `json:"error,omitempty"`
}

// BarsResponse represents historical bars API response
type BarsResponse struct {
	Bars      []*Bar `json:"bars"`
	NoData    bool   `json:"no_data"`
	Count     int    `json:"count"`
	StartTime int64  `json:"start_time,omitempty"`
	EndTime   int64  `json:"end_time,omitempty"`
}

// SymbolsResponse represents symbols API response
type SymbolsResponse struct {
	Symbols []*SymbolInfo `json:"symbols"`
	Count   int           `json:"count"`
	Page    int           `json:"page,omitempty"`
	PerPage int           `json:"per_page,omitempty"`
}

// PriceResponse represents price API response
type PriceResponse struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Volume    float64 `json:"volume"`
	Timestamp int64   `json:"timestamp"`
	Cached    bool    `json:"cached,omitempty"`
}

// MarketWatchResponse represents market watch API response
type MarketWatchResponse struct {
	Symbols []*SymbolInfo `json:"symbols"`
	Count   int           `json:"count"`
	Token   string        `json:"token"`
}

// SessionChangeEvent represents trading session change
type SessionChangeEvent struct {
	ActiveSessions []string       `json:"active_sessions"`
	NextSession    *SessionInfo   `json:"next_session,omitempty"`
	Timestamp      int64          `json:"timestamp"`
}

// SessionInfo represents basic session information
type SessionInfo struct {
	Name     string `json:"name"`
	StartsIn int64  `json:"starts_in"`
}

// ConnectionStatus represents WebSocket connection state
type ConnectionStatus struct {
	ID          string   `json:"id"`
	Connected   bool     `json:"connected"`
	ConnectedAt int64    `json:"connected_at"`
	LastSeen    int64    `json:"last_seen"`
	Symbols     []string `json:"symbols"`
	IPAddress   string   `json:"ip_address"`
}

// PeriodLevelUpdate represents a WebSocket update for period levels
type PeriodLevelUpdate struct {
	Type        string       `json:"type"`        // "new_extreme" | "period_boundary" | "level_approach"
	Symbol      string       `json:"symbol"`
	Period      PeriodType   `json:"period"`
	Level       *PeriodLevel `json:"level,omitempty"`
	OldValue    float64      `json:"old_value,omitempty"`
	NewValue    float64      `json:"new_value,omitempty"`
	CurrentPrice float64     `json:"current_price,omitempty"`
	Timestamp   int64        `json:"timestamp"`
}

// PeriodBoundaryEvent represents when a new period starts
type PeriodBoundaryEvent struct {
	Period      PeriodType              `json:"period"`
	OldPeriod   *PeriodLevel           `json:"old_period,omitempty"`
	NewPeriod   *PeriodLevel           `json:"new_period,omitempty"`
	AffectedSymbols []string           `json:"affected_symbols"`
	Timestamp   int64                  `json:"timestamp"`
}

// LevelApproachAlert represents price approaching a Fibonacci level
type LevelApproachAlert struct {
	Symbol       string     `json:"symbol"`
	Period       PeriodType `json:"period"`
	Level        string     `json:"level"`        // "23.6%" | "38.2%" | "50%" | "61.8%" | "78.6%"
	LevelPrice   float64    `json:"level_price"`
	CurrentPrice float64    `json:"current_price"`
	Distance     float64    `json:"distance"`
	DistancePercent float64 `json:"distance_percent"`
	Direction    string     `json:"direction"`    // "above" | "below"
	Timestamp    int64      `json:"timestamp"`
}