package models

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

// SymbolInfo represents trading symbol metadata
type SymbolInfo struct {
	ID                   int       `json:"id" db:"id"`
	Exchange             string    `json:"exchange" db:"exchange"`
	Symbol               string    `json:"symbol" db:"symbol"`
	FullName             string    `json:"full_name" db:"full_name"`
	InstrumentType       string    `json:"instrument_type" db:"instrument_type"`
	BaseCurrency         string    `json:"base_currency" db:"base_currency"`
	QuoteCurrency        string    `json:"quote_currency" db:"quote_currency"`
	IsActive             bool      `json:"is_active" db:"is_active"`
	MinPriceIncrement    float64   `json:"min_price_increment" db:"min_price_increment"`
	MinQuantityIncrement float64   `json:"min_quantity_increment" db:"min_quantity_increment"`
	CreatedAt            time.Time `json:"created_at" db:"created_at"`
	UpdatedAt            time.Time `json:"updated_at" db:"updated_at"`
}

// TradingSession represents market trading sessions
type TradingSession struct {
	ID         int       `json:"id" db:"id"`
	Name       string    `json:"name" db:"name"`
	StartTime  string    `json:"start_time" db:"start_time"` // MySQL TIME field as string
	EndTime    string    `json:"end_time" db:"end_time"`     // MySQL TIME field as string
	Timezone   string    `json:"timezone" db:"timezone"`
	DaysActive JSONStringArray `json:"days_active" db:"days_active"`
	IsActive   bool      `json:"is_active" db:"is_active"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// MarketWatch represents user's watchlist
type MarketWatch struct {
	ID           int         `json:"id" db:"id"`
	SymbolID     int         `json:"symbol_id" db:"symbol_id"`
	SessionToken string      `json:"session_token" db:"session_token"`
	AddedAt      time.Time   `json:"added_at" db:"added_at"`
	Symbol       *SymbolInfo `json:"symbol,omitempty"`
}

// UserSession represents WebSocket connection session
type UserSession struct {
	ID           int       `json:"id" db:"id"`
	SessionToken string    `json:"session_token" db:"session_token"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	LastActivity time.Time `json:"last_activity" db:"last_activity"`
	IsActive     bool      `json:"is_active" db:"is_active"`
}

// SystemConfig represents configuration values
type SystemConfig struct {
	ID          int       `json:"id" db:"id"`
	ConfigKey   string    `json:"config_key" db:"config_key"`
	ConfigValue string    `json:"config_value" db:"config_value"`
	Description string    `json:"description" db:"description"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// JSONStringArray handles JSON array stored as string in MySQL
type JSONStringArray []string

// Scan implements sql.Scanner interface
func (j *JSONStringArray) Scan(value interface{}) error {
	if value == nil {
		*j = []string{}
		return nil
	}
	
	bytes, ok := value.([]byte)
	if !ok {
		str, ok := value.(string)
		if !ok {
			return nil
		}
		bytes = []byte(str)
	}
	
	return json.Unmarshal(bytes, j)
}

// Value implements driver.Valuer interface
func (j JSONStringArray) Value() (driver.Value, error) {
	if len(j) == 0 {
		return "[]", nil
	}
	return json.Marshal(j)
}