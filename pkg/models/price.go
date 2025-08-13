package models

import (
	"time"
)

// PriceData represents real-time price information
type PriceData struct {
	Symbol        string    `json:"symbol"`
	Exchange      string    `json:"exchange"`        // Exchange name (binance, oanda, etc.)
	Price         float64   `json:"price"`
	Bid           float64   `json:"bid"`
	Ask           float64   `json:"ask"`
	Volume        float64   `json:"volume"`
	Change24h     float64   `json:"change_24h"`     // 24hr absolute change
	ChangePercent float64   `json:"change_percent"`  // 24hr percentage change
	Open24h       float64   `json:"open_24h"`        // 24hr open price
	High24h       float64   `json:"high_24h"`        // 24hr high
	Low24h        float64   `json:"low_24h"`         // 24hr low
	Timestamp     time.Time `json:"timestamp"`
	Sequence      uint64    `json:"sequence,omitempty"`
}

// Bar represents OHLCV candlestick data
type Bar struct {
	Symbol     string    `json:"symbol"`
	Timestamp  time.Time `json:"timestamp"`
	Open       float64   `json:"open"`
	High       float64   `json:"high"`
	Low        float64   `json:"low"`
	Close      float64   `json:"close"`
	Volume     float64   `json:"volume"`
	TradeCount int64     `json:"trade_count,omitempty"`
}

// EnigmaData represents custom technical indicator levels
type EnigmaData struct {
	Symbol    string           `json:"symbol"`
	Level     float64          `json:"level"`
	ATH       float64          `json:"ath"`
	ATL       float64          `json:"atl"`
	FibLevels FibonacciLevels  `json:"fib_levels"`
	Timestamp time.Time        `json:"timestamp"`
}

// FibonacciLevels represents fibonacci retracement levels
type FibonacciLevels struct {
	L0   float64 `json:"0"`     // 0% (ATL)
	L236 float64 `json:"23.6"`  // 23.6%
	L382 float64 `json:"38.2"`  // 38.2%
	L50  float64 `json:"50"`    // 50%
	L618 float64 `json:"61.8"`  // 61.8%
	L786 float64 `json:"78.6"`  // 78.6%
	L100 float64 `json:"100"`   // 100% (ATH)
}