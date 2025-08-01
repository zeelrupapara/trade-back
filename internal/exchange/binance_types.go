package exchange

import (
	"time"
)

// BinanceTickerData represents 24hr ticker data from Binance
type BinanceTickerData struct {
	EventType            string `json:"e"`   // Event type
	EventTime            int64  `json:"E"`   // Event time
	Symbol               string `json:"s"`   // Symbol
	Price                string `json:"c"`   // Last price
	OpenPrice            string `json:"o"`   // Open price
	HighPrice            string `json:"h"`   // High price
	LowPrice             string `json:"l"`   // Low price
	Volume               string `json:"v"`   // Total traded base asset volume
	QuoteVolume          string `json:"q"`   // Total traded quote asset volume
	OpenTime             int64  `json:"O"`   // Statistics open time
	CloseTime            int64  `json:"C"`   // Statistics close time
	FirstTradeID         int64  `json:"F"`   // First trade ID
	LastTradeID          int64  `json:"L"`   // Last trade ID
	TradeCount           int64  `json:"n"`   // Total number of trades
	PriceChange          string `json:"p"`   // Price change
	PriceChangePercent   string `json:"P"`   // Price change percent
	WeightedAvgPrice     string `json:"w"`   // Weighted average price
	BidPrice             string `json:"b"`   // Best bid price
	BidQuantity          string `json:"B"`   // Best bid quantity
	AskPrice             string `json:"a"`   // Best ask price
	AskQuantity          string `json:"A"`   // Best ask quantity
}

// BinanceBookTickerData represents book ticker data
type BinanceBookTickerData struct {
	UpdateID     int64  `json:"u"`   // Update ID
	Symbol       string `json:"s"`   // Symbol
	BidPrice     string `json:"b"`   // Best bid price
	BidQuantity  string `json:"B"`   // Best bid quantity
	AskPrice     string `json:"a"`   // Best ask price
	AskQuantity  string `json:"A"`   // Best ask quantity
	EventTime    int64  `json:"E"`   // Event time
	TransactTime int64  `json:"T"`   // Transaction time
}

// BinanceTradeData represents trade data
type BinanceTradeData struct {
	EventType    string `json:"e"`   // Event type
	EventTime    int64  `json:"E"`   // Event time
	Symbol       string `json:"s"`   // Symbol
	TradeID      int64  `json:"t"`   // Trade ID
	Price        string `json:"p"`   // Price
	Quantity     string `json:"q"`   // Quantity
	BuyerOrderID int64  `json:"b"`   // Buyer order ID
	SellerOrderID int64 `json:"a"`   // Seller order ID
	TradeTime    int64  `json:"T"`   // Trade time
	IsBuyerMaker bool   `json:"m"`   // Is the buyer the market maker?
}

// BinanceKline represents kline/candlestick data
type BinanceKline struct {
	EventType    string `json:"e"`   // Event type
	EventTime    int64  `json:"E"`   // Event time
	Symbol       string `json:"s"`   // Symbol
	Kline        KlineData `json:"k"` // Kline data
}

// KlineData represents the actual kline data
type KlineData struct {
	StartTime            int64  `json:"t"`  // Kline start time
	CloseTime            int64  `json:"T"`  // Kline close time
	Symbol               string `json:"s"`  // Symbol
	Interval             string `json:"i"`  // Interval
	FirstTradeID         int64  `json:"f"`  // First trade ID
	LastTradeID          int64  `json:"L"`  // Last trade ID
	OpenPrice            string `json:"o"`  // Open price
	ClosePrice           string `json:"c"`  // Close price
	HighPrice            string `json:"h"`  // High price
	LowPrice             string `json:"l"`  // Low price
	Volume               string `json:"v"`  // Base asset volume
	TradeCount           int64  `json:"n"`  // Number of trades
	IsClosed             bool   `json:"x"`  // Is this kline closed?
	QuoteVolume          string `json:"q"`  // Quote asset volume
	TakerBuyVolume       string `json:"V"`  // Taker buy base asset volume
	TakerBuyQuoteVolume  string `json:"Q"`  // Taker buy quote asset volume
}

// BinanceExchangeInfo represents exchange information
type BinanceExchangeInfo struct {
	Timezone   string                 `json:"timezone"`
	ServerTime int64                  `json:"serverTime"`
	Symbols    []BinanceSymbolInfo    `json:"symbols"`
}

// BinanceSymbolInfo represents symbol information
type BinanceSymbolInfo struct {
	Symbol              string                   `json:"symbol"`
	Status              string                   `json:"status"`
	BaseAsset           string                   `json:"baseAsset"`
	BaseAssetPrecision  int                      `json:"baseAssetPrecision"`
	QuoteAsset          string                   `json:"quoteAsset"`
	QuotePrecision      int                      `json:"quotePrecision"`
	QuoteAssetPrecision int                      `json:"quoteAssetPrecision"`
	OrderTypes          []string                 `json:"orderTypes"`
	IcebergAllowed      bool                     `json:"icebergAllowed"`
	OcoAllowed          bool                     `json:"ocoAllowed"`
	IsSpotTradingAllowed bool                    `json:"isSpotTradingAllowed"`
	IsMarginTradingAllowed bool                  `json:"isMarginTradingAllowed"`
	Filters             []BinanceFilter          `json:"filters"`
}

// BinanceFilter represents trading filters
type BinanceFilter struct {
	FilterType       string `json:"filterType"`
	MinPrice         string `json:"minPrice,omitempty"`
	MaxPrice         string `json:"maxPrice,omitempty"`
	TickSize         string `json:"tickSize,omitempty"`
	MinQty           string `json:"minQty,omitempty"`
	MaxQty           string `json:"maxQty,omitempty"`
	StepSize         string `json:"stepSize,omitempty"`
	MinNotional      string `json:"minNotional,omitempty"`
	ApplyToMarket    bool   `json:"applyToMarket,omitempty"`
	AvgPriceMins     int    `json:"avgPriceMins,omitempty"`
}

// ParseEventTime converts Binance event time to time.Time
func ParseEventTime(eventTime int64) time.Time {
	return time.Unix(eventTime/1000, (eventTime%1000)*1e6)
}