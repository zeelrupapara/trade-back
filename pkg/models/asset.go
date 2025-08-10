package models

// AssetClass represents the type of financial asset
type AssetClass string

const (
	AssetClassCrypto     AssetClass = "crypto"
	AssetClassForex      AssetClass = "forex"
	AssetClassStock      AssetClass = "stock"
	AssetClassCommodity  AssetClass = "commodity"
	AssetClassIndex      AssetClass = "index"
	AssetClassUnknown    AssetClass = "unknown"
)

// AssetInfo contains information about an asset
type AssetInfo struct {
	Symbol          string     `json:"symbol"`
	NormalizedSymbol string    `json:"normalized_symbol"`
	AssetClass      AssetClass `json:"asset_class"`
	Exchange        string     `json:"exchange"`
	Description     string     `json:"description"`
}

// AssetExtreme represents all-time high/low for an asset
type AssetExtreme struct {
	Symbol      string     `json:"symbol"`
	AssetClass  AssetClass `json:"asset_class"`
	ATH         float64    `json:"ath"`
	ATL         float64    `json:"atl"`
	ATHDate     string     `json:"ath_date"`
	ATLDate     string     `json:"atl_date"`
	Week52High  float64    `json:"week_52_high,omitempty"`
	Week52Low   float64    `json:"week_52_low,omitempty"`
	DataSource  string     `json:"data_source"`
	LastUpdated int64      `json:"last_updated"`
}