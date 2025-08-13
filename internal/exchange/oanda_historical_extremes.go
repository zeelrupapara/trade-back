package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

// OANDAHistoricalExtremes handles fetching historical extremes from OANDA
type OANDAHistoricalExtremes struct {
	apiKey     string
	accountID  string
	baseURL    string
	httpClient *http.Client
	logger     *logrus.Entry
}

// NewOANDAHistoricalExtremes creates a new OANDA historical extremes fetcher
func NewOANDAHistoricalExtremes(apiKey, accountID string, logger *logrus.Logger) *OANDAHistoricalExtremes {
	// Use practice environment for historical data (same API key pattern as streaming)
	baseURL := "https://api-fxpractice.oanda.com"
	
	return &OANDAHistoricalExtremes{
		apiKey:    apiKey,
		accountID: accountID,
		baseURL:   baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger.WithField("component", "oanda-historical"),
	}
}

// CandleResponse represents OANDA candle response
type CandleResponse struct {
	Instrument  string   `json:"instrument"`
	Granularity string   `json:"granularity"`
	Candles     []Candle `json:"candles"`
}

// Candle represents a single candle
type Candle struct {
	Time     string `json:"time"`
	Complete bool   `json:"complete"`
	Mid      struct {
		O string `json:"o"`
		H string `json:"h"`
		L string `json:"l"`
		C string `json:"c"`
	} `json:"mid"`
}

// GetHistoricalExtremes fetches historical extremes for a forex pair
func (o *OANDAHistoricalExtremes) GetHistoricalExtremes(ctx context.Context, pair string, years int) (ath, atl float64, err error) {
	if years > 20 {
		years = 20 // OANDA limit
	}

	// Calculate the date range
	endTime := time.Now()
	startTime := endTime.AddDate(-years, 0, 0)

	// Initialize extremes
	ath = 0.0
	atl = math.MaxFloat64

	// OANDA limits: max 5000 candles per request
	// For daily candles: ~13.7 years of data in one request
	// For weekly candles: ~96 years of data in one request
	granularity := "D" // Daily candles for good coverage

	// Fetch candles in chunks if needed
	currentStart := startTime
	for currentStart.Before(endTime) {
		// Calculate chunk end (max 5000 daily candles = ~13.7 years)
		chunkEnd := currentStart.AddDate(13, 0, 0)
		if chunkEnd.After(endTime) {
			chunkEnd = endTime
		}

		// Fetch chunk
		chunkATH, chunkATL, err := o.fetchCandleChunk(ctx, pair, currentStart, chunkEnd, granularity)
		if err != nil {
			o.logger.WithError(err).WithFields(logrus.Fields{
				"pair":  pair,
				"start": currentStart,
				"end":   chunkEnd,
			}).Warn("Failed to fetch candle chunk")
			// Continue with next chunk even if one fails
			currentStart = chunkEnd
			continue
		}

		// Update extremes
		if chunkATH > ath {
			ath = chunkATH
		}
		if chunkATL < atl {
			atl = chunkATL
		}

		// Move to next chunk
		currentStart = chunkEnd
	}

	if ath == 0 || atl == math.MaxFloat64 {
		return 0, 0, fmt.Errorf("no historical data found for %s", pair)
	}

	o.logger.WithFields(logrus.Fields{
		"pair":  pair,
		"years": years,
		"ath":   ath,
		"atl":   atl,
	}).Info("Fetched historical extremes from OANDA")

	return ath, atl, nil
}

// fetchCandleChunk fetches a chunk of candles
func (o *OANDAHistoricalExtremes) fetchCandleChunk(ctx context.Context, pair string, start, end time.Time, granularity string) (ath, atl float64, err error) {
	// Build request URL
	params := url.Values{}
	params.Set("from", start.Format(time.RFC3339))
	params.Set("to", end.Format(time.RFC3339))
	params.Set("granularity", granularity)
	params.Set("price", "M") // Mid prices

	url := fmt.Sprintf("%s/v3/instruments/%s/candles?%s", o.baseURL, pair, params.Encode())

	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	req.Header.Set("Authorization", "Bearer "+o.apiKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := o.httpClient.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	// Parse response
	var candleResp CandleResponse
	if err := json.NewDecoder(resp.Body).Decode(&candleResp); err != nil {
		return 0, 0, fmt.Errorf("failed to decode response: %w", err)
	}

	// Find extremes in candles
	ath = 0.0
	atl = math.MaxFloat64

	for _, candle := range candleResp.Candles {
		// Parse high and low
		var high, low float64
		fmt.Sscanf(candle.Mid.H, "%f", &high)
		fmt.Sscanf(candle.Mid.L, "%f", &low)

		if high > ath {
			ath = high
		}
		if low < atl && low > 0 {
			atl = low
		}
	}

	return ath, atl, nil
}

// GetAllTimeExtremes fetches all available historical extremes (up to 20 years)
func (o *OANDAHistoricalExtremes) GetAllTimeExtremes(ctx context.Context, pair string) (ath, atl float64, err error) {
	return o.GetHistoricalExtremes(ctx, pair, 20) // Max available from OANDA
}