package models

import (
	"time"
)

// PeriodType represents the time period for levels
type PeriodType string

const (
	PeriodDaily   PeriodType = "daily"
	PeriodWeekly  PeriodType = "weekly"
	PeriodMonthly PeriodType = "monthly"
	PeriodYearly  PeriodType = "yearly"
)

// PeriodLevel represents ATH/ATL for a specific time period (daily, weekly, monthly)
type PeriodLevel struct {
	Symbol      string     `json:"symbol"`
	Period      PeriodType `json:"period"`
	High        float64    `json:"high"`  // ATH for this period
	Low         float64    `json:"low"`   // ATL for this period
	StartTime   time.Time  `json:"start_time"`
	EndTime     time.Time  `json:"end_time"`
	LastUpdated time.Time  `json:"last_updated"`
	IsActive    bool       `json:"is_active"` // True if this is the current period
}

// PeriodLevelsResponse contains levels for all periods
type PeriodLevelsResponse struct {
	Symbol       string                  `json:"symbol"`
	AssetClass   AssetClass              `json:"asset_class"`
	CurrentPrice float64                 `json:"current_price,omitempty"`
	Daily        *PeriodLevel            `json:"daily,omitempty"`
	Weekly       *PeriodLevel            `json:"weekly,omitempty"`
	Monthly      *PeriodLevel            `json:"monthly,omitempty"`
	Yearly       *PeriodLevel            `json:"yearly,omitempty"`
	Timestamp    time.Time               `json:"timestamp"`
}

// PeriodStats provides additional statistics for a period
type PeriodStats struct {
	Symbol         string     `json:"symbol"`
	Period         PeriodType `json:"period"`
	High           float64    `json:"high"`
	Low            float64    `json:"low"`
	Range          float64    `json:"range"`
	RangePercent   float64    `json:"range_percent"`
	CurrentPrice   float64    `json:"current_price,omitempty"`
	PositionInRange float64   `json:"position_in_range"` // 0-1, where 0 is at low, 1 is at high
	Volume         float64    `json:"volume,omitempty"`
	TradeCount     int64      `json:"trade_count,omitempty"`
}

// GetRange returns the range between high and low
func (pl *PeriodLevel) GetRange() float64 {
	return pl.High - pl.Low
}

// GetPositionInRange returns where the current price is within the range (0-1)
func (pl *PeriodLevel) GetPositionInRange(currentPrice float64) float64 {
	if pl.High == pl.Low {
		return 0.5
	}
	
	position := (currentPrice - pl.Low) / (pl.High - pl.Low)
	
	// Clamp between 0 and 1
	if position < 0 {
		return 0
	}
	if position > 1 {
		return 1
	}
	
	return position
}

// IsNearHighLow checks if price is near high or low
func (pl *PeriodLevel) IsNearHighLow(price float64, tolerance float64) (bool, string) {
	if abs(price - pl.High) <= tolerance {
		return true, "high"
	}
	if abs(price - pl.Low) <= tolerance {
		return true, "low"
	}
	return false, ""
}

// Helper function for absolute value
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// GetPeriodBoundaries returns the start and end times for a given period
func GetPeriodBoundaries(period PeriodType, referenceTime time.Time) (start, end time.Time) {
	// Use UTC for consistency
	ref := referenceTime.UTC()
	
	switch period {
	case PeriodDaily:
		start = time.Date(ref.Year(), ref.Month(), ref.Day(), 0, 0, 0, 0, time.UTC)
		end = start.AddDate(0, 0, 1)
		
	case PeriodWeekly:
		// Find the most recent Monday
		weekday := int(ref.Weekday())
		if weekday == 0 { // Sunday
			weekday = 7
		}
		daysToMonday := weekday - 1
		start = time.Date(ref.Year(), ref.Month(), ref.Day()-daysToMonday, 0, 0, 0, 0, time.UTC)
		end = start.AddDate(0, 0, 7)
		
	case PeriodMonthly:
		start = time.Date(ref.Year(), ref.Month(), 1, 0, 0, 0, 0, time.UTC)
		end = start.AddDate(0, 1, 0)
		
	case PeriodYearly:
		start = time.Date(ref.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		end = start.AddDate(1, 0, 0)
		
	default:
		// Default to daily
		start = time.Date(ref.Year(), ref.Month(), ref.Day(), 0, 0, 0, 0, time.UTC)
		end = start.AddDate(0, 0, 1)
	}
	
	return start, end
}

// IsPeriodActive checks if the period is currently active
func IsPeriodActive(period PeriodType, startTime time.Time) bool {
	now := time.Now().UTC()
	_, endTime := GetPeriodBoundaries(period, startTime)
	return now.Before(endTime) && now.After(startTime)
}