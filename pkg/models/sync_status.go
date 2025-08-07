package models

import "time"

// SyncStatus represents the current synchronization status of a symbol
type SyncStatus struct {
	Symbol    string    `json:"symbol"`
	Status    string    `json:"status"`    // "pending", "syncing", "completed", "failed"
	Progress  int       `json:"progress"`  // 0-100 percentage
	TotalBars int       `json:"total_bars"`
	Error     string    `json:"error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SyncStatusUpdate represents a batch of sync status updates
type SyncStatusUpdate struct {
	Symbols []SyncStatus `json:"symbols"`
}