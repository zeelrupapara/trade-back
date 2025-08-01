package exchange

import (
	"context"
	"fmt"
	"time"
)

// logErrorRateLimited logs errors with rate limiting to prevent log spam
func (h *Hub) logErrorRateLimited(err error, message string) {
	h.errorLogMu.Lock()
	defer h.errorLogMu.Unlock()
	
	now := time.Now()
	// Only log errors once every 5 seconds per error type
	if now.Sub(h.lastErrorLog) >= 5*time.Second {
		errorCount := h.stats.ErrorCount.Load()
		h.logger.WithFields(map[string]interface{}{
			"error":       err,
			"error_count": errorCount,
			"since_last":  now.Sub(h.lastErrorLog),
		}).Error(message)
		
		h.lastErrorLog = now
		
		// Log warning if errors are persistent
		if errorCount > 100 {
			h.logger.WithField("error_count", errorCount).
				Warn("High error rate detected. Check system health.")
		}
	}
}

// UpdateSymbols dynamically updates the symbols the hub is subscribed to
func (h *Hub) UpdateSymbols(ctx context.Context, newSymbols []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if !h.running.Load() {
		return fmt.Errorf("hub is not running")
	}
	
	h.logger.WithField("count", len(newSymbols)).Info("Updating hub symbols")
	
	// Stop current connection pool
	if err := h.pool.Stop(); err != nil {
		h.logger.WithError(err).Error("Failed to stop connection pool")
	}
	
	// Update symbols
	h.symbols = newSymbols
	
	// Create new connection pool with updated symbols
	h.pool = NewConnectionPool(newSymbols, &h.cfg.Exchange, h.logger.Logger)
	h.pool.SetPriceHandler(h.processor.ProcessPrice)
	
	// Start new connection pool
	if err := h.pool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start new connection pool: %w", err)
	}
	
	h.logger.WithField("symbols", len(newSymbols)).Info("Hub symbols updated successfully")
	return nil
}