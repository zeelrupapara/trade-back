package exchange

import (
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