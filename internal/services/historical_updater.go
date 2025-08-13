package services

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/pkg/config"
)

// HistoricalUpdater runs as a background service to periodically update historical data
type HistoricalUpdater struct {
	loader *HistoricalLoader
	mysql  *database.MySQLClient
	logger *logrus.Entry
	cfg    *config.Config

	// Control
	running bool
	done    chan struct{}
	wg      sync.WaitGroup
}

// NewHistoricalUpdater creates a new historical data updater service
func NewHistoricalUpdater(
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	cfg *config.Config,
	logger *logrus.Logger,
) *HistoricalUpdater {
	return &HistoricalUpdater{
		loader: NewHistoricalLoader(influx, mysql, cfg, logger),
		mysql:  mysql,
		logger: logger.WithField("component", "historical-updater"),
		cfg:    cfg,
		done:   make(chan struct{}),
	}
}

// Start starts the background updater
func (h *HistoricalUpdater) Start(ctx context.Context) error {
	if h.running {
		return nil
	}

	h.running = true
	h.logger.Info("Starting historical data updater")

	// Start update loop
	h.wg.Add(1)
	go h.updateLoop(ctx)

	return nil
}

// Stop stops the background updater
func (h *HistoricalUpdater) Stop() error {
	if !h.running {
		return nil
	}

	h.logger.Info("Stopping historical data updater")
	close(h.done)
	h.wg.Wait()
	h.running = false

	return nil
}

// updateLoop runs the periodic update cycle
func (h *HistoricalUpdater) updateLoop(ctx context.Context) {
	defer h.wg.Done()

	// Initial update on startup
	h.performUpdate(ctx)

	// Update every 6 hours
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case <-ticker.C:
			h.performUpdate(ctx)
		}
	}
}

// performUpdate performs the actual update
func (h *HistoricalUpdater) performUpdate(ctx context.Context) {
	h.logger.Info("Performing historical data update")

	// Update recent data (last 2 days of 1h data)
	if err := h.loader.LoadAllSymbols(ctx, "1h", 2); err != nil {
		h.logger.WithError(err).Error("Failed to update hourly data")
	}

	// Update daily data (last 7 days)
	if err := h.loader.LoadAllSymbols(ctx, "1d", 7); err != nil {
		h.logger.WithError(err).Error("Failed to update daily data")
	}

	// Recalculate ATH/ATL
	if err := h.loader.LoadATHATL(ctx); err != nil {
		h.logger.WithError(err).Error("Failed to update ATH/ATL")
	}

	h.logger.Info("Historical data update completed")
}

// ForceUpdate triggers an immediate update
func (h *HistoricalUpdater) ForceUpdate(ctx context.Context) error {
	h.logger.Info("Force update requested")
	h.performUpdate(ctx)
	return nil
}
