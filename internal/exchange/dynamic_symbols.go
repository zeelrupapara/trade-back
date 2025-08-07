package exchange

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
)

// DynamicSymbolManager manages dynamic symbol subscriptions based on market watch
type DynamicSymbolManager struct {
	hub          *Hub
	redis        *cache.RedisClient
	logger       *logrus.Entry
	
	// Current subscribed symbols
	subscribedSymbols map[string]bool
	mu                sync.RWMutex
	
	// Polling configuration
	pollInterval      time.Duration
	
	// Synchronization
	done             chan struct{}
	wg               sync.WaitGroup
}

// NewDynamicSymbolManager creates a new dynamic symbol manager
func NewDynamicSymbolManager(hub *Hub, redis *cache.RedisClient, logger *logrus.Logger) *DynamicSymbolManager {
	return &DynamicSymbolManager{
		hub:               hub,
		redis:             redis,
		logger:            logger.WithField("component", "dynamic-symbols"),
		subscribedSymbols: make(map[string]bool),
		pollInterval:      5 * time.Second, // Check for changes every 5 seconds
		done:              make(chan struct{}),
	}
}

// Start starts monitoring market watch symbols
func (dsm *DynamicSymbolManager) Start(ctx context.Context) error {
	// dsm.logger.Info("Starting dynamic symbol manager")
	
	// Initial load
	if err := dsm.updateSubscriptions(ctx); err != nil {
		return err
	}
	
	// Start monitoring
	dsm.wg.Add(1)
	go dsm.monitor(ctx)
	
	return nil
}

// Stop stops the dynamic symbol manager
func (dsm *DynamicSymbolManager) Stop() {
	// dsm.logger.Info("Stopping dynamic symbol manager")
	close(dsm.done)
	dsm.wg.Wait()
}

// monitor continuously monitors for market watch changes
func (dsm *DynamicSymbolManager) monitor(ctx context.Context) {
	defer dsm.wg.Done()
	
	ticker := time.NewTicker(dsm.pollInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-dsm.done:
			return
		case <-ticker.C:
			if err := dsm.updateSubscriptions(ctx); err != nil {
				dsm.logger.WithError(err).Error("Failed to update subscriptions")
			}
		}
	}
}

// updateSubscriptions updates symbol subscriptions based on all market watches
func (dsm *DynamicSymbolManager) updateSubscriptions(ctx context.Context) error {
	// Get all market watch symbols from Redis
	allSymbols, err := dsm.getAllMarketWatchSymbols(ctx)
	if err != nil {
		return err
	}
	
	dsm.mu.Lock()
	defer dsm.mu.Unlock()
	
	// Find symbols to add
	toAdd := make([]string, 0)
	for symbol := range allSymbols {
		if !dsm.subscribedSymbols[symbol] {
			toAdd = append(toAdd, symbol)
		}
	}
	
	// Find symbols to remove
	toRemove := make([]string, 0)
	for symbol := range dsm.subscribedSymbols {
		if !allSymbols[symbol] {
			toRemove = append(toRemove, symbol)
		}
	}
	
	// Apply changes
	if len(toAdd) > 0 || len(toRemove) > 0 {
		dsm.logger.WithFields(logrus.Fields{
			"adding":    toAdd,
			"removing":  toRemove,
			"add_count": len(toAdd),
			"remove_count": len(toRemove),
		})
		
		// Update subscribed symbols map
		for _, symbol := range toAdd {
			dsm.subscribedSymbols[symbol] = true
		}
		for _, symbol := range toRemove {
			delete(dsm.subscribedSymbols, symbol)
		}
		
		// Update connection pool with new symbol list
		newSymbols := make([]string, 0, len(dsm.subscribedSymbols))
		for symbol := range dsm.subscribedSymbols {
			newSymbols = append(newSymbols, symbol)
		}
		
		// Restart connection pool with new symbols
		if err := dsm.hub.UpdateSymbols(ctx, newSymbols); err != nil {
			return err
		}
	}
	
	return nil
}

// getAllMarketWatchSymbols gets all unique symbols from all user market watches
func (dsm *DynamicSymbolManager) getAllMarketWatchSymbols(ctx context.Context) (map[string]bool, error) {
	
	// Get all session tokens
	tokens, err := dsm.redis.GetAllSessionTokens(ctx)
	if err != nil {
		dsm.logger.WithError(err).Error("Failed to get session tokens")
		return nil, err
	}
	
	// dsm.logger.WithFields(logrus.Fields{
	// 	"token_count": len(tokens),
	// 	"tokens": tokens,
	// })
	
	// Collect all unique symbols
	uniqueSymbols := make(map[string]bool)
	for _, token := range tokens {
		symbols, err := dsm.redis.GetMarketWatch(ctx, token)
		if err != nil {
			dsm.logger.WithError(err).WithField("token", token).Warn("Failed to get market watch")
			continue
		}
		
		// dsm.logger.WithFields(logrus.Fields{
		// 	"token": token,
		// 	"symbols": symbols,
		// 	"count": len(symbols),
		// })
		
		for _, symbol := range symbols {
			uniqueSymbols[symbol] = true
		}
	}
	
	return uniqueSymbols, nil
}

// GetSubscribedSymbols returns currently subscribed symbols
func (dsm *DynamicSymbolManager) GetSubscribedSymbols() []string {
	dsm.mu.RLock()
	defer dsm.mu.RUnlock()
	
	symbols := make([]string, 0, len(dsm.subscribedSymbols))
	for symbol := range dsm.subscribedSymbols {
		symbols = append(symbols, symbol)
	}
	return symbols
}