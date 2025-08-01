package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// Manager is a simplified session manager
type Manager struct {
	mysqlDB *database.MySQLClient
	nats    *messaging.NATSClient
	logger  *logrus.Entry
	
	// Session tracking
	sessions   map[string]*models.TradingSession
	sessionsMu sync.RWMutex
	
	// Control
	running bool
	done    chan struct{}
	wg      sync.WaitGroup
}

// NewManager creates a new session manager
func NewManager(
	mysqlDB *database.MySQLClient,
	nats *messaging.NATSClient,
	logger *logrus.Logger,
) *Manager {
	return &Manager{
		mysqlDB:  mysqlDB,
		nats:     nats,
		logger:   logger.WithField("component", "session-manager"),
		sessions: make(map[string]*models.TradingSession),
		done:     make(chan struct{}),
	}
}

// Start starts the session manager
func (sm *Manager) Start(ctx context.Context) error {
	if sm.running {
		return fmt.Errorf("session manager already running")
	}
	
	sm.logger.Info("Starting session manager...")
	
	// Load sessions from database
	if err := sm.loadSessions(); err != nil {
		return fmt.Errorf("failed to load sessions: %w", err)
	}
	
	sm.running = true
	
	sm.logger.Info("Session manager started successfully")
	return nil
}

// Stop stops the session manager
func (sm *Manager) Stop() error {
	if !sm.running {
		return nil
	}
	
	sm.logger.Info("Stopping session manager...")
	
	close(sm.done)
	sm.running = false
	
	// Wait for goroutines
	sm.wg.Wait()
	
	sm.logger.Info("Session manager stopped successfully")
	return nil
}

// loadSessions loads trading session definitions from database
func (sm *Manager) loadSessions() error {
	sessions, err := sm.mysqlDB.GetTradingSessions(context.Background())
	if err != nil {
		return fmt.Errorf("failed to query sessions: %w", err)
	}
	
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()
	
	for _, session := range sessions {
		sm.sessions[session.Name] = session
		sm.logger.WithFields(logrus.Fields{
			"name": session.Name,
		}).Info("Loaded trading session")
	}
	
	return nil
}

// GetSessions returns all sessions
func (sm *Manager) GetSessions() map[string]*models.TradingSession {
	sm.sessionsMu.RLock()
	defer sm.sessionsMu.RUnlock()
	
	sessions := make(map[string]*models.TradingSession)
	for k, v := range sm.sessions {
		sessions[k] = v
	}
	return sessions
}

// IsSessionActive checks if any session is currently active
func (sm *Manager) IsSessionActive() bool {
	// For now, assume markets are always active for crypto
	return true
}

// GetStats returns session manager statistics
func (sm *Manager) GetStats() map[string]interface{} {
	sm.sessionsMu.RLock()
	defer sm.sessionsMu.RUnlock()
	
	stats := map[string]interface{}{
		"total_sessions": len(sm.sessions),
		"running":        sm.running,
	}
	
	return stats
}