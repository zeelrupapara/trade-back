package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"
)

// LoginRequest represents a login request
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse represents a login response
type LoginResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	UserID    string    `json:"user_id"`
}

// SessionInfo represents session information
type SessionInfo struct {
	Token     string
	UserID    string
	Username  string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// handleLogin handles user login requests
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate credentials
	if req.Username == "" || req.Password == "" {
		http.Error(w, "Username and password required", http.StatusBadRequest)
		return
	}

	// For demo purposes, accept any non-empty credentials
	// In production, validate against database
	// TODO: Implement proper authentication
	if req.Username == "demo" && req.Password == "demo123" {
		// Generate session token
		token, err := generateSessionToken()
		if err != nil {
			http.Error(w, "Failed to generate session", http.StatusInternalServerError)
			return
		}

		// Create session
		session := &SessionInfo{
			Token:     token,
			UserID:    "user_" + req.Username,
			Username:  req.Username,
			CreatedAt: time.Now(),
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}

		// Store session in Redis
		ctx := context.Background()
		sessionKey := "session:" + token
		if err := s.redisCache.SetJSON(ctx, sessionKey, session, 24*time.Hour); err != nil {
			http.Error(w, "Failed to create session", http.StatusInternalServerError)
			return
		}

		// Return session token
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(LoginResponse{
			Token:     token,
			ExpiresAt: session.ExpiresAt,
			UserID:    session.UserID,
		})
	} else {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
	}
}

// handleLogout handles user logout requests
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Session-Token")
	if token == "" {
		http.Error(w, "Session token required", http.StatusUnauthorized)
		return
	}

	// Delete session from Redis
	ctx := context.Background()
	sessionKey := "session:" + token
	if err := s.redisCache.Delete(ctx, sessionKey); err != nil {
		// Log error but don't fail the logout
		s.logger.WithError(err).Error("Failed to delete session")
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"message": "Logged out successfully",
	})
}

// handleGetSession returns current session info
func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Session-Token")
	if token == "" {
		http.Error(w, "Session token required", http.StatusUnauthorized)
		return
	}

	// Get session from Redis
	ctx := context.Background()
	sessionKey := "session:" + token
	
	var session SessionInfo
	exists, err := s.redisCache.GetJSON(ctx, sessionKey, &session)
	if err != nil {
		http.Error(w, "Failed to get session", http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "Invalid or expired session", http.StatusUnauthorized)
		return
	}

	// Check if session is expired
	if time.Now().After(session.ExpiresAt) {
		// Delete expired session
		s.redisCache.Delete(ctx, sessionKey)
		http.Error(w, "Session expired", http.StatusUnauthorized)
		return
	}

	// Return session info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user_id":    session.UserID,
		"username":   session.Username,
		"created_at": session.CreatedAt,
		"expires_at": session.ExpiresAt,
		"valid":      true,
	})
}

// validateSession validates a session token
func (s *Server) validateSession(token string) (*SessionInfo, error) {
	ctx := context.Background()
	sessionKey := "session:" + token
	
	var session SessionInfo
	exists, err := s.redisCache.GetJSON(ctx, sessionKey, &session)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	// Check if session is expired
	if time.Now().After(session.ExpiresAt) {
		// Delete expired session
		s.redisCache.Delete(ctx, sessionKey)
		return nil, nil
	}

	return &session, nil
}

// generateSessionToken generates a secure random session token
func generateSessionToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// authMiddleware validates session for protected endpoints
func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-Session-Token")
		if token == "" {
			http.Error(w, "Session token required", http.StatusUnauthorized)
			return
		}

		session, err := s.validateSession(token)
		if err != nil {
			http.Error(w, "Failed to validate session", http.StatusInternalServerError)
			return
		}

		if session == nil {
			http.Error(w, "Invalid or expired session", http.StatusUnauthorized)
			return
		}

		// Add session info to request context
		ctx := context.WithValue(r.Context(), "session", session)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}