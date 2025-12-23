package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
	"nexus-gateway/pkg/response"
)

// RateLimiterConfig configuration for rate limiting
type RateLimiterConfig struct {
	// Requests per minute
	RPM int `json:"rpm"`
	// Burst size
	Burst int `json:"burst"`
	// Cleanup interval for inactive clients
	CleanupInterval time.Duration `json:"cleanupInterval"`
}

// DefaultRateLimiterConfig returns default configuration
func DefaultRateLimiterConfig() RateLimiterConfig {
	return RateLimiterConfig{
		RPM:             60, // 60 requests per minute
		Burst:           10, // Allow bursts of up to 10 requests
		CleanupInterval: 5 * time.Minute,
	}
}

// RateLimiter implements rate limiting middleware
type RateLimiter struct {
	config RateLimiterConfig
	clients map[string]*ClientLimiter
	mutex   sync.RWMutex
}

// ClientLimiter represents rate limiter for a specific client
type ClientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config RateLimiterConfig) *RateLimiter {
	if config.RPM <= 0 {
		config.RPM = 60
	}
	if config.Burst <= 0 {
		config.Burst = 10
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 5 * time.Minute
	}

	rl := &RateLimiter{
		config: config,
		clients: make(map[string]*ClientLimiter),
	}

	// Start cleanup goroutine
	go rl.cleanup()

	return rl
}

// RateLimit creates a rate limiting middleware
func (rl *RateLimiter) RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientID := rl.getClientID(c)

		rl.mutex.Lock()
		if _, exists := rl.clients[clientID]; !exists {
			rl.clients[clientID] = &ClientLimiter{
				limiter:  rate.NewLimiter(rate.Every(time.Minute/time.Duration(rl.config.RPM)), rl.config.Burst),
				lastSeen: time.Now(),
			}
		}

		client := rl.clients[clientID]
		client.lastSeen = time.Now()
		rl.mutex.Unlock()

		if !client.limiter.Allow() {
			rl.rateLimitExceeded(c)
			return
		}

		// Add rate limit headers
		c.Header("X-RateLimit-Limit", string(rune(rl.config.RPM)))
		c.Header("X-RateLimit-Remaining", string(rune(client.limiter.Tokens())))

		c.Next()
	}
}

// getClientID extracts client identifier for rate limiting
func (rl *RateLimiter) getClientID(c *gin.Context) string {
	// Priority order for client ID:
	// 1. User ID (if authenticated)
	// 2. API Key (if provided)
	// 3. IP Address

	// Try user ID first
	if userID, exists := c.Get("user_id"); exists {
		if id, ok := userID.(string); ok {
			return "user:" + id
		}
	}

	// Try API key
	if apiKey := c.GetHeader("X-API-Key"); apiKey != "" {
		return "apikey:" + apiKey
	}

	// Fall back to IP address
	clientIP := c.ClientIP()
	if clientIP == "" {
		clientIP = "unknown"
	}
	return "ip:" + clientIP
}

// rateLimitExceeded handles rate limit exceeded scenario
func (rl *RateLimiter) rateLimitExceeded(c *gin.Context) {
	c.JSON(http.StatusTooManyRequests, response.ErrorResponse(
		"RATE_LIMIT_EXCEEDED",
		"Rate limit exceeded. Please try again later.",
		"Maximum "+string(rune(rl.config.RPM))+" requests per minute allowed",
		rl.getCorrelationID(c),
	))
	c.Abort()
}

// getCorrelationID extracts correlation ID from context
func (rl *RateLimiter) getCorrelationID(c *gin.Context) string {
	if correlationID, exists := c.Get("correlation_id"); exists {
		if id, ok := correlationID.(string); ok {
			return id
		}
	}
	return ""
}

// cleanup removes inactive clients
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(rl.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		rl.mutex.Lock()
		now := time.Now()

		for clientID, client := range rl.clients {
			// Remove clients that haven't been seen for the cleanup interval
			if now.Sub(client.lastSeen) > rl.config.CleanupInterval {
				delete(rl.clients, clientID)
			}
		}

		rl.mutex.Unlock()
	}
}

// GetStats returns current rate limiting statistics
func (rl *RateLimiter) GetStats() RateLimitStats {
	rl.mutex.RLock()
	defer rl.mutex.RUnlock()

	stats := RateLimitStats{
		ActiveClients: len(rl.clients),
		Config:        rl.config,
	}

	return stats
}

// RateLimitStats contains rate limiting statistics
type RateLimitStats struct {
	ActiveClients int                `json:"activeClients"`
	Config        RateLimiterConfig  `json:"config"`
}

// Global rate limiter instance
var globalRateLimiter *RateLimiter

// InitRateLimiter initializes the global rate limiter
func InitRateLimiter(config RateLimiterConfig) {
	globalRateLimiter = NewRateLimiter(config)
}

// GetRateLimiter returns the global rate limiter
func GetRateLimiter() *RateLimiter {
	return globalRateLimiter
}

// RateLimitMiddleware creates a rate limiting middleware using global limiter
func RateLimitMiddleware() gin.HandlerFunc {
	if globalRateLimiter == nil {
		// Initialize with default config if not already done
		InitRateLimiter(DefaultRateLimiterConfig())
	}
	return globalRateLimiter.RateLimit()
}

// Custom rate limiting for specific endpoints

// EndpointRateLimiter allows different rate limits for different endpoints
type EndpointRateLimiter struct {
	limiters map[string]*RateLimiter
	mutex    sync.RWMutex
}

// NewEndpointRateLimiter creates a new endpoint rate limiter
func NewEndpointRateLimiter() *EndpointRateLimiter {
	return &EndpointRateLimiter{
		limiters: make(map[string]*RateLimiter),
	}
}

// AddEndpoint adds rate limiting for a specific endpoint
func (erl *EndpointRateLimiter) AddEndpoint(path string, config RateLimiterConfig) {
	erl.mutex.Lock()
	defer erl.mutex.Unlock()

	erl.limiters[path] = NewRateLimiter(config)
}

// GetEndpointMiddleware returns middleware for a specific endpoint
func (erl *EndpointRateLimiter) GetEndpointMiddleware(path string) gin.HandlerFunc {
	erl.mutex.RLock()
	limiter, exists := erl.limiters[path]
	erl.mutex.RUnlock()

	if !exists {
		// Return default rate limiter if no specific limiter configured
		return RateLimitMiddleware()
	}

	return limiter.RateLimit()
}

// Path-based rate limiting middleware
func (erl *EndpointRateLimiter) RateLimitByPath() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.FullPath()
		middleware := erl.GetEndpointMiddleware(path)
		middleware(c)
	}
}