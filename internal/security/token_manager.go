package security

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TokenManager handles automatic rotation of authentication tokens
type TokenManager struct {
	vault          *CredentialVault
	rotationCheck  time.Duration
	rotationBuffer time.Duration
	rotations      map[string]*tokenRotation
	rotationsMu    sync.RWMutex
	stopChan       chan struct{}
}

type tokenRotation struct {
	expiresAt    time.Time
	onRotate     func(ctx context.Context) (newToken string, expiresAt time.Time, err error)
	lastRotation time.Time
	mu           sync.RWMutex
}

// NewTokenManager creates a new token manager
func NewTokenManager(vault *CredentialVault) *TokenManager {
	return &TokenManager{
		vault:          vault,
		rotationCheck:  5 * time.Minute, // Check every 5 minutes
		rotationBuffer: 5 * time.Minute, // Rotate 5 minutes before expiration
		rotations:      make(map[string]*tokenRotation),
		stopChan:       make(chan struct{}),
	}
}

// RegisterToken registers a token for automatic rotation
func (tm *TokenManager) RegisterToken(dataSourceID string, expiresAt time.Time, onRotate func(ctx context.Context) (string, time.Time, error)) {
	tm.rotationsMu.Lock()
	defer tm.rotationsMu.Unlock()

	tm.rotations[dataSourceID] = &tokenRotation{
		expiresAt: expiresAt,
		onRotate:  onRotate,
	}
}

// Start begins the background rotation checker
func (tm *TokenManager) Start(ctx context.Context) {
	ticker := time.NewTicker(tm.rotationCheck)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tm.stopChan:
			return
		case <-ticker.C:
			tm.checkAndRotateTokens(ctx)
		}
	}
}

// Stop stops the background rotation checker
func (tm *TokenManager) Stop() {
	close(tm.stopChan)
}

// checkAndRotateTokens checks all registered tokens and rotates those expiring soon
func (tm *TokenManager) checkAndRotateTokens(ctx context.Context) {
	tm.rotationsMu.RLock()
	rotationCandidates := make(map[string]*tokenRotation)
	for id, rotation := range tm.rotations {
		rotationCandidates[id] = rotation
	}
	tm.rotationsMu.RUnlock()

	for id, rotation := range rotationCandidates {
		rotation.mu.Lock()
		needsRotation := time.Until(rotation.expiresAt) < tm.rotationBuffer
		rotation.mu.Unlock()

		if needsRotation {
			tm.RotateToken(ctx, id)
		}
	}
}

// RotateToken rotates a specific token
func (tm *TokenManager) RotateToken(ctx context.Context, dataSourceID string) {
	tm.rotationsMu.RLock()
	rotation := tm.rotations[dataSourceID]
	tm.rotationsMu.RUnlock()

	if rotation == nil {
		return
	}

	rotation.mu.Lock()
	defer rotation.mu.Unlock()

	// Call the rotation function
	newToken, expiresAt, err := rotation.onRotate(ctx)
	if err != nil {
		// Log error but don't stop future rotations
		return
	}

	// Update expiration
	rotation.expiresAt = expiresAt
	rotation.lastRotation = time.Now()

	// TODO: Store new token in database via CredentialVault
	_ = newToken
}

// RotateTokenWithResult rotates a specific token and returns the result
func (tm *TokenManager) RotateTokenWithResult(ctx context.Context, dataSourceID string) (string, time.Time, error) {
	tm.rotationsMu.RLock()
	rotation := tm.rotations[dataSourceID]
	tm.rotationsMu.RUnlock()

	if rotation == nil {
		return "", time.Time{}, fmt.Errorf("no rotation handler found for %s", dataSourceID)
	}

	rotation.mu.Lock()
	defer rotation.mu.Unlock()

	// Call the rotation function
	newToken, expiresAt, err := rotation.onRotate(ctx)
	if err != nil {
		return "", time.Time{}, err
	}

	// Update expiration
	rotation.expiresAt = expiresAt
	rotation.lastRotation = time.Now()

	// TODO: Store new token in database via CredentialVault
	_ = newToken

	return newToken, expiresAt, nil
}

// GetToken retrieves the current token for a data source
func (tm *TokenManager) GetToken(dataSourceID string) (string, error) {
	// TODO: Implement token retrieval from database
	return "", nil
}
