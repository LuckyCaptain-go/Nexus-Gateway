package config

import (
	"fmt"
	"strings"
)

// knownPlaceholderSecrets intercepts placeholder JWT secrets that shipped in
// code defaults, docs, or the historical default config. The list only
// catches known defaults; real strength is enforced by minJWTSecretLength.
var knownPlaceholderSecrets = map[string]struct{}{
	"your-secret-key": {},
	"your-super-secret-jwt-key-change-in-production": {},
	"your-jwt-secret": {},
	"change-me":       {},
	"changeme":        {},
	"secret":          {},
}

// minJWTSecretLength matches the HMAC-SHA256 digest size recommendation.
const minJWTSecretLength = 32

const jwtSecretHelp = "set the JWT_SECRET environment variable or jwt_secret in configs/config.yaml (generate one with: openssl rand -base64 32)"

// ValidateSecurity fails closed on insecure authentication settings:
// authentication cannot be disabled in release mode, and enabled
// authentication requires a real secret. It runs before the metadata
// database is touched so misconfiguration is reported without side effects.
func (c *Config) ValidateSecurity() error {
	if c.Server.Mode == "release" && !c.Security.EnableAuth {
		return fmt.Errorf("security.enable_auth cannot be disabled in release mode: enable authentication or run an explicit non-release mode")
	}

	if !c.Security.EnableAuth {
		return nil
	}

	secret := strings.TrimSpace(c.Security.JWTSecret)
	if secret == "" {
		return fmt.Errorf("security.jwt_secret is required when security.enable_auth is true: %s", jwtSecretHelp)
	}
	if _, placeholder := knownPlaceholderSecrets[strings.ToLower(secret)]; placeholder {
		return fmt.Errorf("security.jwt_secret is a known placeholder value: %s", jwtSecretHelp)
	}
	if len(secret) < minJWTSecretLength {
		return fmt.Errorf("security.jwt_secret must be at least %d bytes when authentication is enabled: %s", minJWTSecretLength, jwtSecretHelp)
	}
	if c.Security.JWTExpiration <= 0 {
		return fmt.Errorf("security.jwt_expiration must be positive when security.enable_auth is true")
	}
	return nil
}
