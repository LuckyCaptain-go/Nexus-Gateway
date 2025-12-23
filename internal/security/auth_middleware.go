package security

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"nexus-gateway/pkg/response"
)

// AuthMiddleware provides JWT authentication middleware
type AuthMiddleware struct {
	jwtManager *JWTManager
}

// NewAuthMiddleware creates a new AuthMiddleware
func NewAuthMiddleware(jwtManager *JWTManager) *AuthMiddleware {
	return &AuthMiddleware{
		jwtManager: jwtManager,
	}
}

// RequireAuth creates a middleware that requires authentication
func (am *AuthMiddleware) RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, response.UnauthorizedResponse(
				"Authorization header is required",
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		// Extract token from header
		token, err := am.jwtManager.ExtractTokenFromHeader(authHeader)
		if err != nil {
			c.JSON(http.StatusUnauthorized, response.UnauthorizedResponse(
				err.Error(),
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		// Validate token
		claims, err := am.jwtManager.ValidateToken(token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, response.UnauthorizedResponse(
				"Invalid or expired token",
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		// Store claims in context
		c.Set("user_claims", claims)
		c.Set("user_id", claims.UserID)
		c.Set("username", claims.Username)
		c.Set("user_roles", claims.Roles)

		c.Next()
	}
}

// RequireRole creates a middleware that requires specific role
func (am *AuthMiddleware) RequireRole(role string) gin.HandlerFunc {
	return am.combineMiddleware(
		am.RequireAuth(),
		am.roleMiddleware(role),
	)
}

// RequireAnyRole creates a middleware that requires any of the specified roles
func (am *AuthMiddleware) RequireAnyRole(roles ...string) gin.HandlerFunc {
	return am.combineMiddleware(
		am.RequireAuth(),
		am.anyRoleMiddleware(roles...),
	)
}

// OptionalAuth creates a middleware that optionally validates authentication
func (am *AuthMiddleware) OptionalAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			// No authorization header, continue without authentication
			c.Next()
			return
		}

		// Try to authenticate
		token, err := am.jwtManager.ExtractTokenFromHeader(authHeader)
		if err != nil {
			// Invalid header format, continue without authentication
			c.Next()
			return
		}

		claims, err := am.jwtManager.ValidateToken(token)
		if err != nil {
			// Invalid token, continue without authentication
			c.Next()
			return
		}

		// Store claims in context
		c.Set("user_claims", claims)
		c.Set("user_id", claims.UserID)
		c.Set("username", claims.Username)
		c.Set("user_roles", claims.Roles)

		c.Next()
	}
}

// SkipAuth creates a middleware that skips authentication
func (am *AuthMiddleware) SkipAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
	}
}

// Role-based middleware

func (am *AuthMiddleware) roleMiddleware(requiredRole string) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, exists := c.Get("user_claims")
		if !exists {
			c.JSON(http.StatusUnauthorized, response.UnauthorizedResponse(
				"User claims not found",
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		userClaims, ok := claims.(*Claims)
		if !ok {
			c.JSON(http.StatusInternalServerError, response.InternalServerErrorResponse(
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		if !userClaims.HasRole(requiredRole) {
			c.JSON(http.StatusForbidden, response.ForbiddenResponse(
				"Insufficient permissions",
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		c.Next()
	}
}

func (am *AuthMiddleware) anyRoleMiddleware(requiredRoles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, exists := c.Get("user_claims")
		if !exists {
			c.JSON(http.StatusUnauthorized, response.UnauthorizedResponse(
				"User claims not found",
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		userClaims, ok := claims.(*Claims)
		if !ok {
			c.JSON(http.StatusInternalServerError, response.InternalServerErrorResponse(
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		if !userClaims.HasAnyRole(requiredRoles...) {
			c.JSON(http.StatusForbidden, response.ForbiddenResponse(
				"Insufficient permissions",
				am.getCorrelationID(c),
			))
			c.Abort()
			return
		}

		c.Next()
	}
}

// Helper functions

func (am *AuthMiddleware) combineMiddleware(middlewares ...gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		for _, middleware := range middlewares {
			middleware(c)
			if c.IsAborted() {
				return
			}
		}
		c.Next()
	}
}

func (am *AuthMiddleware) getCorrelationID(c *gin.Context) string {
	if correlationID, exists := c.Get("correlation_id"); exists {
		if id, ok := correlationID.(string); ok {
			return id
		}
	}
	return ""
}

// Helper functions for getting user info from context

// GetUserID extracts user ID from context
func GetUserID(c *gin.Context) (string, bool) {
	userID, exists := c.Get("user_id")
	if !exists {
		return "", false
	}
	id, ok := userID.(string)
	return id, ok
}

// GetUsername extracts username from context
func GetUsername(c *gin.Context) (string, bool) {
	username, exists := c.Get("username")
	if !exists {
		return "", false
	}
	name, ok := username.(string)
	return name, ok
}

// GetUserRoles extracts user roles from context
func GetUserRoles(c *gin.Context) ([]string, bool) {
	roles, exists := c.Get("user_roles")
	if !exists {
		return nil, false
	}
	userRoles, ok := roles.([]string)
	return userRoles, ok
}

// GetUserClaims extracts user claims from context
func GetUserClaims(c *gin.Context) (*Claims, bool) {
	claims, exists := c.Get("user_claims")
	if !exists {
		return nil, false
	}
	userClaims, ok := claims.(*Claims)
	return userClaims, ok
}

// IsAuthenticated checks if user is authenticated
func IsAuthenticated(c *gin.Context) bool {
	_, exists := c.Get("user_claims")
	return exists
}

// HasRole checks if user has specific role
func HasRole(c *gin.Context, role string) bool {
	claims, exists := GetUserClaims(c)
	if !exists {
		return false
	}
	return claims.HasRole(role)
}

// HasAnyRole checks if user has any of the specified roles
func HasAnyRole(c *gin.Context, roles ...string) bool {
	claims, exists := GetUserClaims(c)
	if !exists {
		return false
	}
	return claims.HasAnyRole(roles...)
}