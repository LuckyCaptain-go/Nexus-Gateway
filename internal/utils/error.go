package utils

import (
	"errors"
	"fmt"
)

// Common application errors
var (
	ErrInvalidRequest     = errors.New("invalid request")
	ErrValidationFailed   = errors.New("validation failed")
	ErrUnauthorized       = errors.New("unauthorized access")
	ErrForbidden          = errors.New("forbidden access")
	ErrNotFound           = errors.New("resource not found")
	ErrConflict           = errors.New("resource conflict")
	ErrInternalServer     = errors.New("internal server error")
	ErrDatabaseConnection = errors.New("database connection error")
	ErrQueryExecution     = errors.New("query execution error")
	ErrSQLSyntax          = errors.New("SQL syntax error")
	ErrSQLInjection       = errors.New("potential SQL injection detected")
	ErrConnectionTimeout  = errors.New("connection timeout")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
)

// AppError represents an application error with additional context
type AppError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
	Cause   error  `json:"-"`
}

func (e *AppError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("%s: %s - %s", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error {
	return e.Cause
}

// NewAppError creates a new application error
func NewAppError(code, message, details string, cause error) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Details: details,
		Cause:   cause,
	}
}

// WrapError wraps an existing error with application context
func WrapError(cause error, code, message string) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// IsAppError checks if error is an AppError
func IsAppError(err error) (*AppError, bool) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr, true
	}
	return nil, false
}

// Error codes
const (
	// Request errors
	ErrCodeInvalidRequest     = "INVALID_REQUEST"
	ErrCodeValidationFailed   = "VALIDATION_FAILED"
	ErrCodeMissingParameter   = "MISSING_PARAMETER"
	ErrCodeInvalidParameter   = "INVALID_PARAMETER"

	// Authentication/Authorization errors
	ErrCodeUnauthorized       = "UNAUTHORIZED"
	ErrCodeForbidden          = "FORBIDDEN"
	ErrCodeInvalidToken       = "INVALID_TOKEN"
	ErrCodeTokenExpired       = "TOKEN_EXPIRED"

	// Resource errors
	ErrCodeNotFound           = "NOT_FOUND"
	ErrCodeAlreadyExists      = "ALREADY_EXISTS"
	ErrCodeConflict           = "CONFLICT"

	// Database errors
	ErrCodeDatabaseError      = "DATABASE_ERROR"
	ErrCodeConnectionFailed   = "CONNECTION_FAILED"
	ErrCodeQueryFailed        = "QUERY_FAILED"
	ErrCodeSQLSyntaxError     = "SQL_SYNTAX_ERROR"
	ErrCodeSQLInjection       = "SQL_INJECTION_DETECTED"
	ErrCodeQueryTimeout       = "QUERY_TIMEOUT"

	// Data source errors
	ErrCodeDataSourceNotFound = "DATASOURCE_NOT_FOUND"
	ErrCodeDataSourceExists   = "DATASOURCE_EXISTS"
	ErrCodeInvalidDataSource  = "INVALID_DATASOURCE"
	ErrCodeDataSourceInactive = "DATASOURCE_INACTIVE"

	// System errors
	ErrCodeInternalError      = "INTERNAL_ERROR"
	ErrCodeServiceUnavailable = "SERVICE_UNAVAILABLE"
	ErrCodeRateLimitExceeded  = "RATE_LIMIT_EXCEEDED"
	ErrCodeMaintenance        = "MAINTENANCE"
)