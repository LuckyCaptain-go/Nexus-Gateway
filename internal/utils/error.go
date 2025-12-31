package utils

import (
	"fmt"
	"net/http"
)

// Error codes with HTTP status mapping
const (
	// General errors
	ErrCodeInvalidRequest     = "INVALID_REQUEST"
	ErrCodeValidationFailed   = "VALIDATION_ERROR"
	ErrCodeUnauthorized       = "UNAUTHORIZED"
	ErrCodeForbidden          = "FORBIDDEN"
	ErrCodeNotFound           = "NOT_FOUND"
	ErrCodeConflict           = "CONFLICT"
	ErrCodeInternalError      = "INTERNAL_ERROR"
	ErrCodeServiceUnavailable = "SERVICE_UNAVAILABLE"
	ErrCodeRateLimitExceeded  = "RATE_LIMIT_EXCEEDED"

	// Database errors
	ErrCodeDatabaseError     = "DATABASE_ERROR"
	ErrCodeConnectionFailed  = "CONNECTION_FAILED"
	ErrCodeQueryFailed       = "QUERY_FAILED"
	ErrCodeSQLSyntaxError    = "SQL_SYNTAX_ERROR"
	ErrCodeSQLInjection      = "SQL_INJECTION_DETECTED"
	ErrCodeQueryTimeout      = "QUERY_TIMEOUT"
	ErrCodeTransactionFailed = "TRANSACTION_FAILED"

	// Data source errors
	ErrCodeDataSourceNotFound = "DATASOURCE_NOT_FOUND"
	ErrCodeDataSourceExists   = "DATASOURCE_EXISTS"
	ErrCodeInvalidDataSource  = "INVALID_DATASOURCE"
	ErrCodeDataSourceInactive = "DATASOURCE_INACTIVE"
	ErrCodeInvalidCredentials = "INVALID_CREDENTIALS"

	// Authentication errors
	ErrCodeTokenExpired       = "TOKEN_EXPIRED"
	ErrCodeInvalidToken       = "INVALID_TOKEN"
	ErrCodeTokenRefreshFailed = "TOKEN_REFRESH_FAILED"

	// Security errors
	ErrCodeForbiddenAccess        = "FORBIDDEN_ACCESS"
	ErrCodeInsufficientPrivileges = "INSUFFICIENT_PRIVILEGES"

	// Validation error codes
	ErrCodeInvalidUUID       = "INVALID_UUID"
	ErrCodeInvalidJSON       = "INVALID_JSON"
	ErrCodeInvalidParameters = "INVALID_PARAMETERS"
)

// HTTPStatus maps error codes to HTTP status codes
var HTTPStatus = map[string]int{
	ErrCodeInvalidRequest:     http.StatusBadRequest,
	ErrCodeValidationFailed:   http.StatusUnprocessableEntity,
	ErrCodeUnauthorized:       http.StatusUnauthorized,
	ErrCodeForbidden:          http.StatusForbidden,
	ErrCodeNotFound:           http.StatusNotFound,
	ErrCodeConflict:           http.StatusConflict,
	ErrCodeInternalError:      http.StatusInternalServerError,
	ErrCodeServiceUnavailable: http.StatusServiceUnavailable,
	ErrCodeRateLimitExceeded:  http.StatusTooManyRequests,

	ErrCodeDatabaseError:     http.StatusInternalServerError,
	ErrCodeConnectionFailed:  http.StatusServiceUnavailable,
	ErrCodeQueryFailed:       http.StatusInternalServerError,
	ErrCodeSQLSyntaxError:    http.StatusBadRequest,
	ErrCodeSQLInjection:      http.StatusForbidden,
	ErrCodeQueryTimeout:      http.StatusRequestTimeout,
	ErrCodeTransactionFailed: http.StatusInternalServerError,

	ErrCodeDataSourceNotFound: http.StatusNotFound,
	ErrCodeDataSourceExists:   http.StatusConflict,
	ErrCodeInvalidDataSource:  http.StatusBadRequest,
	ErrCodeDataSourceInactive: http.StatusServiceUnavailable,
	ErrCodeInvalidCredentials: http.StatusUnauthorized,

	ErrCodeTokenExpired:       http.StatusUnauthorized,
	ErrCodeInvalidToken:       http.StatusUnauthorized,
	ErrCodeTokenRefreshFailed: http.StatusInternalServerError,

	ErrCodeForbiddenAccess:        http.StatusForbidden,
	ErrCodeInsufficientPrivileges: http.StatusForbidden,

	ErrCodeInvalidUUID:       http.StatusBadRequest,
	ErrCodeInvalidJSON:       http.StatusBadRequest,
	ErrCodeInvalidParameters: http.StatusBadRequest,
}

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

// ErrorBuilder provides a fluent interface for creating errors
type ErrorBuilder struct {
	code    string
	message string
	details string
	cause   error
	status  int
}

// NewErrorBuilder creates a new error builder
func NewErrorBuilder(code string) *ErrorBuilder {
	return &ErrorBuilder{
		code:   code,
		status: HTTPStatus[code],
	}
}

// WithMessage sets the error message
func (eb *ErrorBuilder) WithMessage(message string) *ErrorBuilder {
	eb.message = message
	return eb
}

// WithDetails sets the error details
func (eb *ErrorBuilder) WithDetails(details string) *ErrorBuilder {
	eb.details = details
	return eb
}

// WithCause sets the underlying error cause
func (eb *ErrorBuilder) WithCause(cause error) *ErrorBuilder {
	eb.cause = cause
	return eb
}

// WithStatus sets the HTTP status code
func (eb *ErrorBuilder) WithStatus(status int) *ErrorBuilder {
	eb.status = status
	return eb
}

// Build constructs the final AppError
func (eb *ErrorBuilder) Build() *AppError {
	if eb.message == "" {
		eb.message = getDefaultMessage(eb.code)
	}

	return &AppError{
		Code:    eb.code,
		Message: eb.message,
		Details: eb.details,
		Cause:   eb.cause,
	}
}

// getDefaultMessage returns a default message for error codes
func getDefaultMessage(code string) string {
	messages := map[string]string{
		ErrCodeInvalidRequest:     "The request is invalid",
		ErrCodeValidationFailed:   "Validation failed",
		ErrCodeUnauthorized:       "Unauthorized access",
		ErrCodeForbidden:          "Access forbidden",
		ErrCodeNotFound:           "Resource not found",
		ErrCodeConflict:           "Resource conflict",
		ErrCodeInternalError:      "Internal server error",
		ErrCodeServiceUnavailable: "Service temporarily unavailable",
		ErrCodeRateLimitExceeded:  "Rate limit exceeded",

		ErrCodeDatabaseError:     "Database error",
		ErrCodeConnectionFailed:  "Database connection failed",
		ErrCodeQueryFailed:       "Query execution failed",
		ErrCodeSQLSyntaxError:    "SQL syntax error",
		ErrCodeSQLInjection:      "Potential SQL injection detected",
		ErrCodeQueryTimeout:      "Query timeout",
		ErrCodeTransactionFailed: "Transaction failed",

		ErrCodeDataSourceNotFound: "Data source not found",
		ErrCodeDataSourceExists:   "Data source already exists",
		ErrCodeInvalidDataSource:  "Invalid data source configuration",
		ErrCodeDataSourceInactive: "Data source is inactive",
		ErrCodeInvalidCredentials: "Invalid credentials",

		ErrCodeTokenExpired:       "Token expired",
		ErrCodeInvalidToken:       "Invalid token",
		ErrCodeTokenRefreshFailed: "Token refresh failed",

		ErrCodeForbiddenAccess:        "Access forbidden",
		ErrCodeInsufficientPrivileges: "Insufficient privileges",

		ErrCodeInvalidUUID:       "Invalid UUID format",
		ErrCodeInvalidJSON:       "Invalid JSON format",
		ErrCodeInvalidParameters: "Invalid parameters",
	}

	if msg, exists := messages[code]; exists {
		return msg
	}
	return "Unknown error"
}

// Convenience functions for common error types
func NewDatabaseError(cause error, details string) *AppError {
	return NewErrorBuilder(ErrCodeDatabaseError).
		WithCause(cause).
		WithDetails(details).
		Build()
}

func NewNotFoundError(resource string) *AppError {
	return NewErrorBuilder(ErrCodeNotFound).
		WithMessage(fmt.Sprintf("%s not found", resource)).
		Build()
}

func NewConflictError(resource string, details string) *AppError {
	return NewErrorBuilder(ErrCodeConflict).
		WithMessage(fmt.Sprintf("%s conflict", resource)).
		WithDetails(details).
		Build()
}

func NewValidationError(message string, details string) *AppError {
	return NewErrorBuilder(ErrCodeValidationFailed).
		WithMessage(message).
		WithDetails(details).
		Build()
}

func NewAuthenticationError(message string) *AppError {
	return NewErrorBuilder(ErrCodeUnauthorized).
		WithMessage(message).
		Build()
}

func NewAuthorizationError(message string) *AppError {
	return NewErrorBuilder(ErrCodeForbidden).
		WithMessage(message).
		Build()
}

// IsErrorType checks if an error matches a specific error code
func IsErrorType(err error, code string) bool {
	if appErr, ok := err.(*AppError); ok {
		return appErr.Code == code
	}
	return false
}

// GetErrorStatus returns the HTTP status code for an error
func GetErrorStatus(err error) int {
	if appErr, ok := err.(*AppError); ok {
		if status, exists := HTTPStatus[appErr.Code]; exists {
			return status
		}
	}
	return http.StatusInternalServerError
}
