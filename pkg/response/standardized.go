package response

import (
	"time"

	"nexus-gateway/internal/utils"
)

// StandardResponse represents a standardized API response
type StandardResponse struct {
	Success       bool        `json:"success"`
	Data          interface{} `json:"data,omitempty"`
	Error         *ErrorInfo  `json:"error,omitempty"`
	Message       string      `json:"message,omitempty"`
	CorrelationID string      `json:"correlationId"`
	Timestamp     time.Time   `json:"timestamp"`
}

// ErrorInfo represents error information in responses
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// SuccessResponse creates a successful response
func SuccessResponse(data interface{}, correlationID string) *StandardResponse {
	return &StandardResponse{
		Success:       true,
		Data:          data,
		CorrelationID: correlationID,
		Timestamp:     time.Now(),
	}
}

// SuccessMessageResponse creates a successful response with a message
func SuccessMessageResponse(message, correlationID string) *StandardResponse {
	return &StandardResponse{
		Success:       true,
		Message:       message,
		CorrelationID: correlationID,
		Timestamp:     time.Now(),
	}
}

// ErrorResponse creates an error response
func ErrorResponse(code, message, details, correlationID string) *StandardResponse {
	return &StandardResponse{
		Success: false,
		Error: &ErrorInfo{
			Code:    code,
			Message: message,
			Details: details,
		},
		CorrelationID: correlationID,
		Timestamp:     time.Now(),
	}
}

// ErrorResponseFromAppError creates an error response from AppError
func ErrorResponseFromAppError(appErr *utils.AppError, correlationID string) *StandardResponse {
	return &StandardResponse{
		Success: false,
		Error: &ErrorInfo{
			Code:    appErr.Code,
			Message: appErr.Message,
			Details: appErr.Details,
		},
		CorrelationID: correlationID,
		Timestamp:     time.Now(),
	}
}

// ValidationErrorResponse creates a validation error response
func ValidationErrorResponse(message string, correlationID string) *StandardResponse {
	return ErrorResponse(utils.ErrCodeValidationFailed, message, "", correlationID)
}

// NotFoundResponse creates a not found error response
func NotFoundResponse(message string, correlationID string) *StandardResponse {
	return ErrorResponse(utils.ErrCodeNotFound, message, "", correlationID)
}

// ConflictResponse creates a conflict error response
func ConflictResponse(message string, correlationID string) *StandardResponse {
	return ErrorResponse(utils.ErrCodeConflict, message, "", correlationID)
}

// InternalServerErrorResponse creates an internal server error response
func InternalServerErrorResponse(correlationID string) *StandardResponse {
	return ErrorResponse(utils.ErrCodeInternalError, "An internal error occurred", "", correlationID)
}

// UnauthorizedResponse creates an unauthorized error response
func UnauthorizedResponse(message string, correlationID string) *StandardResponse {
	if message == "" {
		message = "Unauthorized access"
	}
	return ErrorResponse(utils.ErrCodeUnauthorized, message, "", correlationID)
}

// ForbiddenResponse creates a forbidden error response
func ForbiddenResponse(message string, correlationID string) *StandardResponse {
	if message == "" {
		message = "Forbidden access"
	}
	return ErrorResponse(utils.ErrCodeForbidden, message, "", correlationID)
}
