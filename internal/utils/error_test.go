package utils

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestErrorHelpers(t *testing.T) {
	notFound := NewNotFoundError("data source")
	wrapped := fmt.Errorf("load data source: %w", notFound)
	unknown := NewErrorBuilder("CUSTOM_ERROR").Build()
	outer := NewErrorBuilder(ErrCodeConflict).WithCause(notFound).Build()

	tests := []struct {
		name       string
		err        error
		code       string
		wantType   bool
		wantStatus int
	}{
		{"direct", notFound, ErrCodeNotFound, true, http.StatusNotFound},
		{"direct different code", notFound, ErrCodeConflict, false, http.StatusNotFound},
		{"wrapped", wrapped, ErrCodeNotFound, true, http.StatusNotFound},
		{"wrapped twice", fmt.Errorf("request: %w", wrapped), ErrCodeNotFound, true, http.StatusNotFound},
		{"wrapped different code", wrapped, ErrCodeConflict, false, http.StatusNotFound},
		{"unrelated", errors.New("unrelated failure"), ErrCodeNotFound, false, http.StatusInternalServerError},
		{"nil", nil, ErrCodeNotFound, false, http.StatusInternalServerError},
		{"unknown code", unknown, "CUSTOM_ERROR", true, http.StatusInternalServerError},
		{"wrapped unknown code", fmt.Errorf("request: %w", unknown), "CUSTOM_ERROR", true, http.StatusInternalServerError},
		{"outer app error", outer, ErrCodeConflict, true, http.StatusConflict},
		{"inner app error is not selected", outer, ErrCodeNotFound, false, http.StatusConflict},
		{"wrapped outer app error", fmt.Errorf("request: %w", outer), ErrCodeConflict, true, http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsErrorType(tt.err, tt.code); got != tt.wantType {
				t.Errorf("IsErrorType(%v, %q) = %v, want %v", tt.err, tt.code, got, tt.wantType)
			}
			if got := GetErrorStatus(tt.err); got != tt.wantStatus {
				t.Errorf("GetErrorStatus(%v) = %d, want %d", tt.err, got, tt.wantStatus)
			}
		})
	}
}
