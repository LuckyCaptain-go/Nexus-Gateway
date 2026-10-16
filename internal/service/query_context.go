package service

import (
	"context"
	"time"
)

const (
	defaultQueryTimeoutSeconds    = 60
	maxRequestQueryTimeoutSeconds = 1800
)

func withQueryTimeout(ctx context.Context, timeoutSeconds int) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = defaultQueryTimeoutSeconds
	}
	if timeoutSeconds > maxRequestQueryTimeoutSeconds {
		timeoutSeconds = maxRequestQueryTimeoutSeconds
	}
	return context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
}
