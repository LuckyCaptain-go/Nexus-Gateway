package warehouses

import (
	"context"
	"fmt"
	"time"
)

// DatabricksStatementPoller handles polling for statement completion
type DatabricksStatementPoller struct {
	client       *DatabricksRESTClient
	pollInterval time.Duration
	maxAttempts  int
}

// NewDatabricksStatementPoller creates a new statement poller
func NewDatabricksStatementPoller(client *DatabricksRESTClient) *DatabricksStatementPoller {
	return &DatabricksStatementPoller{
		client:       client,
		pollInterval: 1 * time.Second,
		maxAttempts:  300, // 5 minutes max with 1s polling
	}
}

// ExecuteAndWait executes a statement and waits for completion
func (p *DatabricksStatementPoller) ExecuteAndWait(ctx context.Context, sql string) (*DatabricksQueryResult, error) {
	// Execute the statement
	execution, err := p.client.ExecuteStatement(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	// Poll for completion
	return p.waitForCompletion(ctx, execution.StatementID)
}

// waitForCompletion polls until statement completes
func (p *DatabricksStatementPoller) waitForCompletion(ctx context.Context, statementID string) (*DatabricksQueryResult, error) {
	attempts := 0

	for {
		attempts++
		if attempts > p.maxAttempts {
			return nil, fmt.Errorf("statement timeout after %d attempts", attempts)
		}

		// Check status
		execution, err := p.client.GetStatementStatus(ctx, statementID)
		if err != nil {
			return nil, fmt.Errorf("failed to get statement status: %w", err)
		}

		// Check if completed
		switch execution.Status.State {
		case "SUCCEEDED":
			// Fetch results
			return p.client.GetStatementResults(ctx, statementID, 0)
		case "FAILED", "CANCELED":
			if execution.Status.Error != nil {
				return nil, fmt.Errorf("statement %s: %s", execution.Status.State, execution.Status.Error.Message)
			}
			return nil, fmt.Errorf("statement %s", execution.Status.State)
		case "PENDING", "RUNNING":
			// Continue polling
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(p.pollInterval):
				continue
			}
		default:
			return nil, fmt.Errorf("unknown statement state: %s", execution.Status.State)
		}
	}
}

// PollWithCallback polls and calls callback for each status update
func (p *DatabricksStatementPoller) PollWithCallback(ctx context.Context, statementID string, callback func(*DatabricksStatementExecution) error) (*DatabricksQueryResult, error) {
	attempts := 0

	for {
		attempts++
		if attempts > p.maxAttempts {
			return nil, fmt.Errorf("statement timeout after %d attempts", attempts)
		}

		execution, err := p.client.GetStatementStatus(ctx, statementID)
		if err != nil {
			return nil, fmt.Errorf("failed to get statement status: %w", err)
		}

		// Call callback
		if err := callback(execution); err != nil {
			return nil, fmt.Errorf("callback error: %w", err)
		}

		// Check state
		switch execution.Status.State {
		case "SUCCEEDED":
			return p.client.GetStatementResults(ctx, statementID, 0)
		case "FAILED", "CANCELED":
			if execution.Status.Error != nil {
				return nil, fmt.Errorf("statement %s: %s", execution.Status.State, execution.Status.Error.Message)
			}
			return nil, fmt.Errorf("statement %s", execution.Status.State)
		case "PENDING", "RUNNING":
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(p.pollInterval):
				continue
			}
		default:
			return nil, fmt.Errorf("unknown statement state: %s", execution.Status.State)
		}
	}
}

// StreamResults streams results as they become available
func (p *DatabricksStatementPoller) StreamResults(ctx context.Context, statementID string, offset int64, callback func(*DatabricksQueryResult) error) error {
	for {
		result, err := p.client.GetStatementResults(ctx, statementID, offset)
		if err != nil {
			return fmt.Errorf("failed to get results: %w", err)
		}

		// Call callback with results
		if err := callback(result); err != nil {
			return err
		}

		// Check if more results available
		if len(result.Data) == 0 || len(result.Data) < 10000 {
			break
		}

		offset += int64(len(result.Data))
	}

	return nil
}

// GetStatementProgress retrieves statement progress information
func (p *DatabricksStatementPoller) GetStatementProgress(ctx context.Context, statementID string) (*DatabricksStatementProgress, error) {
	execution, err := p.client.GetStatementStatus(ctx, statementID)
	if err != nil {
		return nil, err
	}

	progress := &DatabricksStatementProgress{
		StatementID: statementID,
		State:       execution.Status.State,
	}

	// Calculate progress if available
	if execution.Status.Error != nil {
		progress.Error = execution.Status.Error.Message
	}

	return progress, nil
}

// DatabricksStatementProgress represents statement progress
type DatabricksStatementProgress struct {
	StatementID    string
	State          string
	RowsProcessed  int64
	BytesProcessed int64
	Error          string
}

// CancelAndWait cancels a statement and waits for cancellation
func (p *DatabricksStatementPoller) CancelAndWait(ctx context.Context, statementID string) error {
	// Send cancel request
	if err := p.client.CancelStatement(ctx, statementID); err != nil {
		return err
	}

	// Wait for cancellation
	attempts := 0
	for {
		attempts++
		if attempts > 30 { // 30 seconds max
			return fmt.Errorf("cancel timeout")
		}

		execution, err := p.client.GetStatementStatus(ctx, statementID)
		if err != nil {
			return err
		}

		if execution.Status.State == "CANCELED" {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			continue
		}
	}
}

// SetPollInterval sets custom polling interval
func (p *DatabricksStatementPoller) SetPollInterval(interval time.Duration) {
	p.pollInterval = interval
}

// SetMaxAttempts sets maximum polling attempts
func (p *DatabricksStatementPoller) SetMaxAttempts(maxAttempts int) {
	p.maxAttempts = maxAttempts
}

// BatchExecute executes multiple statements concurrently
func (p *DatabricksStatementPoller) BatchExecute(ctx context.Context, statements []string) ([]*DatabricksQueryResult, []error) {
	results := make([]*DatabricksQueryResult, len(statements))
	errors := make([]error, len(statements))

	// Execute all statements
	statementIDs := make([]string, len(statements))
	for i, sql := range statements {
		execution, err := p.client.ExecuteStatement(ctx, sql)
		if err != nil {
			errors[i] = err
			continue
		}
		statementIDs[i] = execution.StatementID
	}

	// Wait for all to complete
	for i, statementID := range statementIDs {
		if statementID == "" {
			continue
		}

		result, err := p.waitForCompletion(ctx, statementID)
		if err != nil {
			errors[i] = err
		} else {
			results[i] = result
		}
	}

	return results, errors
}

// ExecuteWithRetry executes statement with retry on transient errors
func (p *DatabricksStatementPoller) ExecuteWithRetry(ctx context.Context, sql string, maxRetries int) (*DatabricksQueryResult, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := p.ExecuteAndWait(ctx, sql)
		if err == nil {
			return result, nil
		}

		// Check if error is transient
		if !isTransientError(err) {
			return nil, err
		}

		lastErr = err

		// Wait before retry with exponential backoff
		if attempt < maxRetries {
			backoff := time.Duration(attempt+1) * time.Second
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// isTransientError checks if error is transient
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	transientErrors := []string{
		"connection reset",
		"timeout",
		"temporary failure",
		"rate limit",
		"service unavailable",
	}

	for _, msg := range transientErrors {
		if contains(errMsg, msg) {
			return true
		}
	}

	return false
}

// GetStatementMetrics retrieves metrics about statement execution
func (p *DatabricksStatementPoller) GetStatementMetrics(ctx context.Context, statementID string) (*DatabricksStatementMetrics, error) {
	// This would require querying the execution history
	// For now, return basic metrics
	return &DatabricksStatementMetrics{
		StatementID: statementID,
	}, nil
}

// DatabricksStatementMetrics contains statement execution metrics
type DatabricksStatementMetrics struct {
	StatementID   string
	ExecutionTime time.Duration
	RowsScanned   int64
	BytesScanned  int64
	RowsProduced  int64
	DurationMs    int64
}

// MonitorLongRunningQuery monitors long-running queries
func (p *DatabricksStatementPoller) MonitorLongRunningQuery(ctx context.Context, statementID string, threshold time.Duration, callback func(*DatabricksStatementProgress) error) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			progress, err := p.GetStatementProgress(ctx, statementID)
			if err != nil {
				return err
			}

			// Check if exceeded threshold
			elapsed := time.Since(startTime)
			if elapsed > threshold && progress.State == "RUNNING" {
				progress.Elapsed = elapsed
				if err := callback(progress); err != nil {
					return err
				}
			}

			// Check if completed
			if progress.State == "SUCCEEDED" || progress.State == "FAILED" || progress.State == "CANCELED" {
				return nil
			}
		}
	}
}
