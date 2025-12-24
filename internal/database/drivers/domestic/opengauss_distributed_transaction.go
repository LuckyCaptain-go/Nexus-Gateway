package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// OpenGaussDistributedTransaction handles distributed transaction features
type OpenGaussDistributedTransaction struct {
	driver *OpenGaussDriver
}

// NewOpenGaussDistributedTransaction creates a new distributed transaction handler
func NewOpenGaussDistributedTransaction(driver *OpenGaussDriver) *OpenGaussDistributedTransaction {
	return &OpenGaussDistributedTransaction{
		driver: driver,
	}
}

// BeginDistributedTransaction begins a distributed transaction
func (t *OpenGaussDistributedTransaction) BeginDistributedTransaction(ctx context.Context, db *sql.DB) (sql.Tx, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin distributed transaction: %w", err)
	}

	// Enable two-phase commit
	_, err = tx.ExecContext(ctx, "SET xa = on")
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to enable XA: %w", err)
	}

	return tx, nil
}

// PrepareTransaction prepares a transaction for commit
func (t *OpenGaussDistributedTransaction) PrepareTransaction(ctx context.Context, tx *sql.Tx, xid string) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf("PREPARE TRANSACTION '%s'", xid))
	if err != nil {
		return fmt.Errorf("failed to prepare transaction: %w", err)
	}
	return nil
}

// CommitPreparedTransaction commits a prepared transaction
func (t *OpenGaussDistributedTransaction) CommitPreparedTransaction(ctx context.Context, db *sql.DB, xid string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("COMMIT PREPARED '%s'", xid))
	if err != nil {
		return fmt.Errorf("failed to commit prepared transaction: %w", err)
	}
	return nil
}

// RollbackPreparedTransaction rolls back a prepared transaction
func (t *OpenGaussDistributedTransaction) RollbackPreparedTransaction(ctx context.Context, db *sql.DB, xid string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("ROLLBACK PREPARED '%s'", xid))
	if err != nil {
		return fmt.Errorf("failed to rollback prepared transaction: %w", err)
	}
	return nil
}

// ListPreparedTransactions lists all prepared transactions
func (t *OpenGaussDistributedTransaction) ListPreparedTransactions(ctx context.Context, db *sql.DB) ([]OpenGaussXATransaction, error) {
	sql := `
		SELECT transaction, gid, prepared, owner, state
		FROM pg_prepared_xacts
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to list prepared transactions: %w", err)
	}
	defer rows.Close()

	var transactions []OpenGaussXATransaction

	for rows.Next() {
		var xa OpenGaussXATransaction
		err := rows.Scan(
			&xa.Transaction,
			&xa.GID,
			&xa.Prepared,
			&xa.Owner,
			&xa.State,
		)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, xa)
	}

	return transactions, nil
}

// OpenGaussXATransaction represents an XA transaction
type OpenGaussXATransaction struct {
	Transaction int
	GID         string
	Prepared    string
	Owner       string
	State       string
}

// GetTransactionInfo retrieves information about a transaction
func (t *OpenGaussDistributedTransaction) GetTransactionInfo(ctx context.Context, db *sql.DB, xid string) (*OpenGaussTransactionInfo, error) {
	sql := `
		SELECT xid, state, prepared, owner
		FROM pg_prepared_xacts
		WHERE gid = ?
	`

	info := &OpenGaussTransactionInfo{
		XID: xid,
	}

	err := db.QueryRowContext(ctx, sql, xid).Scan(
		&info.XID,
		&info.State,
		&info.Prepared,
		&info.Owner,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get transaction info: %w", err)
	}

	return info, nil
}

// OpenGaussTransactionInfo represents transaction information
type OpenGaussTransactionInfo struct {
	XID      string
	State    string
	Prepared bool
	Owner    string
	Database string
}

// SetTransactionTimeout sets the timeout for distributed transactions
func (t *OpenGaussDistributedTransaction) SetTransactionTimeout(ctx context.Context, db *sql.DB, timeout int) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET distributed_transaction_timeout = %d", timeout))
	if err != nil {
		return fmt.Errorf("failed to set transaction timeout: %w", err)
	}
	return nil
}

// EnableGlobalTable enables global table for distributed transactions
func (t *OpenGaussDistributedTransaction) EnableGlobalTable(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s SET (distributed_table = true)", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to enable global table: %w", err)
	}
	return nil
}
