package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// OpenGaussRowSecurity handles row-level security features
type OpenGaussRowSecurity struct {
	driver *OpenGaussDriver
}

// NewOpenGaussRowSecurity creates a new row-level security handler
func NewOpenGaussRowSecurity(driver *OpenGaussDriver) *OpenGaussRowSecurity {
	return &OpenGaussRowSecurity{
		driver: driver,
	}
}

// EnableRowSecurity enables row-level security for a table
func (r *OpenGaussRowSecurity) EnableRowSecurity(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to enable row security: %w", err)
	}
	return nil
}

// DisableRowSecurity disables row-level security for a table
func (r *OpenGaussRowSecurity) DisableRowSecurity(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s DISABLE ROW LEVEL SECURITY", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to disable row security: %w", err)
	}
	return nil
}

// CreateRowSecurityPolicy creates a row-level security policy
func (r *OpenGaussRowSecurity) CreateRowSecurityPolicy(ctx context.Context, db *sql.DB, policy *OpenGaussRLSPolicy) error {
	sql := fmt.Sprintf("CREATE ROW LEVEL SECURITY POLICY %s ON %s USING %s",
		policy.PolicyName, policy.TableName, policy.UsingExpression)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create row security policy: %w", err)
	}
	return nil
}

// DropRowSecurityPolicy drops a row-level security policy
func (r *OpenGaussRowSecurity) DropRowSecurityPolicy(ctx context.Context, db *sql.DB, policyName, tableName string) error {
	sql := fmt.Sprintf("DROP ROW LEVEL SECURITY POLICY %s ON %s", policyName, tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop row security policy: %w", err)
	}
	return nil
}

// ListRowSecurityPolicies lists row-level security policies for a table
func (r *OpenGaussRowSecurity) ListRowSecurityPolicies(ctx context.Context, db *sql.DB, tableName string) ([]OpenGaussRLSPolicy, error) {
	sql := `
		SELECT POLICY_NAME, TABLE_NAME, COMMAND, USING_EXPRESSION
		FROM INFORMATION_SCHEMA.ROW_SECURITY_POLICIES
		WHERE TABLE_NAME = ?
	`

	rows, err := db.QueryContext(ctx, sql, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to list row security policies: %w", err)
	}
	defer rows.Close()

	var policies []OpenGaussRLSPolicy

	for rows.Next() {
		var policy OpenGaussRLSPolicy
		err := rows.Scan(
			&policy.PolicyName,
			&policy.TableName,
			&policy.Command,
			&policy.UsingExpression,
		)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// OpenGaussRLSPolicy represents row-level security policy
type OpenGaussRLSPolicy struct {
	PolicyName         string
	TableName          string
	Command            string // ALL, SELECT, INSERT, UPDATE, DELETE
	UsingExpression    string
	WithCheckExpression string
}

// AssignRowSecurityPolicy assigns a policy to a role
func (r *OpenGaussRowSecurity) AssignRowSecurityPolicy(ctx context.Context, db *sql.DB, roleName, policyName, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to assign policy: %w", err)
	}
	return nil
}

// IsRowSecurityEnabled checks if row-level security is enabled for a table
func (r *OpenGaussRowSecurity) IsRowSecurityEnabled(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	sql := `
		SELECT RELROWSECURITY
		FROM PG_CLASS
		WHERE RELNAME = ?
	`

	var enabled bool
	err := db.QueryRowContext(ctx, sql, tableName).Scan(&enabled)
	if err != nil {
		return false, fmt.Errorf("failed to check row security status: %w", err)
	}

	return enabled, nil
}

// ForceRowSecurity forces row-level security for table owners
func (r *OpenGaussRowSecurity) ForceRowSecurity(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to force row security: %w", err)
	}
	return nil
}

// NoForceRowSecurity disables forced row-level security
func (r *OpenGaussRowSecurity) NoForceRowSecurity(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s NO FORCE ROW LEVEL SECURITY", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to disable forced row security: %w", err)
	}
	return nil
}
