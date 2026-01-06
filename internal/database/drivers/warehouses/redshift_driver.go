package warehouses

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"time"

	"nexus-gateway/internal/model"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

// RedshiftDriver implements Driver interface for Amazon Redshift
type RedshiftDriver struct {
	region         string
	clusterID      string
	iamRoleARN     string
	rdsDataService *rdsdata.Service
	useIAM         bool
}

// NewRedshiftDriver creates a new Redshift driver
func NewRedshiftDriver(region, clusterID string) (*RedshiftDriver, error) {
	return &RedshiftDriver{
		region:    region,
		clusterID: clusterID,
		useIAM:    false,
	}, nil
}

// NewRedshiftDriverWithIAM creates a Redshift driver with IAM authentication
func NewRedshiftDriverWithIAM(region, clusterID, iamRoleARN string) (*RedshiftDriver, error) {
	return &RedshiftDriver{
		region:     region,
		clusterID:  clusterID,
		iamRoleARN: iamRoleARN,
		useIAM:     true,
	}, nil
}

// Open opens a connection to Redshift (via PostgreSQL driver)
func (d *RedshiftDriver) Open(dsn string) (*sql.DB, error) {
	// Redshift is PostgreSQL-compatible
	// Use github.com/lib/pq driver
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open Redshift connection: %w", err)
	}

	return db, nil
}

// ValidateDSN validates the connection string
func (d *RedshiftDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Redshift port
func (d *RedshiftDriver) GetDefaultPort() int {
	return 5439
}

// BuildDSN builds a connection string from configuration
func (d *RedshiftDriver) BuildDSN(config *model.DataSourceConfig) string {
	// Redshift DSN: host=%s port=%d dbname=%s user=%s password=%s
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		config.Host, config.Port, config.Database, config.Username, config.Password)
}

// GetDatabaseTypeName returns the database type name
func (d *RedshiftDriver) GetDatabaseTypeName() string {
	return "redshift"
}

// TestConnection tests if the connection is working
func (d *RedshiftDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *RedshiftDriver) GetDriverName() string {
	return "redshift-postgres"
}

// GetCategory returns the driver category
func (d *RedshiftDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryWarehouse
}

// GetCapabilities returns driver capabilities
func (d *RedshiftDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   d.useIAM,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures IAM authentication
func (d *RedshiftDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// GetSchema retrieves schema information
func (d *RedshiftDriver) GetSchema(ctx context.Context, db *sql.DB, schemaName string) (map[string][]string, error) {
	// Redshift uses PostgreSQL information schema
	query := `
		SELECT table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = $1
		ORDER BY table_name, ordinal_position
	`

	rows, err := db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := make(map[string][]string)
	for rows.Next() {
		var table, column, dataType string
		if err := rows.Scan(&table, &column, &dataType); err != nil {
			return nil, err
		}
		schema[table] = append(schema[table], column)
	}

	return schema, nil
}


// GetClusterStatus retrieves cluster status
func (d *RedshiftDriver) GetClusterStatus(ctx context.Context) (*RedshiftClusterStatus, error) {
	// Would use Redshift API
	return &RedshiftClusterStatus{
		ClusterID: d.clusterID,
		Status:    "unknown",
	}, nil
}

// RedshiftClusterStatus contains cluster status
type RedshiftClusterStatus struct {
	ClusterID string
	Status    string
	Nodes     int
}
