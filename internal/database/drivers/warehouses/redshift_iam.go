package warehouses

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
)

// RedshiftIAMAuthenticator handles IAM-based authentication for Redshift
type RedshiftIAMAuthenticator struct {
	region          string
	clusterID       string
	clusterEndpoint string // cluster endpoint (e.g., cluster-id.abc123 XYZ.us-west-2.redshift.amazonaws.com)
	port            int
	database        string
	dbUser          string // IAM database user
	awsConfig       *aws.Config
	tokenCache      *cachedToken
	mu              sync.RWMutex
}

// cachedToken holds a cached IAM token with expiration
type cachedToken struct {
	token      string
	expiration time.Time
}

// NewRedshiftIAMAuthenticator creates a new IAM authenticator for Redshift
func NewRedshiftIAMAuthenticator(region, clusterID, clusterEndpoint, dbUser, database string, port int) (*RedshiftIAMAuthenticator, error) {
	if region == "" {
		return nil, fmt.Errorf("region is required")
	}
	if clusterID == "" {
		return nil, fmt.Errorf("cluster ID is required")
	}
	if dbUser == "" {
		return nil, fmt.Errorf("database user is required")
	}

	return &RedshiftIAMAuthenticator{
		region:          region,
		clusterID:       clusterID,
		clusterEndpoint: clusterEndpoint,
		port:            port,
		database:        database,
		dbUser:          dbUser,
		tokenCache:      &cachedToken{},
	}, nil
}

// LoadAWSCredentials loads AWS credentials from the environment or credential chain
func (a *RedshiftIAMAuthenticator) LoadAWSCredentials(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(a.region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	a.awsConfig = &cfg
	return nil
}

// GetAuthToken generates a new IAM authentication token
func (a *RedshiftIAMAuthenticator) GetAuthToken(ctx context.Context) (string, error) {
	a.mu.RLock()
	if a.tokenCache != nil && time.Now().Before(a.tokenCache.expiration) {
		token := a.tokenCache.token
		a.mu.RUnlock()
		return token, nil
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock
	if a.tokenCache != nil && time.Now().Before(a.tokenCache.expiration) {
		return a.tokenCache.token, nil
	}

	if a.awsConfig == nil {
		return "", fmt.Errorf("AWS credentials not loaded, call LoadAWSCredentials first")
	}

	// Build endpoint for IAM token
	endpoint := a.buildEndpoint()

	// Generate IAM authentication token
	token, err := auth.BuildAuthToken(ctx, endpoint, a.region, a.dbUser, a.awsConfig.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to build auth token: %w", err)
	}

	// Cache token with 5 minute buffer before actual expiration (15 min lifetime)
	a.tokenCache = &cachedToken{
		token:      token,
		expiration: time.Now().Add(10 * time.Minute),
	}

	return token, nil
}

// buildEndpoint constructs the Redshift cluster endpoint
func (a *RedshiftIAMAuthenticator) buildEndpoint() string {
	if a.clusterEndpoint != "" {
		return fmt.Sprintf("%s:%d", a.clusterEndpoint, a.port)
	}
	return fmt.Sprintf("%s.%s.redshift.amazonaws.com:%d", a.clusterID, a.region, a.port)
}

// GetDSNWithIAMToken builds a DSN with IAM authentication token
func (a *RedshiftIAMAuthenticator) GetDSNWithIAMToken(ctx context.Context) (string, error) {
	token, err := a.GetAuthToken(ctx)
	if err != nil {
		return "", err
	}

	// Build DSN with IAM token
	// PostgreSQL DSN format: host:port:database:user:password
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		a.getHost(), a.port, a.database, a.dbUser, token)

	return dsn, nil
}

// getHost extracts the hostname from cluster endpoint or builds it
func (a *RedshiftIAMAuthenticator) getHost() string {
	if a.clusterEndpoint != "" {
		return a.clusterEndpoint
	}
	return fmt.Sprintf("%s.%s.redshift.amazonaws.com", a.clusterID, a.region)
}

// ConnectWithIAM establishes a database connection using IAM authentication
func (a *RedshiftIAMAuthenticator) ConnectWithIAM(ctx context.Context) (*sql.DB, error) {
	dsn, err := a.GetDSNWithIAMToken(ctx)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open Redshift connection: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping Redshift: %w", err)
	}

	return db, nil
}

// RefreshConnection refreshes the IAM token and returns a new connection
func (a *RedshiftIAMAuthenticator) RefreshConnection(ctx context.Context) (*sql.DB, error) {
	a.mu.Lock()
	// Invalidate cache
	a.tokenCache = &cachedToken{}
	a.mu.Unlock()

	return a.ConnectWithIAM(ctx)
}

// RedshiftIAMConnectionManager manages IAM-based connections with auto-refresh
type RedshiftIAMConnectionManager struct {
	authenticator *RedshiftIAMAuthenticator
	db            *sql.DB
	mu            sync.RWMutex
	lastRefresh   time.Time
	refreshBefore time.Duration // Refresh token before this duration (default 5 min)
}

// NewRedshiftIAMConnectionManager creates a connection manager with automatic token refresh
func NewRedshiftIAMConnectionManager(authenticator *RedshiftIAMAuthenticator) *RedshiftIAMConnectionManager {
	return &RedshiftIAMConnectionManager{
		authenticator: authenticator,
		refreshBefore: 5 * time.Minute,
	}
}

// GetConnection returns a valid database connection, refreshing token if needed
func (m *RedshiftIAMConnectionManager) GetConnection(ctx context.Context) (*sql.DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If no connection or token needs refresh, create new connection
	if m.db == nil || time.Since(m.lastRefresh) > m.refreshBefore {
		if m.db != nil {
			m.db.Close()
		}

		db, err := m.authenticator.ConnectWithIAM(ctx)
		if err != nil {
			return nil, err
		}

		m.db = db
		m.lastRefresh = time.Now()
	}

	return m.db, nil
}

// Close closes the database connection
func (m *RedshiftIAMConnectionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// RefreshConnection forces a token refresh and new connection
func (m *RedshiftIAMConnectionManager) RefreshConnection(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.db != nil {
		m.db.Close()
	}

	db, err := m.authenticator.RefreshConnection(ctx)
	if err != nil {
		return err
	}

	m.db = db
	m.lastRefresh = time.Now()
	return nil
}

// GetClusterInfo returns cluster connection information
func (a *RedshiftIAMAuthenticator) GetClusterInfo() *RedshiftClusterInfo {
	return &RedshiftClusterInfo{
		ClusterID:  a.clusterID,
		Region:     a.region,
		Endpoint:   a.buildEndpoint(),
		Database:   a.database,
		DBUser:     a.dbUser,
		Port:       a.port,
		AuthMethod: "IAM",
	}
}

// RedshiftClusterInfo contains Redshift cluster information
type RedshiftClusterInfo struct {
	ClusterID  string
	Region     string
	Endpoint   string
	Database   string
	DBUser     string
	Port       int
	AuthMethod string
}

// BuildIAMDSNFromConfig builds DSN from configuration map
func BuildIAMDSNFromConfig(config map[string]string) (string, error) {
	required := []string{"cluster_id", "region", "db_user", "database"}
	for _, key := range required {
		if config[key] == "" {
			return "", fmt.Errorf("missing required config: %s", key)
		}
	}

	clusterID := config["cluster_id"]
	region := config["region"]
	dbUser := config["db_user"]
	database := config["database"]
	port := 5439
	if config["port"] != "" {
		fmt.Sscanf(config["port"], "%d", &port)
	}

	auth, err := NewRedshiftIAMAuthenticator(region, clusterID, config["endpoint"], dbUser, database, port)
	if err != nil {
		return "", err
	}

	return auth.GetDSNWithIAMToken(context.Background())
}

