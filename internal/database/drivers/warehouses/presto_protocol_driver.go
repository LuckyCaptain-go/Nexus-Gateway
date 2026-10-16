package warehouses

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/RhinoLink-Tech/data-fabric/internal/database/drivers"
	"github.com/RhinoLink-Tech/data-fabric/internal/model"
	presto "github.com/prestodb/presto-go-client/v2"
)

// PrestoProtocolDriver adapts Trino, Presto, and OpenLooKeng through their
// shared statement REST protocol. Trino uses the X-Trino header family; Presto
// and OpenLooKeng use the X-Presto header family.

type PrestoProtocolDriver struct {
	base *drivers.DriverBase
}

// TrinoDriver remains an alias for callers that use the original constructor.
type TrinoDriver = PrestoProtocolDriver

func NewTrinoDriver() *PrestoProtocolDriver {
	return newPrestoProtocolDriver(model.DatabaseTypeTrino)
}

func NewPrestoDriver() *PrestoProtocolDriver {
	return newPrestoProtocolDriver(model.DatabaseTypePresto)
}

func NewOpenLooKengDriver() *PrestoProtocolDriver {
	return newPrestoProtocolDriver(model.DatabaseTypeOpenLooKeng)
}

func newPrestoProtocolDriver(dbType model.DatabaseType) *PrestoProtocolDriver {
	return &PrestoProtocolDriver{
		base: drivers.NewDriverBase(dbType, drivers.CategoryQueryEngine),
	}
}

func (d *PrestoProtocolDriver) ApplyBatchPagination(query string, batchSize, offset int64) (string, error) {
	if batchSize <= 0 {
		return "", fmt.Errorf("batch size must be greater than 0")
	}
	if offset < 0 {
		return "", fmt.Errorf("offset cannot be negative")
	}
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, batchSize, offset), nil
}

func (d *PrestoProtocolDriver) Open(dsn string) (*sql.DB, error) {
	endpoint, err := d.parseEndpoint(dsn)
	if err != nil {
		return nil, err
	}

	connectorDSN, err := d.protocolDSN(endpoint)
	if err != nil {
		return nil, err
	}
	httpClient, err := newProtocolHTTPClient(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to configure %s TLS: %w", d.engineName(), err)
	}

	connector, err := presto.NewConnector(connectorDSN, presto.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("failed to create %s connector: %w", d.engineName(), err)
	}
	return sql.OpenDB(connector), nil
}

func (d *PrestoProtocolDriver) ValidateDSN(dsn string) error {
	_, err := d.parseEndpoint(dsn)
	return err
}

func (d *PrestoProtocolDriver) parseEndpoint(dsn string) (*url.URL, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid %s connection URL", d.engineName())
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%s connection URL scheme must be http or https", d.engineName())
	}
	if parsed.Hostname() == "" {
		return nil, fmt.Errorf("%s coordinator host is required", d.engineName())
	}
	if port := parsed.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, fmt.Errorf("%s coordinator port must be between 1 and 65535", d.engineName())
		}
	}
	if parsed.User == nil || strings.TrimSpace(parsed.User.Username()) == "" {
		return nil, fmt.Errorf("%s username is required", d.engineName())
	}
	if _, hasPassword := parsed.User.Password(); hasPassword && !endpointUsesTLS(parsed) {
		return nil, fmt.Errorf("%s password authentication requires HTTPS", d.engineName())
	}
	if strings.TrimSpace(parsed.Query().Get("catalog")) == "" {
		return nil, fmt.Errorf("%s catalog is required", d.engineName())
	}
	if parsed.Fragment != "" || parsed.Opaque != "" {
		return nil, fmt.Errorf("%s connection URL must not contain a fragment or opaque path", d.engineName())
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return nil, fmt.Errorf("%s connection URL must use catalog and schema query parameters", d.engineName())
	}
	if err := validateTLSProperties(parsed.Query()); err != nil {
		return nil, fmt.Errorf("invalid %s TLS properties: %w", d.engineName(), err)
	}
	return parsed, nil
}

func validateTLSProperties(query url.Values) error {
	cert, key := strings.TrimSpace(query.Get("ssl_cert")), strings.TrimSpace(query.Get("ssl_key"))
	if (cert == "") != (key == "") {
		return fmt.Errorf("ssl_cert and ssl_key must be provided together")
	}
	if value := strings.TrimSpace(query.Get("ssl_skip_verify")); value != "" {
		if _, err := strconv.ParseBool(value); err != nil && value != "1" && value != "0" {
			return fmt.Errorf("ssl_skip_verify must be a boolean")
		}
	}
	return nil
}

func endpointUsesTLS(endpoint *url.URL) bool {
	if endpoint.Scheme == "https" {
		return true
	}
	query := endpoint.Query()
	return query.Get("ssl_ca") != "" || query.Get("ssl_cert") != "" ||
		query.Get("ssl_key") != "" || query.Get("ssl_skip_verify") != ""
}

func (d *PrestoProtocolDriver) protocolDSN(endpoint *url.URL) (string, error) {
	query := endpoint.Query()
	catalog := strings.TrimSpace(query.Get("catalog"))
	schema := strings.TrimSpace(query.Get("schema"))
	query.Del("catalog")
	query.Del("schema")

	scheme := "presto"
	if d.base.GetDatabaseTypeName() == string(model.DatabaseTypeTrino) {
		scheme = "trino"
	}
	connectorURL := &url.URL{
		Scheme:   scheme,
		Host:     endpoint.Host,
		User:     endpoint.User,
		RawQuery: query.Encode(),
	}

	path := "/" + catalog
	rawPath := "/" + url.PathEscape(catalog)
	if schema != "" {
		path += "/" + schema
		rawPath += "/" + url.PathEscape(schema)
	}
	connectorURL.Path = path
	connectorURL.RawPath = rawPath
	return connectorURL.String(), nil
}

func newProtocolHTTPClient(endpoint *url.URL) (*http.Client, error) {
	tlsConfig := &tls.Config{}
	if value := strings.TrimSpace(endpoint.Query().Get("ssl_skip_verify")); value != "" {
		skipVerify := value == "1"
		if value != "1" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("ssl_skip_verify must be a boolean")
			}
			skipVerify = parsed
		}
		tlsConfig.InsecureSkipVerify = skipVerify
	}

	if caPath := strings.TrimSpace(endpoint.Query().Get("ssl_ca")); caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read ssl_ca: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ssl_ca contains no valid certificates")
		}
		tlsConfig.RootCAs = roots
	}

	certPath := strings.TrimSpace(endpoint.Query().Get("ssl_cert"))
	keyPath := strings.TrimSpace(endpoint.Query().Get("ssl_key"))
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if endpointUsesTLS(endpoint) {
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{
		Transport: &protocolRoundTripper{
			next:       transport,
			forceHTTPS: endpointUsesTLS(endpoint),
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

type protocolRoundTripper struct {
	next       http.RoundTripper
	forceHTTPS bool
}

func (rt *protocolRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if !rt.forceHTTPS || request.URL.Scheme == "https" {
		return rt.next.RoundTrip(request)
	}

	cloned := request.Clone(request.Context())
	urlCopy := *request.URL
	urlCopy.Scheme = "https"
	cloned.URL = &urlCopy
	if cloned.Host == "" {
		cloned.Host = urlCopy.Host
	}
	return rt.next.RoundTrip(cloned)
}

func (d *PrestoProtocolDriver) GetDefaultPort() int { return 8080 }

func (d *PrestoProtocolDriver) BuildDSN(config *model.DataSourceConfig) string {
	if config == nil {
		return ""
	}

	scheme := "http"
	if config.SSL {
		scheme = "https"
	}
	port := config.Port
	if port <= 0 {
		port = d.GetDefaultPort()
	}
	host := strings.Trim(config.Host, "[]")

	connectionURL := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
	}
	if config.Password == "" {
		connectionURL.User = url.User(config.Username)
	} else {
		connectionURL.User = url.UserPassword(config.Username, config.Password)
	}

	params := url.Values{}
	params.Set("catalog", config.Database)
	if config.Schema != "" {
		params.Set("schema", config.Schema)
	}
	if config.Timezone != "" {
		params.Set("timezone", config.Timezone)
	}
	for key, value := range config.AdditionalProps {
		if strings.EqualFold(key, "catalog") || strings.EqualFold(key, "schema") {
			continue
		}
		params.Set(key, fmt.Sprint(value))
	}
	connectionURL.RawQuery = params.Encode()
	return connectionURL.String()
}

func (d *PrestoProtocolDriver) GetDatabaseTypeName() string {
	return d.base.GetDatabaseTypeName()
}

func (d *PrestoProtocolDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result int64
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("%s connectivity check failed: %w", d.engineName(), err)
	}
	if result != 1 {
		return fmt.Errorf("unexpected result from %s connectivity check: %d", d.engineName(), result)
	}
	return nil
}

func (d *PrestoProtocolDriver) GetDriverName() string { return "presto-go-client/v2" }

func (d *PrestoProtocolDriver) GetCategory() drivers.DriverCategory {
	return d.base.GetCategory()
}

func (d *PrestoProtocolDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     false, // Transaction support depends on the selected connector.
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		SupportsStreaming:       true,
	}
}

func (d *PrestoProtocolDriver) ConfigureAuth(interface{}) error { return nil }

func (d *PrestoProtocolDriver) engineName() string {
	switch model.DatabaseType(d.base.GetDatabaseTypeName()) {
	case model.DatabaseTypeTrino:
		return "Trino"
	case model.DatabaseTypePresto:
		return "Presto"
	case model.DatabaseTypeOpenLooKeng:
		return "OpenLooKeng"
	default:
		return "query engine"
	}
}
