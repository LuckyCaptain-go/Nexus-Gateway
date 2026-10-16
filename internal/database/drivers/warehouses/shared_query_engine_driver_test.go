package warehouses

import (
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestSharedQueryEngineProtocolHeaders(t *testing.T) {
	tests := []struct {
		name       string
		driver     *PrestoProtocolDriver
		userHeader string
		catalog    string
		schema     string
	}{
		{
			name:       "trino uses trino headers",
			driver:     NewTrinoDriver(),
			userHeader: "X-Trino-User",
			catalog:    "X-Trino-Catalog",
			schema:     "X-Trino-Schema",
		},
		{
			name:       "presto uses presto headers",
			driver:     NewPrestoDriver(),
			userHeader: "X-Presto-User",
			catalog:    "X-Presto-Catalog",
			schema:     "X-Presto-Schema",
		},
		{
			name:       "openlookeng uses presto headers",
			driver:     NewOpenLooKengDriver(),
			userHeader: "X-Presto-User",
			catalog:    "X-Presto-Catalog",
			schema:     "X-Presto-Schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requestHeaders http.Header
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/statement" {
					http.Error(w, "unexpected request", http.StatusNotFound)
					return
				}
				requestHeaders = r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"id":"mock-query","columns":[{"name":"result","type":"bigint","typeSignature":{"rawType":"bigint"}}],"data":[[1]],"stats":{"state":"FINISHED"}}`)
			}))
			defer server.Close()

			serverURL, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			config := model.DataSourceConfig{
				Host: serverURL.Hostname(), Database: "lakehouse", Schema: "analytics",
				Username: "data-fabric", Port: portFromURL(t, serverURL),
			}
			db, err := tt.driver.Open(tt.driver.BuildDSN(&config))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			var result int64
			if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
				t.Fatalf("mock query failed: %v", err)
			}
			if result != 1 {
				t.Fatalf("query result = %d, want 1", result)
			}
			if got := requestHeaders.Get(tt.userHeader); got != config.Username {
				t.Errorf("%s = %q, want %q", tt.userHeader, got, config.Username)
			}
			if got := requestHeaders.Get(tt.catalog); got != config.Database {
				t.Errorf("%s = %q, want %q", tt.catalog, got, config.Database)
			}
			if got := requestHeaders.Get(tt.schema); got != config.Schema {
				t.Errorf("%s = %q, want %q", tt.schema, got, config.Schema)
			}

			wrongPrefix := "X-Trino-"
			if tt.driver.GetDatabaseTypeName() == string(model.DatabaseTypeTrino) {
				wrongPrefix = "X-Presto-"
			}
			for key := range requestHeaders {
				if strings.HasPrefix(key, wrongPrefix) {
					t.Errorf("unexpected header from other protocol family: %s", key)
				}
			}
		})
	}
}

func TestSharedQueryEngineTLSUsesVerifiedHTTPSConfiguration(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"mock-query","columns":[{"name":"result","type":"bigint","typeSignature":{"rawType":"bigint"}}],"data":[[1]],"stats":{"state":"FINISHED"}}`)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	caFile := t.TempDir() + "/coordinator.pem"
	certificate := server.TLS.Certificates[0].Certificate[0]
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), 0600); err != nil {
		t.Fatal(err)
	}

	config := model.DataSourceConfig{
		Host: serverURL.Hostname(), Port: portFromURL(t, serverURL),
		Database: "lakehouse", Username: "data-fabric", SSL: true,
		AdditionalProps: map[string]interface{}{"ssl_ca": caFile},
	}
	db, err := NewPrestoDriver().Open(NewPrestoDriver().BuildDSN(&config))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var result int64
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
		t.Fatalf("TLS mock query failed: %v", err)
	}
	if result != 1 {
		t.Fatalf("query result = %d, want 1", result)
	}
}

func portFromURL(t *testing.T, parsed *url.URL) int {
	t.Helper()
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatalf("invalid test server port: %v", err)
	}
	return port
}
