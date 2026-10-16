package warehouses

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/RhinoLink-Tech/data-fabric/internal/database/drivers"
	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestTrinoDriverBuildDSN(t *testing.T) {
	driver := NewTrinoDriver()
	dsn := driver.BuildDSN(&model.DataSourceConfig{
		Host: "2001:db8::1", Port: 8443, Database: "lakehouse", Schema: "analytics",
		Username: "user@example.com", Password: "p@ss:/word", SSL: true,
		AdditionalProps: map[string]interface{}{"source": "data-fabric", "catalog": "must-not-override"},
	})

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("BuildDSN returned an invalid URL: %v", err)
	}
	if parsed.Scheme != "https" || parsed.Host != "[2001:db8::1]:8443" {
		t.Fatalf("unexpected query-engine endpoint: %s://%s", parsed.Scheme, parsed.Host)
	}
	if username := parsed.User.Username(); username != "user@example.com" {
		t.Errorf("unexpected username: %q", username)
	}
	if password, ok := parsed.User.Password(); !ok || password != "p@ss:/word" {
		t.Errorf("password was not safely encoded in the URL")
	}
	if got := parsed.Query().Get("catalog"); got != "lakehouse" {
		t.Errorf("catalog was overridden: %q", got)
	}
	if got := parsed.Query().Get("schema"); got != "analytics" {
		t.Errorf("unexpected schema: %q", got)
	}
	if got := parsed.Query().Get("source"); got != "data-fabric" {
		t.Errorf("unexpected additional property: %q", got)
	}
}

func TestTrinoDriverBuildDSNDefaults(t *testing.T) {
	driver := NewTrinoDriver()
	dsn := driver.BuildDSN(&model.DataSourceConfig{Host: "localhost", Database: "memory", Username: "user"})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("BuildDSN returned an invalid URL: %v", err)
	}
	if parsed.Host != "localhost:8080" {
		t.Errorf("expected the default port in host, got %q", parsed.Host)
	}
	if err := driver.ValidateDSN(dsn); err != nil {
		t.Errorf("expected valid DSN, got %v", err)
	}
}

func TestTrinoDriverValidateDSN(t *testing.T) {
	driver := NewTrinoDriver()
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{name: "valid HTTP without password", dsn: "http://user@trino:8080?catalog=lakehouse"},
		{name: "valid HTTPS basic auth", dsn: "https://user:secret@trino:8443?catalog=lakehouse"},
		{name: "password over HTTP", dsn: "http://user:secret@trino:8080?catalog=lakehouse", wantErr: true},
		{name: "empty password over HTTP", dsn: "http://user:@trino:8080?catalog=lakehouse", wantErr: true},
		{name: "missing user", dsn: "http://trino:8080?catalog=lakehouse", wantErr: true},
		{name: "missing catalog", dsn: "http://user@trino:8080", wantErr: true},
		{name: "invalid port", dsn: "http://user@trino:70000?catalog=lakehouse", wantErr: true},
		{name: "unsupported scheme", dsn: "trino://user@trino:8080?catalog=lakehouse", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := driver.ValidateDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateDSN() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTrinoDriverMetadata(t *testing.T) {
	driver := NewTrinoDriver()
	if driver.GetDatabaseTypeName() != string(model.DatabaseTypeTrino) {
		t.Fatalf("unexpected database type: %s", driver.GetDatabaseTypeName())
	}
	if driver.GetDefaultPort() != 8080 {
		t.Fatalf("unexpected default port: %d", driver.GetDefaultPort())
	}
	if driver.GetCategory() != drivers.CategoryQueryEngine {
		t.Fatalf("unexpected driver category: %s", driver.GetCategory())
	}
}

func TestProtocolQueryCancellationSendsDeleteOnlyToCoordinator(t *testing.T) {
	tests := []struct {
		name        string
		nextURIKind string
		wantDelete  bool
	}{
		{name: "active coordinator query", nextURIKind: "local", wantDelete: true},
		{name: "external next uri is rejected", nextURIKind: "external"},
		{name: "finished query", nextURIKind: "finished"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleteReceived := make(chan struct{}, 1)
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					nextURI := ""
					switch tt.nextURIKind {
					case "local":
						nextURI = server.URL + "/v1/next/query"
					case "external":
						nextURI = "http://example.invalid/v1/next/query"
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{"id": "query-1", "nextUri": nextURI})
				case http.MethodDelete:
					if got := r.Header.Get("X-Trino-User"); got != "cancel-test" {
						t.Errorf("DELETE X-Trino-User = %q", got)
					}
					deleteReceived <- struct{}{}
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			endpoint, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			base := &protocolRoundTripper{next: http.DefaultTransport, forceHTTPS: false}
			client := &http.Client{Transport: newQueryCancellationRoundTripper(base, endpoint)}
			ctx, cancel := context.WithCancel(context.Background())
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/statement", strings.NewReader("SELECT 1"))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("X-Trino-User", "cancel-test")
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			cancel()

			if tt.wantDelete {
				select {
				case <-deleteReceived:
				case <-time.After(2 * time.Second):
					t.Fatal("coordinator did not receive query cancellation DELETE")
				}
			} else {
				select {
				case <-deleteReceived:
					t.Fatal("sent a cancellation DELETE without a same-coordinator nextUri")
				case <-time.After(50 * time.Millisecond):
				}
			}
		})
	}
}

func TestDecodeNextURIUsesTopLevelProtocolField(t *testing.T) {
	nested := `{"nextUri":"http://example.invalid/untrusted"}`
	body, err := json.Marshal(map[string]interface{}{
		"data":    [][]string{{nested}},
		"nextUri": "http://coordinator.example/v1/next/query",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := decodeNextURI(body); !ok || got != "http://coordinator.example/v1/next/query" {
		t.Fatalf("decodeNextURI() = %q, %v", got, ok)
	}
}
