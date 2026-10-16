package database

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestQueryEngineConnectivityCheck(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantStatus string
	}{
		{
			name:       "queries coordinator successfully",
			statusCode: http.StatusOK,
			body:       `{"id":"mock-query","columns":[{"name":"result","type":"bigint","typeSignature":{"rawType":"bigint"}}],"data":[[1]],"stats":{"state":"FINISHED"}}`,
			wantStatus: "healthy",
		},
		{
			name:       "reports coordinator query failure",
			statusCode: http.StatusOK,
			body:       `{"id":"mock-query","error":{"message":"coordinator unavailable"},"stats":{"state":"FAILED"}}`,
			wantStatus: "unhealthy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				_, _ = fmt.Fprint(w, tt.body)
			}))
			defer server.Close()

			endpoint, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(endpoint.Port())
			if err != nil {
				t.Fatal(err)
			}

			checker := &HealthChecker{registry: NewDriverRegistry()}
			config := &model.DataSourceConfig{
				Host: endpoint.Hostname(), Port: port, Database: "lakehouse", Username: "data-fabric",
			}
			result, err := checker.CheckDataSourceConnectivity(context.Background(), config, model.DatabaseTypeTrino)
			if err != nil {
				t.Fatalf("CheckDataSourceConnectivity() error = %v", err)
			}
			if result.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q; message: %s", result.Status, tt.wantStatus, result.Message)
			}
			if result.Message == "" {
				t.Fatal("expected a connection result message")
			}
		})
	}
}
