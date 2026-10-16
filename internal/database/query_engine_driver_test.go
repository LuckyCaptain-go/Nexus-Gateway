package database

import (
	"errors"
	"strings"
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestQueryEngineDriverRegistrationAndConfiguration(t *testing.T) {
	registry := NewDriverRegistry()
	checker := &HealthChecker{registry: registry}

	for _, dbType := range []model.DatabaseType{
		model.DatabaseTypeTrino,
		model.DatabaseTypePresto,
		model.DatabaseTypeOpenLooKeng,
	} {
		t.Run(string(dbType), func(t *testing.T) {
			if !registry.IsSupported(dbType) {
				t.Fatalf("%s driver should be registered", dbType)
			}

			driver, err := registry.GetDriver(dbType)
			if err != nil {
				t.Fatalf("GetDriver(%s) error = %v", dbType, err)
			}
			if got := driver.GetDriverName(); got != "presto-go-client/v2" {
				t.Errorf("driver name = %q, want presto-go-client/v2", got)
			}

			config := &model.DataSourceConfig{
				Host:     "coordinator.example.com",
				Database: "lakehouse",
				Username: "data-fabric",
			}
			if err := checker.ValidateDataSourceConfiguration(config, dbType); err != nil {
				t.Fatalf("%s configuration should validate: %v", dbType, err)
			}
			if config.Port != 8080 {
				t.Errorf("%s default port = %d, want 8080", dbType, config.Port)
			}
		})
	}
}

func TestQueryEngineConnectionErrorsRedactSecrets(t *testing.T) {
	config := &model.DataSourceConfig{
		Username: "data-fabric",
		Password: "password-secret",
		AdditionalProps: map[string]interface{}{
			"access_token": "token-secret",
			"auth": map[string]interface{}{
				"client_secret": "nested-secret",
			},
		},
	}
	err := errors.New("request failed for https://data-fabric:password-secret@coordinator:8443?access_token=token-secret&client_secret=nested-secret Authorization: Bearer bearer-secret")
	message := redactConnectionError(err, config)

	for _, secret := range []string{"password-secret", "token-secret", "nested-secret", "bearer-secret"} {
		if strings.Contains(message, secret) {
			t.Errorf("connection error leaked %q: %s", secret, message)
		}
	}
	if !strings.Contains(message, "coordinator:8443") {
		t.Errorf("expected actionable endpoint context to remain, got %q", message)
	}
}
