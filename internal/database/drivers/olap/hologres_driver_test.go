package olap

import (
	"testing"
	
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"
)

func TestHologresDriverCreation(t *testing.T) {
	driver := NewHologresDriver()
	
	if driver == nil {
		t.Fatal("Expected HologresDriver instance, got nil")
	}
	
	if driver.GetDatabaseTypeName() != string(model.DatabaseTypeHologres) {
		t.Errorf("Expected database type %s, got %s", model.DatabaseTypeHologres, driver.GetDatabaseTypeName())
	}
	
	expectedCategory := drivers.CategoryOLAP
	if driver.GetCategory() != expectedCategory {
		t.Errorf("Expected category %s, got %s", expectedCategory, driver.GetCategory())
	}
}

func TestHologresDriverCapabilities(t *testing.T) {
	driver := NewHologresDriver()
	caps := driver.GetCapabilities()
	
	if !caps.SupportsSQL {
		t.Error("Expected Hologres driver to support SQL")
	}
	
	if !caps.SupportsTransaction {
		t.Error("Expected Hologres driver to support transactions")
	}
	
	if !caps.SupportsSchemaDiscovery {
		t.Error("Expected Hologres driver to support schema discovery")
	}
	
	if caps.SupportsTimeTravel {
		t.Error("Expected Hologres driver to not support time travel")
	}
	
	if caps.RequiresTokenRotation {
		t.Error("Expected Hologres driver to not require token rotation")
	}
	
	if !caps.SupportsStreaming {
		t.Error("Expected Hologres driver to support streaming")
	}
}

func TestHologresDriverDefaultPort(t *testing.T) {
	driver := NewHologresDriver()
	
	defaultPort := driver.GetDefaultPort()
	expectedPort := 5432 // Default PostgreSQL/Hologres port
	
	if defaultPort != expectedPort {
		t.Errorf("Expected default port %d, got %d", expectedPort, defaultPort)
	}
}