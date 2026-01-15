package warehouses

import (
	"testing"
	
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"
)

func TestHiveDriverCreation(t *testing.T) {
	driver := NewHiveDriver()
	
	if driver == nil {
		t.Fatal("Expected HiveDriver instance, got nil")
	}
	
	if driver.GetDatabaseTypeName() != string(model.DatabaseTypeApacheHive) {
		t.Errorf("Expected database type %s, got %s", model.DatabaseTypeApacheHive, driver.GetDatabaseTypeName())
	}
	
	expectedCategory := drivers.CategoryCloudDataWarehouse
	if driver.GetCategory() != expectedCategory {
		t.Errorf("Expected category %s, got %s", expectedCategory, driver.GetCategory())
	}
}

func TestHiveDriverCapabilities(t *testing.T) {
	driver := NewHiveDriver()
	caps := driver.GetCapabilities()
	
	if !caps.SupportsSQL {
		t.Error("Expected Hive driver to support SQL")
	}
	
	if caps.SupportsTransaction {
		t.Error("Expected Hive driver to not support transactions")
	}
	
	if !caps.SupportsSchemaDiscovery {
		t.Error("Expected Hive driver to support schema discovery")
	}
	
	if caps.SupportsTimeTravel {
		t.Error("Expected Hive driver to not support time travel")
	}
	
	if caps.RequiresTokenRotation {
		t.Error("Expected Hive driver to not require token rotation")
	}
	
	if !caps.SupportsStreaming {
		t.Error("Expected Hive driver to support streaming")
	}
}

func TestHiveDriverDefaultPort(t *testing.T) {
	driver := NewHiveDriver()
	
	defaultPort := driver.GetDefaultPort()
	expectedPort := 10000 // Default HiveServer2 port
	
	if defaultPort != expectedPort {
		t.Errorf("Expected default port %d, got %d", expectedPort, defaultPort)
	}
}