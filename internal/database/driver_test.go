package database

import (
	"testing"
	"nexus-gateway/internal/model"
)

func TestMaxComputeDriverRegistration(t *testing.T) {
	registry := NewDriverRegistry()
	
	// Test if MaxCompute driver is registered
	if !registry.IsSupported(model.DatabaseTypeMaxCompute) {
		t.Errorf("MaxCompute driver should be supported")
		return
	}
	
	driver, err := registry.GetDriver(model.DatabaseTypeMaxCompute)
	if err != nil {
		t.Errorf("Failed to get MaxCompute driver: %v", err)
		return
	}
	
	if driver.GetDatabaseTypeName() != string(model.DatabaseTypeMaxCompute) {
		t.Errorf("Expected database type %s, got %s", model.DatabaseTypeMaxCompute, driver.GetDatabaseTypeName())
	}
	
	category, err := registry.GetDriverCategory(model.DatabaseTypeMaxCompute)
	if err != nil {
		t.Errorf("Failed to get MaxCompute driver category: %v", err)
	} else if string(category) != "warehouses" {
		t.Errorf("Expected category 'warehouses', got '%s'", category)
	}
	
	t.Logf("MaxCompute driver successfully registered with type: %s and category: %s", 
		   driver.GetDatabaseTypeName(), category)
}