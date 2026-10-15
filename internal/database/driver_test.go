package database

import (
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/database/drivers"
	"github.com/RhinoLink-Tech/data-fabric/internal/model"
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
	} else if category != drivers.CategoryWarehouse {
		t.Errorf("Expected category %q, got %q", drivers.CategoryWarehouse, category)
	}

	t.Logf("MaxCompute driver successfully registered with type: %s and category: %s",
		driver.GetDatabaseTypeName(), category)
}
