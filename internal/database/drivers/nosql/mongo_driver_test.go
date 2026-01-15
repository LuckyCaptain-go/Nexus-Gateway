package nosql

import (
	"testing"
	
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"
)

func TestMongoDriverCreation(t *testing.T) {
	driver := NewMongoDriver()
	
	if driver == nil {
		t.Fatal("Expected MongoDriver instance, got nil")
	}
	
	if driver.GetDatabaseTypeName() != string(model.DatabaseTypeMongoDB) {
		t.Errorf("Expected database type %s, got %s", model.DatabaseTypeMongoDB, driver.GetDatabaseTypeName())
	}
	
	expectedCategory := drivers.CategoryFileSystem // Using filesystem category as a placeholder for NoSQL
	if driver.GetCategory() != expectedCategory {
		t.Errorf("Expected category %s, got %s", expectedCategory, driver.GetCategory())
	}
}

func TestMongoDriverCapabilities(t *testing.T) {
	driver := NewMongoDriver()
	caps := driver.GetCapabilities()
	
	if caps.SupportsSQL {
		t.Error("Expected MongoDB driver to not support SQL")
	}
	
	if !caps.SupportsTransaction {
		t.Error("Expected MongoDB driver to support transactions")
	}
	
	if !caps.SupportsSchemaDiscovery {
		t.Error("Expected MongoDB driver to support schema discovery")
	}
	
	if caps.SupportsTimeTravel {
		t.Error("Expected MongoDB driver to not support time travel")
	}
	
	if caps.RequiresTokenRotation {
		t.Error("Expected MongoDB driver to not require token rotation")
	}
	
	if !caps.SupportsStreaming {
		t.Error("Expected MongoDB driver to support streaming")
	}
}

func TestMongoDriverDefaultPort(t *testing.T) {
	driver := NewMongoDriver()
	
	defaultPort := driver.GetDefaultPort()
	expectedPort := 27017 // Default MongoDB port
	
	if defaultPort != expectedPort {
		t.Errorf("Expected default port %d, got %d", expectedPort, defaultPort)
	}
}

func TestMongoDriverPagination(t *testing.T) {
	driver := NewMongoDriver()
	
	sql := "find({'name': 'test'})"
	batchSize := int64(10)
	offset := int64(20)
	
	result, err := driver.ApplyBatchPagination(sql, batchSize, offset)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	expected := "MONGO_QUERY|find({'name': 'test'})|LIMIT|10|SKIP|20"
	if result != expected {
		t.Errorf("Expected pagination result '%s', got '%s'", expected, result)
	}
}