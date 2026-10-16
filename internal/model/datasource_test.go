package model

import "testing"

func TestQueryEngineDatabaseTypes(t *testing.T) {
	for _, dbType := range []DatabaseType{
		DatabaseTypeTrino,
		DatabaseTypePresto,
		DatabaseTypeOpenLooKeng,
	} {
		t.Run(string(dbType), func(t *testing.T) {
			if !IsValidDatabaseType(string(dbType)) {
				t.Fatalf("%s should be a valid data source type", dbType)
			}
			if got := GetDatabaseCategory(dbType); got != "query_engine" {
				t.Fatalf("unexpected %s category: %q", dbType, got)
			}
		})
	}
}
