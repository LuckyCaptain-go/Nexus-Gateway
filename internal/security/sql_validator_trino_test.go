package security

import (
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestQueryEngineSQLCapabilities(t *testing.T) {
	for _, dbType := range []model.DatabaseType{
		model.DatabaseTypeTrino,
		model.DatabaseTypePresto,
		model.DatabaseTypeOpenLooKeng,
	} {
		config := GetDataSourceSQLConfig(dbType)
		if config.IdentifierQuote != `"` || !config.SupportsArrays || !config.SupportsStructs || !config.SupportsWindowFunctions {
			t.Errorf("unexpected %s SQL capabilities: %+v", dbType, config)
		}
	}
}

func TestFederatedValidatorAcceptsCatalogQualifiedSelect(t *testing.T) {
	validator := NewSQLValidator(10000)
	query := "SELECT o.order_id, c.customer_name FROM lakehouse.sales.orders o JOIN postgresql.crm.customers c ON o.customer_id = c.customer_id"

	for _, dbType := range []model.DatabaseType{
		model.DatabaseTypeTrino,
		model.DatabaseTypePresto,
		model.DatabaseTypeOpenLooKeng,
	} {
		if err := validator.ValidateStatementForDataSource(query, dbType); err != nil {
			t.Errorf("%s rejected catalog-qualified read query: %v", dbType, err)
		}
	}

	if err := validator.ValidateStatement(query); err == nil {
		t.Fatal("generic validator unexpectedly accepted a three-part identifier")
	}
}

func TestFederatedValidatorStillRejectsNonReadOnlyAndMultipleStatements(t *testing.T) {
	validator := NewSQLValidator(10000)
	for _, query := range []string{
		"DELETE FROM lakehouse.sales.orders",
		"SELECT * FROM lakehouse.sales.orders; DROP TABLE lakehouse.sales.orders",
	} {
		if err := validator.ValidateStatementForDataSource(query, model.DatabaseTypeTrino); err == nil {
			t.Errorf("accepted unsafe query %q", query)
		}
	}
}

func TestStripCatalogPrefixesPreservesStringsAndQuotedIdentifiers(t *testing.T) {
	input := `SELECT 'catalog.schema.table', catalog."schema"."table" FROM lakehouse.sales.orders`
	want := `SELECT 'catalog.schema.table', "schema"."table" FROM sales.orders`
	if got := stripCatalogPrefixes(input); got != want {
		t.Fatalf("stripCatalogPrefixes() = %q, want %q", got, want)
	}
}
