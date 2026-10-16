package sql_translator

import (
	"slices"
	"testing"
)

func TestQueryEngineDialectsAreAdvertised(t *testing.T) {
	translators := []SQLTranslator{
		NewExternalSQLTranslator(true),
		NewMockSQLTranslator(),
	}
	for _, translator := range translators {
		for _, dialect := range []string{"trino", "presto", "openlookeng"} {
			if !slices.Contains(translator.SupportedDialects(), dialect) {
				t.Errorf("%T does not report dialect %q as supported", translator, dialect)
			}
		}
	}
}
