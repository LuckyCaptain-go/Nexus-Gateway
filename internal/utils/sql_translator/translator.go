package sql_translator

import "errors"

// SQLTranslator defines the interface for SQL dialect translation
type SQLTranslator interface {
	// Translate converts SQL from source dialect to target dialect
	Translate(sql, sourceDialect, targetDialect string) (string, error)
	
	// SupportedDialects returns a list of supported SQL dialects
	SupportedDialects() []string
	
	// Validate checks if the SQL is valid for the given dialect
	Validate(sql, dialect string) (bool, error)
}

// Dialect represents different SQL dialects
type Dialect string

const (
	MySQL      Dialect = "mysql"
	PostgreSQL Dialect = "postgres"
	Trino      Dialect = "trino"
	Spark      Dialect = "spark"
	Oracle     Dialect = "oracle"
	SQLServer  Dialect = "sqlserver"
	Snowflake  Dialect = "snowflake"
	SQLite     Dialect = "sqlite"
	DuckDB     Dialect = "duckdb"
	Redshift   Dialect = "redshift"
)

// ValidationError represents an error when SQL validation fails
type ValidationError struct {
	Dialect string
	Message string
}

func (e *ValidationError) Error() string {
	return "validation error for dialect " + e.Dialect + ": " + e.Message
}

// TranslationError represents an error when SQL translation fails
type TranslationError struct {
	SourceDialect string
	TargetDialect string
	Message       string
}

func (e *TranslationError) Error() string {
	return "translation error from " + e.SourceDialect + " to " + e.TargetDialect + ": " + e.Message
}

// IsValidationError checks if an error is a validation error
func IsValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// IsTranslationError checks if an error is a translation error
func IsTranslationError(err error) bool {
	var te *TranslationError
	return errors.As(err, &te)
}