package repository

import "errors"

// Common repository errors
var (
	ErrDataSourceNotFound = errors.New("data source not found")
	ErrDataSourceExists   = errors.New("data source already exists")
	ErrInvalidUUID        = errors.New("invalid UUID format")
	ErrInvalidDatabaseType = errors.New("invalid database type")
	ErrConnectionFailed   = errors.New("database connection failed")
)