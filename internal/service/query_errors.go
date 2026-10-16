package service

import (
	"context"
	"errors"

	"github.com/RhinoLink-Tech/data-fabric/internal/repository"
	"github.com/RhinoLink-Tech/data-fabric/internal/security"
)

var (
	ErrQuerySessionUnavailable = errors.New("query session is unavailable or expired")
	ErrInvalidQuerySession     = errors.New("query session credentials are invalid")
)

// SafeQueryErrorMessage maps internal failures to stable, user-actionable text.
func SafeQueryErrorMessage(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "The query timed out. Reduce the query scope or increase the request timeout."
	case errors.Is(err, context.Canceled):
		return "The query was cancelled before completion."
	case errors.Is(err, repository.ErrDataSourceNotFound):
		return "The selected data source was not found."
	case errors.Is(err, ErrQuerySessionUnavailable), errors.Is(err, ErrInvalidQuerySession):
		return "The query session is invalid or has expired. Submit the query again."
	case errors.Is(err, security.ErrNotSelectQuery):
		return "Only read-only SELECT queries are supported."
	case errors.Is(err, security.ErrSQLSyntaxError), errors.Is(err, security.ErrDangerousKeyword),
		errors.Is(err, security.ErrEmptyQuery), errors.Is(err, security.ErrQueryTooLong):
		return "The query is invalid or uses unsupported SQL syntax."
	case errors.Is(err, security.ErrSQLInjection):
		return "The query did not pass SQL safety validation."
	default:
		return "The data-source query failed. Check the SQL dialect, catalog and schema, permissions, and query-engine availability."
	}
}
