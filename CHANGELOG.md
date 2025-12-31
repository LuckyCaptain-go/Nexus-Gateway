# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2025-12-31

### Changed
- `ExecuteQuery` now prefers establishing a backend streaming cursor when supported. Returns the first batch of rows and a `nextUri` to continue the stream.
- Added `/fetch` POST and `/fetch/{query_id}/{slug}/{token}` GET endpoints for explicit batch/streaming retrieval.
- `QueryResponse` and `FetchQueryResponse` now include `nextUri` to indicate continuation URIs for streaming sessions.
- Added server-side `QuerySession` management to hold streaming cursors (`Rows`) and cancellation handles.
- OpenAPI spec updated: `specs/001-phase1-enhanced-single-source/contracts/openapi.yaml` now documents `/fetch` endpoints and `FetchQueryRequest`/`FetchQueryResponse` schemas.
- README updated with streaming usage examples and new `query.prefer_streaming` configuration documentation.
- `FETCH_API_USAGE.md` updated: example requests now use `batch_size` (snake_case) to match API model.
- `test_fetch_api.sh` updated to use `batch_size` in JSON payloads.

### Added
- `CHANGELOG.md` (this file).

### Notes
- The streaming behavior is configurable via `query.prefer_streaming` (default `true`). When `prefer_streaming` is `true` and the backend does not support streaming, queries will fail; set it to `false` to allow automatic fallback to LIMIT/OFFSET continuation.
- Sessions have a default TTL and are subject to server-side cleanup; clients should exhaust or explicitly complete streams when possible.

