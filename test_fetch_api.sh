#!/bin/bash

# Test script for fetch API endpoints

echo "=== Testing Nexus-Gateway Fetch API ==="
echo ""

BASE_URL="http://localhost:8080"

# Test 1: Public fetch API without authentication
echo "1. Testing public fetch API POST /api/v1/fetch"
curl -X POST "$BASE_URL/api/v1/fetch" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-token" \
  -d '{
    "dataSourceId": "12345678-1234-1234-1234-123456789012",
    "sql": "SELECT * FROM test_table LIMIT 1000",
    "type": 1,
    "batchSize": 100,
    "timeout": 60
  }'

echo -e "\n\n"

# Test 2: Public fetch API GET next batch
echo "2. Testing public fetch API GET /api/v1/fetch/{query_id}/{slug}/{token}"
curl -X GET "$BASE_URL/api/v1/fetch/test-query-id/test-slug/test-token?batch_size=50" \
  -H "Authorization: Bearer test-token"

echo -e "\n\n"

# Test 3: Internal fetch API with account headers
echo "3. Testing internal fetch API POST /api/internal/vega-gateway/v2/fetch"
curl -X POST "$BASE_URL/api/internal/vega-gateway/v2/fetch" \
  -H "Content-Type: application/json" \
  -H "x-account-id: test-account-123" \
  -H "x-account-type: internal" \
  -d '{
    "dataSourceId": "12345678-1234-1234-1234-123456789012",
    "sql": "SELECT * FROM test_table LIMIT 1000",
    "type": 1,
    "batchSize": 100,
    "timeout": 60
  }'

echo -e "\n\n"

# Test 4: Internal fetch API GET next batch
echo "4. Testing internal fetch API GET /api/internal/vega-gateway/v2/fetch/{query_id}/{slug}/{token}"
curl -X GET "$BASE_URL/api/internal/vega-gateway/v2/fetch/test-query-id/test-slug/test-token?batch_size=50" \
  -H "x-account-id: test-account-123" \
  -H "x-account-type: internal"

echo -e "\n\n"

echo "=== Test completed ==="