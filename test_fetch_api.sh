#!/bin/bash

# Test script for fetch API endpoints

echo "=== Testing Data Fabric Fetch API ==="
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
    "batch_size": 100,
    "timeout": 60
  }'

echo -e "\n\n"

# Test 2: Public fetch API GET next batch
echo "2. Testing public fetch API GET /api/v1/fetch/{query_id}/{slug}/{token}"
curl -X GET "$BASE_URL/api/v1/fetch/test-query-id/test-slug/test-token?batch_size=50" \
  -H "Authorization: Bearer test-token"

echo -e "\n\n"

echo "=== Test completed ==="