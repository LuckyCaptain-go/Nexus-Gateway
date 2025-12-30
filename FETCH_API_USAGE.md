# Fetch API 使用指南

## 概述

Nexus-Gateway 的 Fetch API 提供了高性能的分段式数据查询功能，支持大数据量的批量获取，类似于 Vega-Gateway 的数据查询模式。

## 主要特性

- **分段查询**: 支持分批次获取大数据集
- **流式处理**: 支持流式查询（type=2）
- **超时控制**: 可配置查询超时时间
- **批量大小区分**: 支持自定义每批次数据量
- **安全认证**: 支持内部和外部API认证

## 接口说明

### 1. 公开 Fetch API

#### POST /api/v1/fetch
执行初始查询，返回第一批次数据和分页信息。

**请求示例:**
```bash
curl -X POST "http://localhost:8080/api/v1/fetch" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "dataSourceId": "12345678-1234-1234-1234-123456789012",
    "sql": "SELECT * FROM large_table WHERE created_at > \"2024-01-01\"",
    "type": 1,
    "batchSize": 1000,
    "timeout": 120
  }'
```

**响应示例:**
```json
{
  "code": "SUCCESS",
  "message": "OK",
  "data": {
    "queryId": "550e8400-e29b-41d4-a716-446655440000",
    "slug": "a1b2c3d4",
    "token": "e5f6g7h8-i9j0-k1l2-m3n4-o5p6q7r8s9t0",
    "nextUri": "/api/v1/fetch/550e8400-e29b-41d4-a716-446655440000/a1b2c3d4/e5f6g7h8-i9j0-k1l2-m3n4-o5p6q7r8s9t0?batch_size=1000",
    "columns": [
      {
        "name": "id",
        "type": "integer",
        "nullable": false
      },
      {
        "name": "name",
        "type": "string",
        "nullable": true
      },
      {
        "name": "created_at",
        "type": "datetime",
        "nullable": false
      }
    ],
    "entries": [
      ["1", "Alice", "2024-01-01T10:00:00Z"],
      ["2", "Bob", "2024-01-01T11:00:00Z"],
      ["3", "Charlie", "2024-01-01T12:00:00Z"]
    ],
    "totalCount": 5000
  },
  "correlationId": "req-123456"
}
```

#### GET /api/v1/fetch/{query_id}/{slug}/{token}
获取下一批次数据。

**请求示例:**
```bash
curl -X GET "http://localhost:8080/api/v1/fetch/550e8400-e29b-41d4-a716-446655440000/a1b2c3d4/e5f6g7h8-i9j0-k1l2-m3n4-o5p6q7r8s9t0?batch_size=2000" \
  -H "Authorization: Bearer your-token"
```

### 2. 内部 Fetch API (兼容 Vega-Gateway)

#### POST /api/internal/vega-gateway/v2/fetch
内部查询接口，需要账户头信息。

**请求示例:**
```bash
curl -X POST "http://localhost:8080/api/internal/vega-gateway/v2/fetch" \
  -H "Content-Type: application/json" \
  -H "x-account-id": "account-123456" \
  -H "x-account-type": "production" \
  -d '{
    "dataSourceId": "12345678-1234-1234-1234-123456789012",
    "sql": "SELECT * FROM large_table WHERE created_at > \"2024-01-01\"",
    "type": 1,
    "batchSize": 1000,
    "timeout": 120
  }'
```

#### GET /api/internal/vega-gateway/v2/fetch/{query_id}/{slug}/{token}
内部下一批次获取。

**请求示例:**
```bash
curl -X GET "http://localhost:8080/api/internal/vega-gateway/v2/fetch/550e8400-e29b-41d4-a716-446655440000/a1b2c3d4/e5f6g7h8-i9j0-k1l2-m3n4-o5p6q7r8s9t0?batch_size=2000" \
  -H "x-account-id": "account-123456" \
  -H "x-account-type": "production"
```

## 参数说明

### FetchQueryRequest 请求参数

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `dataSourceId` | string | 是 | 数据源ID (UUID格式) |
| `sql` | string | 是 | SQL查询语句 (只支持SELECT) |
| `type` | integer | 是 | 查询类型: 1-同步查询, 2-流式查询 |
| `batchSize` | integer | 否 | 每批次数据量，范围: 1-10000，默认10000 |
| `timeout` | integer | 否 | 查询超时时间(秒)，范围: 1-1800，默认60 |

### 查询参数

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `query_id` | string | 是 | 查询ID |
| `slug` | string | 是 | 查询签名 |
| `token` | string | 是 | 查询令牌 |
| `batch_size` | integer | 否 | 本次批次大小，范围: 1-10000，默认10000 |

## 响应数据格式

### FetchQueryResponse 响应

| 字段 | 类型 | 说明 |
|------|------|------|
| `queryId` | string | 查询会话ID |
| `slug` | string | 查询签名 |
| `token` | string | 认证令牌 |
| `nextUri` | string | 下一批次数据URI (还有数据时存在) |
| `columns` | array | 列信息数组 |
| `entries` | array | 数据行数组，每行是字符串数组 |
| `totalCount` | integer | 总数据行数 |

### ColumnInfo 列信息

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 列名 |
| `type` | string | 数据类型 |
| `nullable` | boolean | 是否允许NULL |

## 使用流程

1. **发起初始查询**: 调用POST /api/v1/fetch，传入SQL和查询参数
2. **获取分页信息**: 从响应中获取`nextUri`、`queryId`、`slug`和`token`
3. **获取后续批次**: 使用分页信息调用GET接口获取下一批数据
4. **重复获取**: 继续调用直到所有数据获取完成（不再有`nextUri`）

## 最佳实践

### 1. 批量大小选择
- 小数据量: 100-1000条
- 中等数据量: 1000-5000条
- 大数据量: 5000-10000条

### 2. 超时设置
- 简单查询: 30-60秒
- 复杂查询: 60-300秒
- 大数据量: 300-1800秒

### 3. 错误处理
- 检查响应状态码和消息
- 处理会话过期情况
- 重试失败的分页请求

### 4. 性能优化
- 使用合适的索引
- 避免SELECT *
- 分页查询时使用WHERE条件过滤

## 安全说明

- 所有SQL查询都会进行安全验证
- 只允许SELECT语句，禁止数据修改操作
- 支持IP白名单和请求频率限制
- 内部API需要有效的账户认证

## 限制说明

1. 最大批次大小: 10,000条记录
2. 最大超时时间: 1,800秒 (30分钟)
3. 会话超时: 30分钟
4. 最大查询结果集: 取决于数据库配置

## 故障排除

### 常见错误

1. **INVALID_REQUEST**: 请求参数错误
2. **DATASOURCE_NOT_FOUND**: 数据源不存在
3. **SECURITY_VIOLATION**: SQL安全验证失败
4. **SESSION_EXPIRED**: 会话已过期
5. **INVALID_PARAMETERS**: 分页参数错误

### 调试建议

1. 检查数据源配置和连接状态
2. 验证SQL语法和权限
3. 确认认证信息有效
4. 监控查询性能和资源使用

## 示例代码

### Python 示例

```python
import requests
import json

# 初始查询
response = requests.post('http://localhost:8080/api/v1/fetch', 
    headers={
        'Content-Type': 'application/json',
        'Authorization': 'Bearer your-token'
    },
    json={
        'dataSourceId': '12345678-1234-1234-1234-123456789012',
        'sql': 'SELECT * FROM users WHERE created_at > "2024-01-01"',
        'type': 1,
        'batchSize': 1000,
        'timeout': 60
    }
)

data = response.json()
query_id = data['data']['queryId']
slug = data['data']['slug']
token = data['data']['token']

# 获取后续批次
while data['data'].get('nextUri'):
    next_url = data['data']['nextUri']
    response = requests.get(next_url, headers={
        'Authorization': 'Bearer your-token'
    })
    data = response.json()
    # 处理数据...
    print(f"Fetched {len(data['data']['entries'])} rows")
```

这个实现提供了完整的分段查询功能，支持大数据量的高效处理，同时保持了与Vega-Gateway API的兼容性。