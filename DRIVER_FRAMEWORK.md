# Nexus-Gateway 驱动框架文档

## 1. 概述

Nexus-Gateway 是一个通用的多数据库代理网关，支持90多种数据源的统一访问。其核心是驱动框架，允许以插件化方式支持不同类型的数据库、数据湖、对象存储和文件系统。

## 2. 项目架构

### 2.1 整体架构

```
Nexus-Gateway/
├── cmd/server/                    # 应用程序入口点
├── internal/
│   ├── controller/               # HTTP请求处理器
│   ├── service/                  # 业务逻辑层
│   ├── repository/               # 数据访问层
│   ├── model/                    # 数据模型和DTO
│   ├── middleware/               # HTTP中间件
│   └── database/
│       ├── connection_pool.go    # 连接池管理
│       ├── driver_registry.go    # 驱动注册表
│       ├── health_checker.go     # 健康检查器
│       └── drivers/              # 数据库驱动 (90+ 实现)
│           ├── common/           # 通用驱动功能
│           │   └── types.go      # 驱动接口定义
│           ├── table_formats/    # 数据湖表格式 (Iceberg, Delta, Hudi)
│           ├── warehouses/       # 云数据仓库 (Snowflake, Databricks, Redshift, BigQuery)
│           ├── olap/             # OLAP引擎 (ClickHouse, Doris, StarRocks, Druid)
│           ├── object_storage/   # 对象存储 (S3, MinIO, OSS, COS, Azure Blob)
│           ├── file_system/      # 文件系统 (HDFS, Apache Ozone)
│           └── domestic/         # 国产数据库 (OceanBase, TiDB, TDSQL等)
├── configs/                      # 配置文件
├── pkg/response/                 # 响应格式化
├── migrations/                   # 数据库迁移
└── specs/                        # 规范文档
```

## 3. 驱动框架设计

### 3.1 核心组件

#### 3.1.1 驱动接口 (drivers.Driver)

所有驱动必须实现的统一接口：

```go
type Driver interface {
    Open(dsn string) (*sql.DB, error)           // 打开数据库连接
    ValidateDSN(dsn string) error               // 验证连接字符串
    GetDefaultPort() int                        // 获取默认端口
    BuildDSN(config *model.DataSourceConfig) string  // 构建连接字符串
    GetDatabaseTypeName() string                // 获取数据库类型名称
    TestConnection(db *sql.DB) error            // 测试连接
    GetDriverName() string                      // 获取驱动名称
    GetCategory() DriverCategory                // 获取驱动类别
    GetCapabilities() DriverCapabilities        // 获取驱动能力
    ConfigureAuth(authConfig interface{}) error // 配置认证
}
```

#### 3.1.2 驱动注册表 (DriverRegistry)

- **单例模式**: 通过 `GetDriverRegistry()` 函数提供全局唯一的驱动注册表实例
- **工厂模式**: 使用 `map[model.DatabaseType]func() drivers.Driver` 存储驱动创建函数
- **线程安全**: 使用 `sync.RWMutex` 确保并发安全

#### 3.1.3 驱动类别 (DriverCategory)

- `traditional`: 传统关系型数据库 (MySQL, PostgreSQL, Oracle等)
- `warehouses`: 云数据仓库 (Snowflake, Redshift, BigQuery等)
- `olap`: OLAP引擎 (ClickHouse, Doris, StarRocks等)
- `object_storage`: 对象存储 (S3, MinIO等)
- `file_system`: 分布式文件系统 (HDFS, Ozone等)
- `table_formats`: 数据湖表格式 (Iceberg, Delta, Hudi)
- `domestic`: 国产数据库 (OceanBase, TiDB, 达梦等)

### 3.2 驱动注册机制

驱动注册通过 `DriverRegistry.registerDrivers()` 方法实现，采用以下策略：

1. **静态注册**: 在应用启动时注册所有支持的驱动
2. **工厂函数**: 每个驱动类型关联一个创建函数，支持按需实例化
3. **分类管理**: 按功能类别组织驱动，便于管理和扩展

## 4. 支持的数据源类型

### 4.1 传统关系型数据库

| 数据库 | 类型 | 特性 |
|--------|------|------|
| MySQL | `mysql` | 标准SQL协议，连接池管理 |
| MariaDB | `mariadb` | MySQL兼容协议 |
| PostgreSQL | `postgresql` | 标准SQL协议，JSON支持 |
| Oracle | `oracle` | 企业级特性，复杂类型支持 |

### 4.2 云数据仓库

| 数据库 | 类型 | 特性 |
|--------|------|------|
| Snowflake | `snowflake` | 仓库管理，时间旅行，类型映射 |
| Databricks | `databricks` | Delta Lake集成，SQL仓库REST API |
| Redshift | `redshift` | IAM认证，PostgreSQL兼容 |
| BigQuery | `bigquery` | 原生Go客户端，分页查询 |

### 4.3 数据湖表格式

| 格式 | 类型 | 特性 |
|------|------|------|
| Apache Iceberg | `iceberg` | 快照时间旅行，模式演进 |
| Delta Lake | `delta_lake` | ACID事务，版本控制 |
| Apache Hudi | `hudi` | COPY_ON_WRITE/MERGE_ON_READ |

### 4.4 OLAP引擎

| 引擎 | 类型 | 特性 |
|------|------|------|
| ClickHouse | `clickhouse` | 原生协议，字典，数组类型 |
| Apache Doris | `doris` | 物化视图，滚动聚合 |
| StarRocks | `starrocks` | 管道引擎执行 |
| Apache Druid | `druid` | SQL API，时序查询 |

### 4.5 对象存储

| 存储 | 类型 | 支持格式 |
|------|------|----------|
| AWS S3 | `s3_*` | Parquet, ORC, Avro, CSV, JSON等 |
| MinIO | `minio_*` | S3兼容，Parquet, CSV, JSON等 |
| 阿里云OSS | `oss_*` | Parquet, CSV, JSON, Delta Lake |
| 腾讯云COS | `cos_*` | Parquet, CSV, JSON, Delta Lake |
| Azure Blob | `azure_blob_*` | Parquet, CSV, JSON, SAS令牌 |

### 4.6 分布式文件系统

| 系统 | 类型 | 支持格式 |
|------|------|----------|
| HDFS | `hdfs_*` | Parquet, ORC, Avro, CSV, JSON等 |
| Apache Ozone | `ozone_*` | Parquet, ORC, Avro, CSV, JSON等 |

### 4.7 国产数据库

| 数据库 | 类型 | 特性 |
|--------|------|------|
| OceanBase | `oceanbase_mysql/oracle` | MySQL/Oracle兼容模式 |
| TiDB | `tidb` | 分布式查询优化，放置规则 |
| TDSQL | `tdsql` | 分片管理，读写分离 |
| GaussDB | `gaussdb_mysql/postgres` | 高可用，流复制 |
| 达梦数据库 | `dameng` | 字符集支持，闪回查询 |
| 人大金仓 | `kingbasees` | Oracle兼容模式 |
| 南大通用 | `gbase_8s/8t` | Informix兼容，字符集转换 |
| 神通数据库 | `oscar` | 集群模式，分片管理 |
| openGauss | `opengauss` | 行级安全，分布式事务 |

## 5. 驱动实现标准

### 5.1 驱动接口实现要求

所有驱动必须实现以下方法：

1. **连接管理**:
   - `Open()`: 建立数据库连接
   - `ValidateDSN()`: 验证连接参数
   - `GetDefaultPort()`: 返回默认端口

2. **配置处理**:
   - `BuildDSN()`: 根据配置构建连接字符串
   - `ConfigureAuth()`: 配置认证参数

3. **元数据**:
   - `GetDatabaseTypeName()`: 返回数据库类型
   - `GetDriverName()`: 返回驱动名称
   - `GetCategory()`: 返回驱动类别

4. **能力查询**:
   - `GetCapabilities()`: 返回驱动能力

### 5.2 类型映射标准

为确保跨数据库的兼容性，所有驱动需实现类型映射：

```go
type StandardizedType int

const (
    StandardizedTypeBoolean StandardizedType = iota
    StandardizedTypeInteger
    StandardizedTypeLong
    StandardizedTypeFloat
    StandardizedTypeDouble
    StandardizedTypeDecimal
    StandardizedTypeString
    StandardizedTypeText
    StandardizedTypeDate
    StandardizedTypeTime
    StandardizedTypeTimestamp
    StandardizedTypeDateTime
    StandardizedTypeTimestampWithZone
    StandardizedTypeBinary
    StandardizedTypeVariant
    StandardizedTypeArray
    StandardizedTypeStruct
    StandardizedTypeObject
    StandardizedTypeGeography
)
```

### 5.3 安全特性

1. **SQL注入防护**: 所有查询使用参数化查询
2. **只读查询**: 默认只允许SELECT语句
3. **连接池管理**: 每个数据源独立连接池
4. **认证加密**: 连接凭据加密存储

## 6. 扩展新驱动

### 6.1 添加新驱动的步骤

1. **创建驱动文件**: 在相应目录创建新的驱动实现
2. **实现Driver接口**: 实现所有必需的方法
3. **注册驱动**: 在`DriverRegistry.registerDrivers()`中添加注册代码
4. **更新模型**: 在`model/datasource.go`中添加新的数据库类型常量
5. **测试验证**: 编写单元测试验证功能

### 6.2 示例驱动结构

```go
package warehouses

import (
    "database/sql"
    "nexus-gateway/internal/database/drivers"
    "nexus-gateway/internal/model"
)

type ExampleDriver struct{}

func (d *ExampleDriver) Open(dsn string) (*sql.DB, error) {
    // 实现数据库连接逻辑
}

func (d *ExampleDriver) ValidateDSN(dsn string) error {
    // 实现连接字符串验证
}

// ... 实现其他接口方法

func (d *ExampleDriver) GetCategory() drivers.DriverCategory {
    return drivers.CategoryWarehouse
}

// 在driver_registry.go中注册:
// dr.register(model.DatabaseTypeExample, func() drivers.Driver {
//     return &warehouses.ExampleDriver{}
// })
```

## 7. 性能优化

### 7.1 连接池优化

- **动态池大小**: 根据负载自动调整连接池大小
- **健康检查**: 定期检查连接健康状态
- **连接复用**: 最大化连接复用率

### 7.2 查询优化

- **参数化查询**: 防止SQL注入并提高查询缓存效率
- **结果集流式处理**: 支持大数据集的流式返回
- **查询计划缓存**: 缓存查询计划以提高重复查询性能

## 8. 监控与诊断

### 8.1 指标收集

- **查询性能**: 执行时间、行数统计
- **连接池指标**: 活跃连接数、等待时间
- **错误统计**: 各类错误的发生频率

### 8.2 日志记录

- **结构化日志**: 使用JSON格式记录详细信息
- **关联ID**: 通过关联ID追踪请求链路
- **审计日志**: 记录所有查询操作

## 9. 安全特性

### 9.1 访问控制

- **JWT认证**: 基于JWT的API访问控制
- **速率限制**: 防止滥用和DoS攻击
- **权限验证**: 数据源访问权限控制

### 9.2 数据保护

- **传输加密**: 所有通信使用TLS加密
- **凭据保护**: 数据库凭据加密存储
- **查询审计**: 记录所有查询操作用于审计

## 10. 开发最佳实践

### 10.1 代码组织

- **目录结构**: 按功能类型组织驱动代码
- **接口一致性**: 统一的驱动接口实现
- **错误处理**: 标准化的错误处理机制

### 10.2 测试策略

- **单元测试**: 每个驱动的独立功能测试
- **集成测试**: 端到端的集成验证
- **性能测试**: 验证性能指标符合要求

### 10.3 文档维护

- **API文档**: 保持API文档与代码同步
- **驱动文档**: 为每个驱动提供详细文档
- **配置指南**: 提供清晰的配置说明

## 11. 未来发展方向

### 11.1 计算引擎集成

- **Trino集成**: 分布式SQL查询引擎联邦
- **Spark集成**: 批处理和流处理
- **Flink集成**: 实时流处理

### 11.2 高级功能

- **查询结果缓存**: 智能缓存和失效策略
- **GraphQL API**: 提供GraphQL接口
- **AI查询优化**: 基于AI的查询优化

---

本框架设计旨在提供一个可扩展、高性能、安全的多数据库访问解决方案，支持90+种数据源的统一访问和管理。