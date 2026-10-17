# Data Fabric：轻量多源数据统一管理与查询网关

<div align="center">
  <img src="assets/data-fabric-icon.png" alt="Data Fabric 图标" width="176">
  <p><strong>统一纳管多种数据源，目标是以一套查询语言访问和处理数据</strong></p>
  <p><a href="README.md">项目主页</a> · <a href="README_en.md">English</a></p>
  <p>
    <img src="https://img.shields.io/badge/Go-1.26.1+-00ADD8?logo=go" alt="Go 1.26.1+">
    <img src="https://img.shields.io/badge/License-Apache--2.0-blue" alt="Apache 2.0">
    <a href="https://github.com/RhinoLink-Tech/data-fabric/actions/workflows/go.yml"><img src="https://github.com/RhinoLink-Tech/data-fabric/actions/workflows/go.yml/badge.svg" alt="Go CI"></a>
  </p>
</div>

Data Fabric 面向 AI Agent、RAG 系统、BI 工具和业务应用，解决一个常见问题：每个应用都要重复接入数据库、数仓、数据湖、对象存储和文件系统，分别维护驱动、连接参数、查询调用及结果分页。

Data Fabric 将企业和集群中的数据库、数仓、湖仓、对象存储及文件源纳入统一的数据源管理入口，集中管理连接配置、连接诊断和访问 API。产品核心目标是让应用面向一套查询语言和统一 API，尽量屏蔽不同数据源的接入方式与查询语法差异，实现统一提交和获取查询结果；跨源执行能力可通过查询引擎适配扩展。

## 产品生态位

**Data Fabric 是应用与多种数据系统之间的轻量统一入口。**它面向企业和集群的多数据源环境，集中管理数据源连接、配置、诊断与访问；产品目标是让应用使用一套查询语言和统一 API，减少针对不同数据源重复适配连接和 SQL 方言的工作。跨源查询引擎可按需通过适配层接入，原始数据仍由各自系统保存和管理。

| 组成部分 | 负责什么 |
| --- | --- |
| Data Fabric | 应用侧统一 API、数据源连接管理、连接诊断、单源查询结果获取和网关控制 |
| 跨源查询引擎 | 通过 catalogs 和连接器访问多个数据系统，规划并执行跨源 SQL |
| 原始数据系统 | 保存和管理实际数据 |

### 为什么强调轻量

- **单源使用无需额外引擎：**当前网关可以通过已接入的驱动访问单个数据源，不需要为了简单的数据接入额外部署一套联邦计算引擎。
- **按需扩展：**只有在需要跨数据源关联查询时，才需要部署跨源查询引擎及其相应的连接器和资源。
- **降低重复集成成本：**数据源在一个入口集中管理；应用面向统一 API 提交和获取查询结果，产品目标是无需逐种数据源重复适配其连接方式和查询语法。
- **不复制全部数据：**联邦查询可以让数据继续留在各自系统中；查询执行仍可能产生网络传输和中间结果交换。
- **轻量和快速部署是产品主打：**项目以较小的网关服务形态、低常驻资源占用和缩短部署链路为目标。仓库尚无公开的资源占用或性能基准，因此不对内存、CPU 或启动时长作量化承诺。

### Data Fabric 的职责边界

Data Fabric 聚焦应用侧连接管理、统一访问 API 和请求入口，不试图成为另一套分布式 SQL 引擎。跨源查询引擎负责 SQL 规划、跨源调度和结果计算；Data Fabric 也不是数据仓库、ETL 或数据复制平台。

系统可以先以轻量网关模式接入单个数据源，再在真正需要跨源查询时扩展联邦执行能力。是否部署查询引擎取决于应用是否需要联邦 SQL。

## 当前实现状态

Data Fabric 已提供数据源管理、驱动注册、连接诊断、配置校验、统一查询 API、JWT 与限流组件，以及分批查询结果获取。支持流式读取的驱动可逐批返回结果，降低大结果集的一次性内存占用。

**统一查询入口：**应用通过统一 API 访问已登记的数据源。指定 sourceDialect 后，Data Fabric 可在 SQL v1 兼容范围内转换常见查询语法；未指定时按目标数据源或查询引擎的原生 SQL 执行。兼容范围和失败行为见 [统一 SQL v1 说明](SQL_TRANSLATION_FEATURES.md)。

**跨源联邦查询：**Data Fabric 提供统一的查询引擎连接适配，支持将兼容的外部联邦查询引擎登记为数据源。应用通过统一查询 API 提交 SQL；外部引擎使用其配置的 catalogs 和 connectors 访问多个数据系统并执行跨目录查询。Data Fabric 负责统一连接入口、查询路由、结果归一化和查询生命周期管理。

单源访问可直接使用网关驱动；需要跨源关联时，可连接外部查询引擎。引擎版本、目录权限、连接器和 SQL 扩展由对应引擎部署决定。

~~~mermaid
flowchart LR
    App[AI Agent / RAG / BI / 业务应用] --> Gateway[Data Fabric<br/>统一 API · 数据源管理]
    Gateway --> Drivers[单源数据源驱动]
    Drivers --> Sources[数据库 / 数仓 / 湖仓 / 对象存储 / 文件]
    Gateway --> Adapter[查询引擎连接适配]
    Adapter --> Engine[外部联邦查询引擎]
    Engine --> Catalogs[引擎 catalogs 与 connectors]
    Catalogs --> Sources
~~~

应用可以通过网关驱动访问单个数据源，也可以使用联邦引擎跨多个 catalog 查询。

## 网关能力

- **数据源管理：**创建、查看、更新、启用、停用和删除数据源。
- **驱动扩展：**通过统一驱动接口和注册表扩展数据源类型。
- **连接诊断：**测试连接，校验配置，并查看连接统计和健康状态。
- **查询结果获取：**通过网关查询接口获取数据，并通过 fetch API 分批拉取后续结果；支持流式读取的驱动可减少大结果集的内存占用。
- **类型发现：**通过接口查看当前服务识别的数据源类型。
- **网关控制：**代码中包含 JWT、请求限流、配置校验和健康检查组件。实际认证范围由部署配置和路由组决定。

## 数据源类别

下列为代码库中定义的代表性数据源类型。驱动的实现完整度、认证方式、可用功能和当前构建是否启用可能不同；以服务端类型接口及具体驱动实现为准。

| 类别 | 代表性类型 |
| --- | --- |
| 关系型与兼容数据库 | MySQL、MariaDB、PostgreSQL、Oracle、SQL Server |
| 国产数据库 | OceanBase、TiDB、TDSQL、GaussDB、达梦、KingbaseES、GBase、神通、OpenGauss |
| 分析型数据库 | ClickHouse、Doris、StarRocks、Druid、Hologres |
| 云数仓与大数据平台 | Snowflake、Databricks、Redshift、BigQuery、Hive、MaxCompute |
| 湖仓表格式 | Apache Iceberg、Delta Lake、Apache Hudi |
| 对象存储与文件系统 | S3、MinIO、阿里云 OSS、腾讯云 COS、Azure Blob、HDFS、Apache Ozone |
| 文件格式 | Parquet、ORC、Avro、CSV、JSON、XML、文本 |

## 快速开始

### 环境要求

- Go 1.26.1 或更高版本。
- 按服务配置准备元数据数据库及可访问的数据源。
- 单源网关模式不需要部署跨源查询引擎；跨源联邦能力需要完成相应的引擎适配并部署所选引擎。

### 获取代码并启动

~~~bash
git clone https://github.com/RhinoLink-Tech/data-fabric.git
cd data-fabric
go mod download
~~~

先复制 configs/config.yaml.example 为 configs/config.yaml，检查其中的服务端口、元数据数据库和安全参数（敏感字段建议通过环境变量注入），再启动：

~~~bash
export DATA_FABRIC_CREDENTIAL_ACTIVE_KEY_ID=dev-v1
export DATA_FABRIC_CREDENTIAL_KEYS='{"dev-v1":"<base64-encoded-32-byte-key>"}'
go run ./cmd/server
~~~

健康检查路径为 /health。默认值和部署参数见 [配置指南](docs/configuration.md)。

请将示例占位值替换为 openssl rand -base64 32 生成的密钥，并在服务重启时继续使用同一密钥。密钥注入、旧凭据迁移和轮换方式见配置指南。

## API 概览

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | /health | 服务健康检查 |
| GET | /api/v1/database/types | 查询服务识别的数据源类型 |
| GET | /api/v1/database/compatibility | 查询数据库兼容性信息 |
| GET / POST | /api/v1/datasources | 列出或创建数据源 |
| GET / PUT / DELETE | /api/v1/datasources/:id | 查看、更新或删除数据源 |
| POST | /api/v1/datasources/:id/activate | 启用数据源 |
| POST | /api/v1/datasources/:id/deactivate | 停用数据源 |
| POST | /api/v1/database/test-connection | 测试数据源连接 |
| POST | /api/v1/database/validate-config | 校验数据源配置 |
| POST | /api/v1/fetch | 执行批量查询并获取首批结果 |
| GET | /api/v1/fetch/:query_id/:slug/:token | 获取后续结果批次 |

具体请求字段、响应结构和认证行为见 [API 使用指南](docs/api_usage.md)。

## 文档

- [API 使用指南](docs/api_usage.md)
- [架构设计](docs/architecture.md)
- [配置指南](docs/configuration.md)
- [开发指南](docs/development.md)
- [贡献指南](docs/contributing.md)
- [English README](README_en.md)

## 参与开源共建

如果 Data Fabric 对你有帮助，欢迎在 GitHub 为项目点亮 Star。也欢迎通过 Issue、Pull Request 和文档改进参与开源共建。

## 跨源查询引擎接入

Data Fabric 通过统一连接适配接入 Trino、Presto 和 OpenLooKeng。将协调器登记为数据源后，应用即可通过 Data Fabric 查询 API 使用引擎 SQL；跨 catalog 的规划和执行由协调器及其 connectors 负责。配置示例见 [API 使用指南](docs/api_usage.md)，SQL 兼容范围见 [统一 SQL v1 说明](SQL_TRANSLATION_FEATURES.md)。

## 社区与发布

[贡献指南](CONTRIBUTING.md) · [安全政策](SECURITY.md) · [获取帮助](SUPPORT.md) · [发布流程](docs/releasing.md) · [版本列表](https://github.com/RhinoLink-Tech/data-fabric/releases)

## 许可证

本项目采用 Apache License 2.0，详见 [LICENSE](LICENSE)。