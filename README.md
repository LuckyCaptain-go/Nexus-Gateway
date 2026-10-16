# Data Fabric

<div align="center">
  <img src="assets/data-fabric-icon.png" alt="Data Fabric 图标" width="160">
  <h2>轻量异构数据网关：统一管理多源，用一套查询语言访问</h2>
  <p>让应用通过统一入口管理和处理企业与集群中的多种数据源</p>
  <p><a href="README_zh.md">完整中文介绍</a> · <a href="README_en.md">English</a></p>
  <p>
    <img src="https://img.shields.io/badge/Go-1.26.1+-00ADD8?logo=go" alt="Go 1.26.1+">
    <img src="https://img.shields.io/badge/License-Apache--2.0-blue" alt="Apache 2.0">
    <a href="https://github.com/RhinoLink-Tech/data-fabric/actions/workflows/go.yml"><img src="https://github.com/RhinoLink-Tech/data-fabric/actions/workflows/go.yml/badge.svg" alt="Go CI"></a>
  </p>
</div>

Data Fabric 面向 AI、RAG、BI 和业务应用，服务于企业与集群中的多数据源环境。它的核心目标是统一纳管数据库、数仓、湖仓、对象存储和文件源，并让应用面向一套查询语言与统一 API，减少按数据源分别处理连接方式和查询语法的工作。

## 我们的生态位

Data Fabric 位于应用与数据系统之间：**它负责集中管理多种数据源及其连接，为应用提供统一查询入口；产品目标是通过一套查询语言屏蔽不同数据源的语法差异，并统一提交和获取查询结果。**跨源执行可通过查询引擎适配扩展，原始数据仍由各自的数据系统管理。

- **轻量接入模式：**单源访问可由网关驱动直接完成，不需要额外部署跨源查询引擎。运行服务仍需要按配置准备元数据数据库。
- **按需扩展跨源查询：**需要跨数据源关联查询时，可通过查询引擎适配层扩展联邦能力；查询时仍会产生网络传输和中间结果交换。
- **明确的产品边界：**Data Fabric 聚焦应用侧统一入口、数据源连接管理和访问控制，不是数据库、数仓、ETL 或数据复制平台，也不重新实现分布式 SQL 引擎。

产品主打轻量、低资源占用和快速部署；当前仓库没有公开的资源或性能基准，因此不承诺具体的内存、CPU 或启动时间数字。

## 当前能力与集成状态

当前仓库提供统一的数据源管理和查询 API，包括驱动注册、连接诊断、配置校验、JWT 与限流组件，以及查询结果分批获取。支持流式读取的驱动可以逐批返回结果，减少大结果集一次性占用的内存。

**统一查询入口：**应用使用统一 API 访问已登记的数据源。指定 sourceDialect 后，Data Fabric 可在 SQL v1 兼容范围内将常见查询语法转换为目标方言；省略时按目标数据源或查询引擎的原生 SQL 执行。方言转换范围、兼容矩阵和错误行为见 [统一 SQL v1 说明](SQL_TRANSLATION_FEATURES.md)。

**跨源联邦查询：**Data Fabric 提供统一的查询引擎连接适配，可将兼容的外部联邦查询引擎登记为数据源。应用仍通过统一查询 API 提交 SQL；联邦引擎通过自身配置的 catalogs 和 connectors 访问不同数据系统，负责跨目录规划和执行，Data Fabric 负责连接、请求路由、结果归一化及查询生命周期管理。

联邦查询是可选扩展：简单的单源访问直接使用网关驱动；需要跨源关联时，再连接外部查询引擎。引擎版本、catalog 权限和 connector 能力由对应部署决定。

~~~mermaid
flowchart LR
    App[AI / RAG / BI / 业务应用] --> Gateway[Data Fabric<br/>统一 API 与数据源管理]
    Gateway --> Drivers[单源数据源驱动]
    Drivers --> Sources[数据库 / 数仓 / 湖仓 / 对象存储 / 文件]
    Gateway --> Adapter[查询引擎连接适配]
    Adapter --> Engine[外部联邦查询引擎]
    Engine --> Catalogs[引擎 catalogs 与 connectors]
    Catalogs --> Sources
~~~

图中展示两种接入方式：应用可以经网关驱动访问单个数据源，也可以将 SQL 发往外部联邦查询引擎，由引擎访问其已配置的 catalogs。

## 数据源范围

代码库中定义的代表性类型包括 MySQL、PostgreSQL、Oracle、SQL Server、OceanBase、TiDB、达梦、KingbaseES、ClickHouse、Doris、StarRocks、Snowflake、BigQuery、Iceberg、Delta Lake、Hudi，以及 S3、MinIO、OSS、COS、Azure Blob、HDFS、Ozone 和常见文件格式。类型是否在当前构建中启用、认证方式及高级能力因驱动而异，可通过服务端数据源类型接口和对应实现确认。

## 快速开始

需要 Go 1.26.1 或更高版本，以及按配置准备的元数据数据库。先检查 [配置说明](docs/configuration.md) 中的端口、数据库和安全选项，再获取代码：

~~~bash
git clone https://github.com/RhinoLink-Tech/data-fabric.git
cd data-fabric
go mod download
export DATA_FABRIC_CREDENTIAL_ACTIVE_KEY_ID=dev-v1
export DATA_FABRIC_CREDENTIAL_KEYS='{"dev-v1":"<base64-encoded-32-byte-key>"}'
go run ./cmd/server
~~~

请将示例占位值替换为 openssl rand -base64 32 生成并安全保存的密钥；服务重启时继续使用同一密钥。健康检查路径为 /health，生产环境应通过密钥管理系统注入。

更多信息参见 [API 使用指南](docs/api_usage.md)、[架构设计](docs/architecture.md)、[配置指南](docs/configuration.md)、[开发指南](docs/development.md) 和 [中文完整说明](README_zh.md)。

如果这个项目对你有帮助，欢迎点亮 GitHub Star，也欢迎通过 Issue、Pull Request 参与开源共建。

## 跨源查询引擎接入

Data Fabric 通过统一的连接适配接入 Trino、Presto 和 OpenLooKeng 查询引擎。管理员将协调器配置为一个数据源后，应用即可通过 Data Fabric 查询 API 使用引擎 SQL；跨 catalog 查询由协调器及其 connectors 执行。配置示例见 [API 使用指南](docs/api_usage.md)，查询语言边界见 [统一 SQL v1 说明](SQL_TRANSLATION_FEATURES.md)。

## 项目与社区

[贡献指南](CONTRIBUTING.md) · [安全政策](SECURITY.md) · [获取帮助](SUPPORT.md) · [发布流程](docs/releasing.md) · [GitHub Releases](https://github.com/RhinoLink-Tech/data-fabric/releases)