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

当前仓库包含数据源管理、驱动注册、连接诊断、配置校验、JWT 与限流组件，以及查询结果分批获取；兼容的驱动可使用流式游标。具体驱动能力以服务端类型接口和实现为准。

**统一查询能力：**应用通过统一查询语言提交查询，Data Fabric 会根据目标数据源适配查询语法，并通过统一 API 返回结果，减少为每种数据源重复改写常见查询的工作。不同数据源支持的能力范围存在差异，使用特定数据源专有功能时仍可能需要对应语法。**跨源查询引擎适配尚未交付：**当前服务请求链路尚未接入联邦查询执行适配器，也没有跨源引擎连接配置的自动管理能力；目前还不能经 Data Fabric 执行跨源联邦 SQL。图中的虚线表示目标集成边界，不代表已交付能力。

~~~mermaid
flowchart LR
    App[AI / RAG / BI / 业务应用] --> Gateway[Data Fabric<br/>应用侧 API 与数据源管理]
    Gateway --> Drivers[当前：驱动与单源访问]
    Drivers --> Sources[数据库 / 数仓 / 湖仓 / 对象存储 / 文件]
    Gateway -. 目标集成：联邦查询适配器 .-> Engine[跨源查询引擎]
    Engine -.-> Catalogs[Catalogs 与连接器]
    Catalogs -.-> Sources
~~~

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

## 跨源查询引擎扩展

后续将面向多种跨源查询引擎提供统一、无缝的接入能力，例如 Trino、Presto、OpenLooKeng 等。上述引擎是后续适配方向，当前仓库尚未交付相应的联邦查询适配器。

本项目采用 Apache License 2.0，详见 [LICENSE](LICENSE)。

## 项目与社区

[贡献指南](CONTRIBUTING.md) · [安全政策](SECURITY.md) · [获取帮助](SUPPORT.md) · [发布流程](docs/releasing.md) · [GitHub Releases](https://github.com/RhinoLink-Tech/data-fabric/releases)