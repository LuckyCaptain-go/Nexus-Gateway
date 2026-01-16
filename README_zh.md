# Nexus-Gateway - 企业级统一数据访问平台

<div align="center">

**赋能数据驱动决策的全能数据网关**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Gin](https://img.shields.io/badge/Gin-Web-Framework-green?style=flat)](https://gin-gonic.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat)](LICENSE)
[![Drivers](https://img.shields.io/badge/Database_Drivers-90+-brightgreen?style=flat)]()

</div>

## 🌟 核心优势

Nexus-Gateway 是一款企业级多数据库代理网关，专为解决现代复杂数据环境中的挑战而设计。我们致力于提供一个统一、安全、高性能的数据访问解决方案，帮助企业在异构数据源环境中实现数据驱动转型。

### 🚀 无与伦比的数据源覆盖能力
- **90+ 数据源支持**：从传统关系型数据库到现代云数据仓库，从数据湖表格式到国产数据库，一网打尽
- **一站式数据接入**：无论您的数据存储在 Snowflake、Databricks、ClickHouse、HDFS、S3，还是国产的 OceanBase、TiDB、达梦等，Nexus-Gateway 提供统一的 SQL 接口
- **持续扩展能力**：插件化驱动架构，轻松支持新增数据源

### 🔒 企业级安全保障
- **零信任安全模型**：JWT 认证、SQL 注入防护、只读查询强制执行
- **细粒度权限控制**：基于角色的访问控制，确保数据安全合规
- **速率限制与监控**：防止单个用户或应用过度消耗资源
- **凭证加密存储**：敏感信息 AES-256-GCM 加密，安全无忧

### ⚡ 卓越性能表现
- **智能连接池**：针对每种数据源优化的连接管理，显著降低连接开销
- **流式查询处理**：支持大数据集流式传输，避免内存溢出
- **批量分页优化**：高效处理大规模数据查询
- **缓存机制**：智能缓存减少重复查询，提升响应速度

### 🌊 先进的数据功能
- **时间旅行查询**：支持 Iceberg、Delta Lake、Hudi 等数据湖的时间点查询
- **自动模式发现**：智能检测数据结构，简化数据探索过程
- **参数化查询**：防止 SQL 注入，确保查询安全性
- **分布式查询支持**：处理跨分片、跨分区的复杂查询

## 💼 业务价值

### 降低技术复杂度
- **单一接口**：无需为每种数据源学习不同的 API 和 SDK
- **标准化开发**：统一的数据访问模式，减少开发工作量
- **简化运维**：集中管理数据连接，降低运维成本

### 加速数据驱动决策
- **实时数据访问**：快速获取跨系统数据，支持实时分析
- **灵活数据集成**：轻松整合来自不同系统的数据
- **自助式分析**：为业务用户提供便捷的数据访问途径

### 支持企业数字化转型
- **数据孤岛消除**：打破不同系统间的数据壁垒
- **数据治理基础**：统一的数据访问层，便于数据治理实施
- **合规性保障**：完善的审计日志和访问控制

## 🛠 技术架构

Nexus-Gateway 采用模块化、可扩展的架构设计：

- **API 层**：RESTful 接口，支持标准 HTTP 协议
- **安全层**：认证、授权、限流、审计
- **服务层**：查询处理、连接管理、缓存服务
- **驱动层**：90+ 种数据源驱动，支持扩展
- **数据层**：各类数据源

## 📈 市场竞争力

与其他数据访问解决方案相比，Nexus-Gateway 具备以下独特优势：

- **覆盖面最广**：支持 90+ 种数据源，包括众多国产数据库，市场覆盖率领先
- **企业级安全**：全方位安全防护，满足金融、政务等行业严苛要求
- **高性能表现**：针对大数据场景优化，支持流式处理
- **云原生友好**：Docker、Kubernetes 原生支持，易于部署和扩展
- **开源开放**：Apache 2.0 许可证，社区驱动，持续演进

## 🚀 快速开始

### 环境要求
- Go 1.25 或更高版本
- Docker（可选）

### 部署方式

**本地部署：**
```bash
# 克隆项目
git clone https://github.com/LuckyCaptain-go/Nexus-Gateway.git
cd Nexus-Gateway

# 安装依赖
go mod download

# 配置应用
cp configs/config.yaml.example configs/config.yaml

# 启动服务
go run cmd/server/main.go
```

**Docker 部署：**
```bash
# 构建镜像
docker build -t nexus-gateway:latest .

# 运行容器
docker run -d \
  --name nexus-gateway \
  -p 8099:8099 \
  -v $(pwd)/configs:/app/configs \
  nexus-gateway:latest
```

服务启动后将在 `http://localhost:8099` 上运行。

## 📚 学习资源

- [API 使用指南](docs/api_usage.md) - 详细的 API 使用说明和示例
- [架构设计](docs/architecture.md) - 系统架构和技术细节
- [配置参考](docs/configuration.md) - 完整的配置选项说明
- [开发指南](docs/development.md) - 扩展和定制开发指导
- [贡献指南](docs/contributing.md) - 如何参与项目贡献

## 🛣 发展路线

### ✅ 第一阶段：增强单源能力（已完成 - 2025年12月）

#### 数据湖与数仓 ✅
- [x] **表格式支持**：Apache Iceberg、Delta Lake、Apache Hudi，支持模式演化和分区管理
- [x] **云数仓**：Snowflake、Databricks、Redshift、BigQuery，具备高级认证和查询优化能力
- [x] **OLAP 引擎**：ClickHouse、Apache Doris、StarRocks、Apache Druid，具备高性能查询能力

#### 对象存储与文件系统 ✅
- [x] **对象存储**：AWS S3、MinIO、阿里云 OSS、腾讯云 COS、Azure Blob，通过 S3 Select 等技术提供高级查询能力
- [x] **分布式存储**：HDFS、Apache Ozone，支持 Kerberos 认证和高可用性
- [x] **文件格式**：Parquet、ORC、Avro、CSV、JSON、XML、文本（带压缩），支持模式推断和投影下推

#### 国产数据库支持（中国）✅
- [x] **分布式数据库**：OceanBase（MySQL/Oracle 兼容模式）、TiDB、腾讯 TDSQL、华为 GaussDB
- [x] **传统数据库**：达梦（DM）、人大金仓、GBase、神通、OpenGauss，支持中文字符编码

### 🔄 第二阶段：计算引擎集成（进行中 - 2026年1-5月）
- [ ] **Trino 集成**：分布式 SQL 查询引擎联邦，支持跨数据源关联查询
- [ ] **Spark 集成**：批处理和流处理，集成 Spark SQL 连接器
- [ ] **Flink 集成**：实时流处理，具备 CDC 能力
- [ ] **计算引擎编排**：基于查询特征智能路由到最优引擎
- [ ] **基于成本的优化**：跨计算引擎的查询计划成本估算
- [ ] **资源管理**：动态分配和调度计算资源

### 🚧 第三阶段：高级分析与机器学习集成（2026年第二季度-第三季度）
- [ ] **机器学习流水线集成**：直接从查询访问 ML 模型预测
- [ ] **高级分析函数**：内置统计和预测分析功能
- [ ] **数据质量监控**：自动化异常检测和数据质量评分
- [ ] **预测查询优化**：基于机器学习的查询计划优化
- [ ] **自动扩缩容**：基于工作负载模式的智能扩缩容
- [ ] **多模型数据库支持**：图数据库、文档数据库和向量数据库集成

### 🚀 第四阶段：智能化与自动化（2026年第三季度-第四季度）
- [ ] **AI 驱动的查询助手**：自然语言转 SQL 转换
- [ ] **自动化模式演化**：自适应变化的数据模式
- [ ] **智能数据放置**：自动数据分级和放置优化
- [ ] **异常检测**：主动识别异常查询模式或性能问题
- [ ] **预测性维护**：自动化健康检查和维护调度
- [ ] **自愈基础设施**：自动故障恢复

### 🌐 第五阶段：生态与联邦（2027年）
- [ ] **跨云联邦**：跨多个云提供商的无缝访问
- [ ] **边缘计算支持**：边缘场景的本地数据处理能力
- [ ] **区块链集成**：不可变数据来源和审计跟踪
- [ ] **高级安全**：零知识证明和同态加密支持
- [ ] **行业特定连接器**：针对医疗、金融和政府的定制解决方案
- [ ] **全球数据网格**：分布式数据架构支持

## 🤝 社区与支持

- **文档中心**：[完整文档](https://LuckyCaptain-go.github.io/Nexus-Gateway)
- **问题反馈**：[GitHub Issues](https://github.com/LuckyCaptain-go/Nexus-Gateway/issues)
- **社区讨论**：[GitHub Discussions](https://github.com/LuckyCaptain-go/Nexus-Gateway/discussions)

## 📄 许可证

本项目采用 Apache License 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

<div align="center">
由 Nexus-Gateway 团队 ❤️ 倾力打造
</div>