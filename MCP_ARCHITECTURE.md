# Data Fabric MCP 架构

~~~mermaid
flowchart LR
    Client[可信 MCP 客户端] <-->|stdio| Server[Data Fabric MCP 服务]
    Server --> Tools[MCP 工具处理器]
    Tools --> Query[统一查询服务]
    Query --> Pool[连接池与数据源驱动]
    Pool --> Sources[(已配置的数据源)]
    Tools --> Translate[SQL 方言转换器]
~~~

MCP 服务作为 Data Fabric 进程的一种启动模式运行。它复用现有数据源存储、连接池和查询服务；方言转换是可选能力。当前传输仅实现 stdio，因此由本机客户端启动和管理进程。该架构不代表提供远程 HTTP/SSE 服务。
