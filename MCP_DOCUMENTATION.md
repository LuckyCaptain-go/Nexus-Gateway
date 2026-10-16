# Data Fabric MCP 使用说明

Data Fabric 可通过 MCP stdio 传输向本机 MCP 客户端提供查询工具。它复用 Data Fabric 中的数据源配置和查询服务。此实现只支持 stdio；配置成 HTTP 或 SSE 会在启动时明确报错。

## 前置条件

- Go 1.26.1 或更高版本。
- 已配置并可访问 Data Fabric 元数据数据库。
- 如果使用 SQL 方言转换功能，需要 Python 3，并在运行环境安装 sqlglot：python3 -m pip install -r requirements-sql-translation.txt。
- 从仓库根目录启动程序，以便转换脚本能被定位。

## 启用和启动

在 configs/config.yaml 中设置：

~~~yaml
mcp:
  enabled: true
  transport: "stdio"
~~~

然后运行：

~~~bash
go run ./cmd/server
~~~

mcp.enabled 默认为 false，因此普通 HTTP 服务仍是默认启动模式。MCP stdio 模式会占用进程标准输入和输出，应由 MCP 客户端启动并管理该进程，不要把输出重定向到协议客户端的标准输出。

## 可用工具

- list_data_sources：列出数据源 ID、名称、类型、状态和连接位置摘要。
- get_data_source_info：返回指定数据源的基本信息和连接位置摘要，不返回用户名或密码。
- execute_sql_query：通过统一查询服务执行查询；可选 source_dialect 用于在执行前转换 SQL。
- fetch_next_batch：使用 execute_sql_query 返回的 query_id、slug 和 token 获取下一批结果。
- validate_sql_query：对查询请求进行校验；可选 source_dialect。
- list_tables：按数据源类型查询表名。
- translate_sql：在支持的 SQL 方言之间转换 SQL。
- list_supported_dialects：列出转换器声明支持的方言。

方言转换目前覆盖 MySQL、PostgreSQL、Trino、Spark、Oracle、SQL Server、Snowflake、SQLite、DuckDB 和 Redshift，并将部分兼容数据库名称映射到相应方言。实际转换能力取决于语句和目标数据库的语法；转换成功不等同于语义等价。

## 安全边界

MCP stdio 进程具有启动它的 MCP 客户端进程权限。已连接的 MCP 客户端可以调用这里列出的工具，并通过 Data Fabric 查询服务访问其可用数据源。只应将它配置给可信客户端，并使用数据库侧的最小权限账号。该实现尚无独立的 MCP 用户认证和逐用户授权，也不提供远程 HTTP/SSE 入口。

数据源信息工具不返回用户名和密码。查询授权仍由现有服务端查询校验及数据库账号权限共同决定。
